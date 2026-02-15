import asyncio
import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional

from src.openai_client import AzureOpenAIClient
from src.result_writer import ResultWriter
from src.entity_value_registry import EntityValueRegistry, ENTITY_VALUES
from utils.logger import Logger


def count_lines(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def read_jsonl_lines(path: str, skip: int) -> Iterable[str]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    if not content.strip():
        return

    if content.lstrip().startswith("["):
        try:
            data = json.loads(content)
        except Exception:
            return
        if isinstance(data, list):
            for item in data[skip:]:
                yield json.dumps(item, ensure_ascii=False)
        return

    lines = content.splitlines()
    for line in lines[skip:]:
        line = line.strip()
        if not line:
            continue
        yield line


class BatchProcessor:
    def __init__(
        self,
        input_path: str,
        output_path: str,
        registry_path: str,
        system_prompt: str,
        batch_size: int = 10,
        concurrency: int = 100,
        max_tokens: int = 2048,
        template_only_path: Optional[str] = None,
        reset: bool = False,
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.registry_path = registry_path
        self.system_prompt = system_prompt
        self.batch_size = batch_size
        self.concurrency = concurrency
        self.max_tokens = max_tokens
        self.template_only_path = template_only_path
        self.reset = reset
        self.logger = Logger()
        self.writer = ResultWriter(self.output_path)
        self.registry = EntityValueRegistry(self.registry_path)
        self.client = AzureOpenAIClient(max_tokens=self.max_tokens)
        self._write_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # LLM output helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_llm_output(raw: str) -> str:
        """Strip markdown code fences and surrounding whitespace."""
        text = raw.strip()
        if text.startswith("```"):
            first_nl = text.find("\n")
            if first_nl != -1:
                text = text[first_nl + 1:]
            if text.endswith("```"):
                text = text[:-3]
        return text.strip()

    # ------------------------------------------------------------------
    # Post-process validator
    # ------------------------------------------------------------------

    @staticmethod
    def _find_leaked_entities(
        template: str, reference_values: Dict[str, List[str]]
    ) -> List[Dict[str, str]]:
        """Return entity values that still appear as literal text in the template."""
        # Remove {PLACEHOLDER} tags — only check remaining literal text
        clean = re.sub(r"\{[A-Z_]+\}", " ", template)
        clean_lower = clean.lower()

        if not clean_lower.strip():
            return []

        # Get blocked values to skip generic words
        blocked = EntityValueRegistry.BLOCKED_VALUES

        # Collect (label, value) sorted longest-first to prefer longest match
        candidates: List[tuple] = []
        for label, values in reference_values.items():
            label_blocked = blocked.get(label, set())
            for val in values:
                val = val.strip()
                if len(val) < 2:
                    continue
                # Skip blocked/generic values
                if val.lower().strip() in label_blocked:
                    continue
                candidates.append((label, val))
        candidates.sort(key=lambda x: len(x[1]), reverse=True)

        leaked: List[Dict[str, str]] = []
        seen_lower: set = set()

        for label, val in candidates:
            val_lower = val.lower()
            if val_lower in seen_lower:
                continue
            # Word-boundary match so we don't false-positive on substrings
            pattern = r"\b" + re.escape(val_lower) + r"\b"
            if re.search(pattern, clean_lower):
                leaked.append({"label": label, "value": val})
                seen_lower.add(val_lower)
                # Remove matched span to avoid double-matching shorter substrings
                clean_lower = re.sub(pattern, " ", clean_lower, count=1)

        return leaked

    def _build_correction_payload(
        self,
        query: str,
        template: str,
        leaked: List[Dict[str, str]],
        reference_values: Dict[str, List[str]],
    ) -> str:
        """Build a correction payload telling the LLM exactly what it missed."""
        issues = "\n".join(
            f'  - "{item["value"]}" must be replaced with {{{item["label"]}}}'
            for item in leaked
        )
        correction = {
            "queries": [query],
            "previous_template": template,
            "errors": (
                "Your previous template left these entity values as literal text. "
                "Every one of them MUST become a {PLACEHOLDER}:\n" + issues
            ),
            "instruction": (
                "Return the corrected JSON ARRAY with ALL listed literal values "
                "replaced by their correct {LABEL} placeholder. "
                "Do NOT leave any of them as text."
            ),
            "entity_labels": self.registry.get_entity_labels(),
            "entity_values_reference": reference_values,
        }
        return json.dumps(correction, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Core processing (batch mode)
    # ------------------------------------------------------------------

    async def _process_batch_queries(
        self, queries: List[str], reference_values: Dict[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """Process a batch of queries in a single LLM call."""
        payload = {
            "queries": queries,
            "entity_labels": self.registry.get_entity_labels(),
            "entity_values_reference": reference_values,
        }
        payload_str = json.dumps(payload, ensure_ascii=False)

        # --- initial LLM call (up to 3 retries) ---
        results: Optional[List[Dict[str, Any]]] = None
        last_error: Optional[str] = None
        for attempt in range(3):
            try:
                raw = await self.client.chat_completion(self.system_prompt, payload_str)
                content = self._sanitize_llm_output(raw)
                results = json.loads(content)
                if not isinstance(results, list):
                    raise ValueError("LLM output is not a JSON array")
                if len(results) != len(queries):
                    raise ValueError(
                        f"LLM returned {len(results)} results for {len(queries)} queries"
                    )
                # Validate each result has "ignore" field
                for i, result in enumerate(results):
                    if not isinstance(result, dict) or "ignore" not in result:
                        raise ValueError(f"Result {i} missing 'ignore' field")
                break
            except Exception as exc:
                last_error = str(exc)
                results = None
                await asyncio.sleep(0.5)

        if results is None:
            self.logger.error(f"LLM batch failed after retries: {last_error}")
            return [{"ignore": True} for _ in queries]

        # --- post-process validation: catch leaked entities ---
        for i, (query, result) in enumerate(zip(queries, results)):
            if result.get("ignore") is False and "template" in result:
                leaked = self._find_leaked_entities(result["template"], reference_values)
                if leaked:
                    labels_missed = ", ".join(
                        f'"{l["value"]}"→{l["label"]}' for l in leaked
                    )
                    self.logger.info(
                        f"Leaked entities in query {i}: [{labels_missed}]. "
                        f"Retrying with correction hint."
                    )
                    corrected = await self._retry_with_correction(
                        query, result["template"], leaked, reference_values
                    )
                    if corrected is not None and len(corrected) == 1:
                        results[i] = corrected[0]

        return results

    async def _retry_with_correction(
        self,
        query: str,
        template: str,
        leaked: List[Dict[str, str]],
        reference_values: Dict[str, List[str]],
    ) -> Optional[List[Dict[str, Any]]]:
        """Send a single correction retry to the LLM."""
        correction_payload = self._build_correction_payload(
            query, template, leaked, reference_values
        )
        try:
            raw = await self.client.chat_completion(
                self.system_prompt, correction_payload
            )
            content = self._sanitize_llm_output(raw)
            corrected = json.loads(content)
            if isinstance(corrected, list) and len(corrected) == 1:
                if isinstance(corrected[0], dict) and "ignore" in corrected[0]:
                    return corrected
        except Exception as exc:
            self.logger.error(f"Correction retry failed: {exc}")
        return None

    # ------------------------------------------------------------------
    # Result handling & orchestration
    # ------------------------------------------------------------------

    async def _handle_results(
        self, queries: List[str], results: List[Dict[str, Any]]
    ) -> None:
        async with self._write_lock:
            for query, result in zip(queries, results):
                # Update registry with any new entity values
                if result.get("ignore") is False:
                    new_values = result.get("new_entity_values", {})
                    if isinstance(new_values, dict):
                        self.registry.update_with_new_values(new_values)

                # Write query + template to main output
                if result.get("ignore") is True:
                    output_obj = {"query": query, "ignore": True}
                else:
                    output_obj = {
                        "query": query,
                        "template": result.get("template", ""),
                    }
                self.writer.append_template_result(output_obj)

                # Write template-only to separate file
                if self.template_only_path and result.get("ignore") is False:
                    template = result.get("template", "")
                    if template:
                        with open(self.template_only_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps(template, ensure_ascii=False) + ",\n")

    async def _bounded_process(
        self,
        queries: List[str],
        reference_values: Dict[str, List[str]],
        semaphore: asyncio.Semaphore,
    ) -> None:
        async with semaphore:
            results = await self._process_batch_queries(queries, reference_values)
            await self._handle_results(queries, results)

    async def run(self) -> None:
        if self.reset:
            already_done = 0
            self.logger.info("Reset flag enabled - processing all queries from the beginning")
        else:
            already_done = count_lines(self.output_path)
            self.logger.info(f"Skipping {already_done} already processed queries")

        semaphore = asyncio.Semaphore(self.concurrency)
        batch: List[str] = []
        total_processed = 0

        for line in read_jsonl_lines(self.input_path, already_done):
            try:
                query = json.loads(line)
                if not isinstance(query, str):
                    raise ValueError("Query line is not a JSON string")
            except Exception:
                # Skip invalid lines
                continue

            batch.append(query)
            if len(batch) >= self.batch_size:
                await self._process_batch_group(batch, semaphore)
                total_processed += len(batch)
                # Log progress every 10 batches (or every batch_size * 10 queries)
                if total_processed % (self.batch_size * 10) == 0:
                    self.logger.info(f"✓ Processed {total_processed} queries...")
                batch = []

        if batch:
            await self._process_batch_group(batch, semaphore)
            total_processed += len(batch)
        
        self.logger.info(f"✅ Total processed: {total_processed} queries")

    async def _process_batch_group(
        self, batch: List[str], semaphore: asyncio.Semaphore
    ) -> None:
        """Process a single batch group."""
        reference_values = self.registry.get_reference_values()
        await self._bounded_process(batch, reference_values, semaphore)
