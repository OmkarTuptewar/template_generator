import argparse
import asyncio
import os
import sys
import time
from dotenv import load_dotenv


load_dotenv()
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.batch_processor import BatchProcessor


def load_system_prompt(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bus NER template generator")
    parser.add_argument(
        "--input",
        default="/Users/int1964/TEMPLATE_GENRATOR/data/raw_queries.json",
        help="Path to input JSONL file",
    )
    parser.add_argument(
        "--output",
        default="/Users/int1964/TEMPLATE_GENRATOR/data/templates_output.jsonl",
        help="Path to output JSONL file",
    )
    parser.add_argument(
        "--registry",
        default="/Users/int1964/TEMPLATE_GENRATOR/data/new_entity_values.json",
        help="Path to new entity values JSON file",
    )
    parser.add_argument(
        "--system-prompt",
        default="/Users/int1964/TEMPLATE_GENRATOR/prompts/system_prompt.txt",
        help="Path to system prompt text file",
    )
    parser.add_argument(
        "--template-only",
        default="/Users/int1964/TEMPLATE_GENRATOR/data/only_template_output.txt",
        help="Path to template-only output file for training",
    )
    parser.add_argument("--batch-size", type=int, default=20, help="Queries per LLM call")
    parser.add_argument("--concurrency", type=int, default=150, help="Parallel LLM calls")
    parser.add_argument("--max-tokens", type=int, default=1536, help="Max tokens per LLM response")
    parser.add_argument("--reset", action="store_true", help="Ignore already processed queries and start fresh")
    parser.add_argument("--fast", action="store_true", help="Fast mode: batch=20, concurrency=150, max_tokens=1536")
    parser.add_argument("--ultra-fast", action="store_true", help="Ultra-fast mode: batch=30, concurrency=200, max_tokens=1024")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Apply performance presets
    batch_size = args.batch_size
    concurrency = args.concurrency
    max_tokens = args.max_tokens

    if args.ultra_fast:
        batch_size = 30
        concurrency = 200
        max_tokens = 1024
        print("⚡ ULTRA-FAST MODE: batch=30, concurrency=200, max_tokens=1024")
    elif args.fast:
        batch_size = 20
        concurrency = 150
        max_tokens = 1536
        print("🚀 FAST MODE: batch=20, concurrency=150, max_tokens=1536")

    system_prompt = load_system_prompt(args.system_prompt)

    # Count total queries to process
    import json
    total_queries = 0
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content.startswith('['):
                data = json.loads(content)
                total_queries = len(data)
            else:
                total_queries = sum(1 for _ in content.splitlines() if _.strip())
    except:
        pass

    print(f"📊 Processing {total_queries} queries...")
    start_time = time.time()

    processor = BatchProcessor(
        input_path=args.input,
        output_path=args.output,
        registry_path=args.registry,
        system_prompt=system_prompt,
        batch_size=batch_size,
        concurrency=concurrency,
        max_tokens=max_tokens,
        template_only_path=args.template_only,
        reset=args.reset,
    )

    asyncio.run(processor.run())

    # Performance stats
    elapsed = time.time() - start_time
    if total_queries > 0:
        qps = total_queries / elapsed
        print(f"\n✅ Completed in {elapsed:.2f}s ({qps:.2f} queries/sec)")
    else:
        print(f"\n✅ Completed in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
