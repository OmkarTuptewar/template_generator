import os
from typing import Any

from openai import AsyncAzureOpenAI


class AzureOpenAIClient:
    def __init__(self, max_tokens: int = 512):
        api_key = os.environ.get("AZURE_OPENAI_API_KEY")
        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION")
        deployment = os.environ.get("AZURE_CHAT_DEPLOYMENT")
        missing = [
            name
            for name, val in [
                ("AZURE_OPENAI_API_KEY", api_key),
                ("AZURE_OPENAI_ENDPOINT", endpoint),
                ("AZURE_OPENAI_API_VERSION", api_version),
                ("AZURE_CHAT_DEPLOYMENT", deployment),
            ]
            if not val
        ]
        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

        self.deployment = deployment
        self.client = AsyncAzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
        )
        self.max_tokens = max_tokens

    async def chat_completion(self, system_prompt: str, user_payload: str) -> str:
        response = await self.client.chat.completions.create(
            model=self.deployment,
            temperature=0.0,
            top_p=1,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_payload},
            ],
        )
        return response.choices[0].message.content or ""
