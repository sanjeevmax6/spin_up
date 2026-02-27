from __future__ import annotations

import logging
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self, api_key: str, model: str) -> None:
        self._client = OpenAI(api_key=api_key)
        self._model = model

    def complete(self, prompt: str) -> tuple[str, dict[str, int]]:
        logger.info("LLM request model=%s prompt_chars=%s", self._model, len(prompt))
        response = self._client.responses.create(
            model=self._model,
            input=prompt,
        )
        text = response.output_text
        usage: dict[str, int] = {}
        if response.usage:
            usage = {
                "input_tokens": int(getattr(response.usage, "input_tokens", 0)),
                "output_tokens": int(getattr(response.usage, "output_tokens", 0)),
                "total_tokens": int(getattr(response.usage, "total_tokens", 0)),
            }
        logger.info("LLM response model=%s tokens=%s", self._model, usage.get("total_tokens", 0))
        return text, usage


class DryRunLLMClient:
    def complete(self, prompt: str) -> tuple[str, dict[str, int]]:
        logger.info("LLM dry-run request prompt_chars=%s", len(prompt))
        preview = prompt.splitlines()[:8]
        return (
            "\n".join(
                [
                    "# Dry Run Output",
                    "Generated in dry-run mode. Prompt preview:",
                    "",
                    *preview,
                ]
            ),
            {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        )


def build_llm_client(api_key: str, model: str, dry_run: bool) -> Any:
    if dry_run:
        return DryRunLLMClient()
    return LLMClient(api_key=api_key, model=model)
