"""
FlowForge LLM Client — Configurable LLM provider abstraction.

Supports OpenAI, Google Gemini, Anthropic Claude, and local Ollama.
"""

import json
import logging
from typing import Any

from flowforge.config import config

logger = logging.getLogger(__name__)


class LLMClient:
    """Unified LLM client that abstracts multiple providers."""

    def __init__(self, provider: str | None = None, model: str | None = None, api_key: str | None = None):
        self.provider = provider or config.llm.provider
        self.model = model or config.llm.model
        self.api_key = api_key or config.llm.api_key
        self.temperature = config.llm.temperature
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        if self.provider == "openai":
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
        elif self.provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=self.api_key)
            self._client = genai.GenerativeModel(self.model)
        elif self.provider == "anthropic":
            from anthropic import Anthropic
            self._client = Anthropic(api_key=self.api_key)
        elif self.provider == "ollama":
            from openai import OpenAI
            self._client = OpenAI(
                base_url=config.llm.base_url or "http://localhost:11434/v1",
                api_key="ollama",
            )
        else:
            raise ValueError(f"Unknown LLM provider: {self.provider}")

        return self._client

    def chat(
        self,
        messages: list[dict[str, str]],
        tools: list[dict] | None = None,
        tool_choice: str = "auto",
    ) -> dict:
        """Send a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tool definitions for function calling
            tool_choice: Tool selection strategy — 'auto', 'none', 'required'

        Returns:
            Dict with 'content' (str) and optionally 'tool_calls' (list)
        """
        client = self._get_client()

        if self.provider in ("openai", "ollama"):
            return self._chat_openai(client, messages, tools, tool_choice)
        elif self.provider == "gemini":
            return self._chat_gemini(client, messages)
        elif self.provider == "anthropic":
            return self._chat_anthropic(client, messages, tools)

    def _chat_openai(self, client, messages, tools, tool_choice) -> dict:
        kwargs = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = tool_choice

        response = client.chat.completions.create(**kwargs)
        choice = response.choices[0]

        result = {"content": choice.message.content or ""}
        if choice.message.tool_calls:
            result["tool_calls"] = [
                {
                    "id": tc.id,
                    "function": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                }
                for tc in choice.message.tool_calls
            ]
        return result

    def _chat_gemini(self, client, messages) -> dict:
        # Convert messages to Gemini format
        prompt_parts = []
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            if role == "system":
                prompt_parts.insert(0, content)
            else:
                prompt_parts.append(f"{role}: {content}")

        response = client.generate_content("\n\n".join(prompt_parts))
        return {"content": response.text}

    def _chat_anthropic(self, client, messages, tools) -> dict:
        system_msg = None
        chat_messages = []
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                chat_messages.append(msg)

        kwargs = {
            "model": self.model,
            "messages": chat_messages,
            "max_tokens": 4096,
            "temperature": self.temperature,
        }
        if system_msg:
            kwargs["system"] = system_msg

        response = client.messages.create(**kwargs)

        content = ""
        tool_calls = []
        for block in response.content:
            if block.type == "text":
                content += block.text
            elif block.type == "tool_use":
                tool_calls.append({
                    "id": block.id,
                    "function": block.name,
                    "arguments": block.input,
                })

        result = {"content": content}
        if tool_calls:
            result["tool_calls"] = tool_calls
        return result
