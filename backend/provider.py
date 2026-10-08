"""Codex provider configuration.

By default the plugin delegates to the user's normal Codex CLI configuration.
An external OpenAI-compatible router can be selected explicitly with
CODEX_SUBAGENTS_ROUTER_URL and CODEX_SUBAGENTS_ROUTER_API_KEY.
"""
from __future__ import annotations
import os

BASE_URL = os.environ.get("CODEX_SUBAGENTS_ROUTER_URL", "")
ENV_KEY = "OPENAI_API_KEY"


def environment() -> dict[str, str]:
    env = dict(os.environ)
    if env.get("CODEX_SUBAGENTS_ROUTER_API_KEY") and BASE_URL:
        env.setdefault("OPENAI_API_KEY", env["CODEX_SUBAGENTS_ROUTER_API_KEY"])
    return env


def request_defaults(model: str | None = None, effort: str | None = None) -> dict[str, object]:
    provider = "external" if os.environ.get("CODEX_SUBAGENTS_ROUTER_URL") else "openai"
    return {"modelProvider": provider, "model": model or "gpt-5", "effort": effort or "medium", "serviceTier": "default"}


def app_server_args(base_url: str | None = None) -> list[str]:
    """Return overrides only when an external router is explicitly configured."""
    url = base_url or BASE_URL
    if not url:
        return []
    provider = "external"
    return [
        "-c", f"model_provider={provider}",
        "-c", f"model_providers.{provider}.name={provider}",
        "-c", f"model_providers.{provider}.base_url={url}",
        "-c", f"model_providers.{provider}.env_key={ENV_KEY}",
        "-c", f"model_providers.{provider}.wire_api=responses",
        "-c", f"model_providers.{provider}.requires_openai_auth=false",
    ]
