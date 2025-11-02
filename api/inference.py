import os
import requests
import json

# Fireworks native defaults
FW_KEY = os.getenv("FIREWORKS_API_KEY")
FW_MODEL = os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/gpt-oss-20b")
FW_URL_CHAT = os.getenv("FIREWORKS_URL_CHAT", "https://api.fireworks.ai/inference/v1/chat/completions")
FW_URL_COMP = os.getenv("FIREWORKS_URL_COMP", "https://api.fireworks.ai/inference/v1/completions")


def _call_llm(
    prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.2,
    model: str | None = None,
    provider: str | None = None,
) -> str:
    """Unified LLM call with explicit provider.

    - fireworks: uses native chat endpoint, falls back to completions on 400/404
    - hf_router: optional fallback to HF Router OpenAI-style endpoint
    """
    effective_provider = (provider or os.getenv("LLM_PROVIDER") or "fireworks").lower()

    if effective_provider == "fireworks":
        if not FW_KEY:
            raise RuntimeError("Set FIREWORKS_API_KEY")
        mdl = model or FW_MODEL
        # If a router-style id sneaks in, force native id
        if isinstance(mdl, str) and (":fireworks-ai" in mdl or not mdl.startswith("accounts/")):
            mdl = FW_MODEL
        headers = {"Authorization": f"Bearer {FW_KEY}", "Content-Type": "application/json"}

        # 1) Try chat/completions
        chat_body = {
            "model": mdl,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "stream": False,
        }
        r = requests.post(FW_URL_CHAT, headers=headers, json=chat_body, timeout=60)
        if r.status_code == 200:
            j = r.json()
            return j["choices"][0]["message"]["content"]

        # 2) Fallback to plain completions
        if r.status_code in (400, 404):
            try:
                err = r.json()
            except Exception:
                err = {"error": {"message": r.text}}
            code = (err.get("error") or {}).get("code", "").lower()
            msg = (err.get("error") or {}).get("message", "").lower()
            if "not_found" in code or "model" in msg:
                comp_body = {
                    "model": mdl,
                    "prompt": prompt,
                    "max_tokens": int(max_tokens),
                    "temperature": float(temperature),
                    "stream": False,
                }
                rc = requests.post(FW_URL_COMP, headers=headers, json=comp_body, timeout=60)
                if rc.status_code == 200:
                    jc = rc.json()
                    return jc["choices"][0].get("text") or jc["choices"][0]["message"]["content"]
                raise RuntimeError(f"Fireworks completions error {rc.status_code}: {rc.text}")

        # Other chat errors
        raise RuntimeError(f"Fireworks chat error {r.status_code}: {r.text}")

    elif effective_provider in ("hf_router", "hf"):
        router_url = os.getenv("HF_ROUTER_URL", "https://router.huggingface.co/v1/chat/completions")
        router_model = os.getenv("HF_ROUTER_MODEL", "openai/gpt-oss-20b:fireworks-ai")
        token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACEHUB_API_TOKEN")
        if not token:
            raise RuntimeError("Set HF_TOKEN (or HUGGINGFACEHUB_API_TOKEN) for hf_router provider")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "model": model or router_model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "stream": False,
        }
        resp = requests.post(router_url, headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"HF Router error {resp.status_code}: {resp.text}")
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER={effective_provider}")


def _router_call(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: str | None = None) -> str:
    # Back-compat shim
    return _call_llm(prompt, max_tokens=max_tokens, temperature=temperature, model=model)

