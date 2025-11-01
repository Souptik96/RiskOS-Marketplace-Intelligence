import os, requests, json

# Fireworks native
FIREWORKS_URL = os.getenv("FIREWORKS_URL", "https://api.fireworks.ai/inference/v1/chat/completions")
FIREWORKS_MODEL = os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/qwen3-coder-30b-a3b-instruct")

# HF Router (optional fallback)
HF_ROUTER_URL = os.getenv("HF_ROUTER_URL", "https://router.huggingface.co/v1/chat/completions")
HF_ROUTER_MODEL = os.getenv("HF_ROUTER_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct:fireworks-ai")


def _call_llm(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: str | None = None) -> str:
    provider = (os.getenv("LLM_PROVIDER") or "fireworks").lower()

    if provider == "fireworks":
        key = os.getenv("FIREWORKS_API_KEY")
        if not key:
            raise RuntimeError("Set FIREWORKS_API_KEY for Fireworks provider")
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        body = {
            "model": model or FIREWORKS_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "stream": False,
        }
        resp = requests.post(FIREWORKS_URL, headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Fireworks error {resp.status_code}: {resp.text}")
        data = resp.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            return data["choices"][0].get("text", "")

    elif provider in ("hf_router", "hf"):
        token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACEHUB_API_TOKEN")
        if not token:
            raise RuntimeError("Set HF_TOKEN (or HUGGINGFACEHUB_API_TOKEN) for hf_router provider")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "model": model or HF_ROUTER_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": int(max_tokens),
            "temperature": float(temperature),
            "stream": False,
        }
        resp = requests.post(HF_ROUTER_URL, headers=headers, json=body, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"HF Router error {resp.status_code}: {resp.text}")
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}")


def _router_call(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: str | None = None) -> str:
    return _call_llm(prompt, max_tokens=max_tokens, temperature=temperature, model=model)