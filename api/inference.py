import os
import requests
from typing import Optional

API_URL = "https://router.huggingface.co/v1/chat/completions"


def _call_llm(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: Optional[str] = None) -> str:
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise RuntimeError("Set HF_TOKEN in env")
    headers = {"Authorization": f"Bearer {hf_token}"}
    payload = {
        "model": model or os.getenv("HF_ROUTER_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct:fireworks-ai"),
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": False,
    }
    resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        print("HF Router error:", resp.text)
        resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

