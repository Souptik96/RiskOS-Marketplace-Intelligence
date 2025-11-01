import os, requests, json

# Use a dedicated env var for the HF Router to avoid collisions
ROUTER_URL = os.getenv("HF_ROUTER_URL") or "https://router.huggingface.co/v1/chat/completions"
print("Router→", ROUTER_URL)
DEFAULT_MODEL = os.getenv("HF_ROUTER_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct:fireworks-ai")


def _call_llm(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: str | None = None) -> str:
    token = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACEHUB_API_TOKEN")
    if not token:
        raise RuntimeError("Set HF_TOKEN (or HUGGINGFACEHUB_API_TOKEN) in env")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "model": model or DEFAULT_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": float(temperature),
        "stream": False
    }

    try:
        resp = requests.post(ROUTER_URL, headers=headers, json=body, timeout=60)
    except requests.exceptions.ConnectionError as e:
        # Log the actual error without falling back to port 8000
        error_msg = f"HF Router connection failed: {str(e)}"
        print(error_msg)
        raise RuntimeError(error_msg)
    
    if resp.status_code != 200:
        # Surface router/provider error clearly
        try:
            failing_model = body.get("model")
        except Exception:
            failing_model = None
        print(f"HF Router error {resp.status_code} for model={failing_model}: {resp.text}")
        raise RuntimeError(f"HF Router error {resp.status_code}: {resp.text}")

    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception:
        raise RuntimeError(f"Unexpected HF Router response: {json.dumps(data)[:800]}")