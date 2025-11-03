import os
import json
import requests
from typing import Any, Dict, Optional, List

# ---- Fireworks native (defaults match your .env) ----
FIREWORKS_MODEL = os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/gpt-oss-20b")
FIREWORKS_URL_CHAT = os.getenv("FIREWORKS_URL_CHAT", "https://api.fireworks.ai/inference/v1/chat/completions")
FIREWORKS_URL_COMP = os.getenv("FIREWORKS_URL_COMP", "https://api.fireworks.ai/inference/v1/completions")

def _extract_text_from_choices(data: Dict[str, Any]) -> Optional[str]:
    """
    Robustly extract assistant text from Fireworks/OpenAI-style responses.

    Tries (in order):
      - choices[0].message.content (str or list of segments)
      - choices[0].text (completions-style)
      - choices[*] concatenate any available text fields (best-effort)
    """
    choices: List[Dict[str, Any]] = data.get("choices") or []
    if not choices:
        return None

    # Helper to normalize content which may be str or list of dicts/segments
    def _norm_content(val: Any) -> Optional[str]:
        if val is None:
            return None
        if isinstance(val, str):
            return val.strip()
        if isinstance(val, list):
            parts: List[str] = []
            for seg in val:
                if isinstance(seg, str):
                    parts.append(seg)
                elif isinstance(seg, dict):
                    # Common keys used by various providers
                    txt = seg.get("text") or seg.get("content") or seg.get("value")
                    if isinstance(txt, str):
                        parts.append(txt)
            return ("\n".join(p for p in parts if p.strip())) or None
        # Unknown structure
        return None

    # 1) chat style
    msg = choices[0].get("message")
    if isinstance(msg, dict):
        content = _norm_content(msg.get("content"))
        if content:
            return content

    # 2) completions style
    text = choices[0].get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    # 3) best-effort scan
    buf: List[str] = []
    for ch in choices:
        if isinstance(ch, dict):
            # message.content path
            if isinstance(ch.get("message"), dict):
                c = _norm_content(ch["message"].get("content"))
                if c:
                    buf.append(c)
            # text path
            t = ch.get("text")
            if isinstance(t, str) and t.strip():
                buf.append(t.strip())
            # delta.content (streaming shards)
            delta = ch.get("delta")
            if isinstance(delta, dict):
                dc = _norm_content(delta.get("content"))
                if dc:
                    buf.append(dc)
    if buf:
        return "\n".join(buf).strip()

    return None


def _post_fireworks_chat(model: str, prompt: str, max_tokens: int, temperature: float, key: str) -> requests.Response:
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": int(max_tokens),
        "temperature": float(temperature),
        "stream": False,
    }
    return requests.post(FIREWORKS_URL_CHAT, headers=headers, json=body, timeout=60)


def _post_fireworks_completions(model: str, prompt: str, max_tokens: int, temperature: float, key: str) -> requests.Response:
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "prompt": prompt,
        "max_tokens": int(max_tokens),
        "temperature": float(temperature),
        "stream": False,
    }
    return requests.post(FIREWORKS_URL_COMP, headers=headers, json=body, timeout=60)


def _call_llm(
    prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.2,
    model: Optional[str] = None,
    provider: Optional[str] = None,
) -> str:
    effective_provider = (provider or os.getenv("LLM_PROVIDER") or "fireworks").lower()

    if effective_provider == "fireworks":
        key = os.getenv("FIREWORKS_API_KEY")
        if not key:
            raise RuntimeError("Set FIREWORKS_API_KEY for Fireworks provider")

        mdl = model or FIREWORKS_MODEL

        # 1) Try chat endpoint first
        r = _post_fireworks_chat(mdl, prompt, max_tokens, temperature, key)

        if r.status_code == 200:
            data = r.json()
            text = _extract_text_from_choices(data)
            if text:
                return text
            # If chat returned 200 but no content (some models respond oddly), try completions as a fallback
            rc = _post_fireworks_completions(mdl, prompt, max_tokens, temperature, key)
            if rc.status_code == 200:
                data_c = rc.json()
                text_c = _extract_text_from_choices(data_c)
                if text_c:
                    return text_c
                raise RuntimeError(f"Fireworks completions returned 200 but no text: {json.dumps(data_c)[:800]}")
            raise RuntimeError(f"Fireworks completions error {rc.status_code}: {rc.text}")

        # For common client errors, retry via completions
        if r.status_code in (400, 403, 404, 415, 422):
            rc = _post_fireworks_completions(mdl, prompt, max_tokens, temperature, key)
            if rc.status_code == 200:
                data_c = rc.json()
                text_c = _extract_text_from_choices(data_c)
                if text_c:
                    return text_c
                raise RuntimeError(f"Fireworks completions returned 200 but no text: {json.dumps(data_c)[:800]}")
            raise RuntimeError(f"Fireworks completions error {rc.status_code}: {rc.text}")

        # Any other error from chat endpoint
        raise RuntimeError(f"Fireworks chat error {r.status_code}: {r.text}")

    elif effective_provider in ("hf_router", "hf"):
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
        text = _extract_text_from_choices(data)
        if text:
            return text
        raise RuntimeError(f"HF Router returned 200 but no text: {json.dumps(data)[:800]}")

    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER={effective_provider}")


# Back-compat shim for old callers
def _router_call(
    prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.2,
    model: Optional[str] = None
) -> str:
    return _call_llm(prompt, max_tokens=max_tokens, temperature=temperature, model=model)