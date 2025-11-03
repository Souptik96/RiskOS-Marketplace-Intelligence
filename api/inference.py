import os
import json
import requests
from typing import Any, Dict, Optional, List

# ---- Fireworks native (defaults match your .env) ----
FIREWORKS_MODEL = os.getenv("FIREWORKS_MODEL_ID", "accounts/fireworks/models/gpt-oss-20b")
FIREWORKS_URL_CHAT = os.getenv("FIREWORKS_URL_CHAT", "https://api.fireworks.ai/inference/v1/chat/completions")
FIREWORKS_URL_COMP = os.getenv("FIREWORKS_URL_COMP", "https://api.fireworks.ai/inference/v1/completions")

def _norm_content(val) -> Optional[str]:
    if val is None: return None
    if isinstance(val, str): return val.strip()
    if isinstance(val, list):  # sometimes providers return list segments
        parts=[]
        for seg in val:
            if isinstance(seg, str): parts.append(seg)
            elif isinstance(seg, dict):
                txt = seg.get("text") or seg.get("content") or seg.get("value")
                if isinstance(txt, str): parts.append(txt)
        return ("\n".join(p for p in parts if p.strip())) or None
    return None

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

def _fireworks_client() -> OpenAI:
    key = os.getenv("FIREWORKS_API_KEY")
    if not key:
        raise RuntimeError("Set FIREWORKS_API_KEY for Fireworks provider")
    return OpenAI(api_key=key, base_url="https://api.fireworks.ai/inference/v1")

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
    mdl = model or FIREWORKS_MODEL

    if effective_provider == "fireworks":
        client = _fireworks_client()
        # 1) try chat
        try:
            resp: ChatCompletion = client.chat.completions.create(
                model=mdl,
                messages=[{"role":"user","content":prompt}],
                max_tokens=int(max_tokens),
                temperature=float(temperature),
            )
            txt = _norm_content(resp.choices[0].message.content)
            if txt: return txt
        except (BadRequestError, NotFoundError) as e:
            # fall through to completions
            pass
        except AuthenticationError as e:
            raise RuntimeError(f"Fireworks auth error: {e}") from e
        except Exception as e:
            # if chat endpoint hiccups, try completions too
            pass

        # 2) completions fallback
        try:
            comp = client.completions.create(
                model=mdl,
                prompt=prompt,
                max_tokens=int(max_tokens),
                temperature=float(temperature),
            )
            # OpenAI SDK for completions returns .choices[0].text
            text = getattr(comp.choices[0], "text", None)
            if isinstance(text, str) and text.strip():
                return text.strip()
            # last resort: jsonify & parse
            data = comp.model_dump()
            txt = _extract_text_from_choices(data)
            if txt: return txt
            raise RuntimeError(f"Fireworks completions returned no text: {json.dumps(data)[:800]}")
        except Exception as e:
            raise RuntimeError(f"Fireworks completions error: {e} (model='{mdl}')")

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
        txt = _extract_text_from_choices(data)
        if txt: return txt
        raise RuntimeError(f"HF Router returned 200 but no text: {json.dumps(data)[:800]}")

    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER={effective_provider}")


# Back-compat shim for old callers
def _router_call(prompt: str, max_tokens: int = 512, temperature: float = 0.2, model: Optional[str] = None) -> str:
    return _call_llm(prompt, max_tokens=max_tokens, temperature=temperature, model=model)