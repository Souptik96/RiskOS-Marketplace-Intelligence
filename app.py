import os
import pathlib
import sys

SCRIPT_PATH = pathlib.Path(__file__).with_name("ui_app.py")

if __name__ == "__main__":
    env = os.environ.copy()
    port = env.get("PORT", "7860")
    env.setdefault("STREAMLIT_SERVER_ADDRESS", "0.0.0.0")
    env.setdefault("STREAMLIT_SERVER_PORT", port)
    env.setdefault("STREAMLIT_SERVER_ENABLECORS", "false")
    env.setdefault("STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION", "false")
    args = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(SCRIPT_PATH),
        "--server.headless=true",
        "--server.address=0.0.0.0",
        f"--server.port={port}",
        "--server.enableCORS=false",
        "--server.enableXsrfProtection=false",
    ]
    os.execve(sys.executable, args, env)
