import os
import pathlib
import subprocess
import sys

SCRIPT_PATH = pathlib.Path(__file__).with_name("ui_app.py")

if __name__ == "__main__":
    env = os.environ.copy()
    port = env.get("PORT", "7860")
    env.setdefault("STREAMLIT_SERVER_ADDRESS", "0.0.0.0")
    env.setdefault("STREAMLIT_SERVER_PORT", port)
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(SCRIPT_PATH),
        "--server.headless=true",
        "--server.address=0.0.0.0",
        f"--server.port={port}",
    ]
    subprocess.run(cmd, env=env, check=True)
