import pathlib
from streamlit.web import bootstrap

SCRIPT_PATH = pathlib.Path(__file__).with_name("ui_app.py")

if __name__ == "__main__":
    bootstrap.run(str(SCRIPT_PATH), "", [], {})
