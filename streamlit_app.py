"""Streamlit Entrypoint for Chat Application.

Allows hosting this FastAPI + WebSocket Chat Application on Streamlit Community Cloud
for 100% free with one-click GitHub deployment.
"""

import os
import threading
import time
import streamlit as st
import streamlit.components.v1 as components
from fakeredis import TcpFakeServer
import uvicorn

# Page Configuration
st.set_page_config(
    page_title="Chat-Paglu",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom styling to make the iframe fill 100% of the screen
st.markdown(
    """
    <style>
        .block-container {
            padding: 0 !important;
            max-width: 100% !important;
        }
        header[data-testid="stHeader"] {
            display: none !important;
        }
        footer {
            display: none !important;
        }
        iframe {
            width: 100vw !important;
            height: 100vh !important;
            border: none !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def start_backend():
    """Start local Redis and FastAPI servers in background threads once."""
    # 1. Start In-Memory Redis
    def run_redis():
        try:
            server = TcpFakeServer(("127.0.0.1", 6379))
            server.serve_forever()
        except Exception:
            pass

    redis_thread = threading.Thread(target=run_redis, daemon=True)
    redis_thread.start()
    time.sleep(0.5)

    # 2. Start FastAPI ASGI Server
    os.environ["WORKER_ID"] = "streamlit-cloud"
    os.environ["PORT"] = "8000"
    os.environ["REDIS_URL"] = "redis://127.0.0.1:6379/0"

    from app.main import app

    def run_fastapi():
        uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")

    api_thread = threading.Thread(target=run_fastapi, daemon=True)
    api_thread.start()
    time.sleep(1)
    return True


# Ensure backend is started
start_backend()

# Read the HTML, CSS, and JS files to render directly
static_dir = os.path.join(os.path.dirname(__file__), "static")
with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
    html_content = f.read()

with open(os.path.join(static_dir, "style.css"), "r", encoding="utf-8") as f:
    css_content = f.read()

with open(os.path.join(static_dir, "app.js"), "r", encoding="utf-8") as f:
    js_content = f.read()

# Inline CSS and JS into HTML for seamless Streamlit embedding
embedded_html = html_content.replace(
    '<link rel="stylesheet" href="/static/style.css" />',
    f"<style>{css_content}</style>",
).replace(
    '<script src="/static/app.js"></script>',
    f"<script>{js_content}</script>",
)

# Render full-screen chat UI
components.html(embedded_html, height=850, scrolling=False)

