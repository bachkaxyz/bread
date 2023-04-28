import os
from dash import Dash, html, dcc
import dash
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")
if API_URL is None:
    raise Exception("API_URL environment variable not set")

CHAIN_NAME = os.getenv("CHAIN_NAME")
if CHAIN_NAME is None:
    raise Exception("CHAIN_NAME environment variable not set")
app = Dash(__name__, use_pages=True)

app.layout = html.Div(
    [
        html.H1(f"{CHAIN_NAME.capitalize()} Analytics Dashboard"),
        dash.page_container,
    ]
)

server = app.server

if __name__ == "__main__":
    print("Restarted Server")
    print("ENVIRONMENT", os.getenv("ENVIRONMENT"))
    print("API_URL", API_URL)
    print("CHAIN_NAME", CHAIN_NAME)
    app.run(
        debug=True if os.getenv("ENVIRONMENT") == "development" else False,
        host="0.0.0.0",
    )
