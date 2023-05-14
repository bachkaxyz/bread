import os
from dash import Dash, html, dcc
import dash
from dotenv import load_dotenv

from dashboard.config import API_URL, CHAIN_NAME, PORT
from dashboard.components import nav

load_dotenv()

app = Dash(__name__, use_pages=True)

server = app.server

app.layout = html.Div(
    [
        nav.NavBar(),
        dash.page_container,
    ]
)


if __name__ == "__main__":
    print("Restarted Server")
    print("ENVIRONMENT", os.getenv("ENVIRONMENT"))
    print("API_URL", API_URL)
    print("CHAIN_NAME", CHAIN_NAME)
    app.run(
        debug=True if os.getenv("ENVIRONMENT") == "development" else False,
        host="0.0.0.0",
        port=PORT if PORT else "8050",
    )
