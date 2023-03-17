import os
from dash import Dash, html, dcc
import dash
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL")

app = Dash(__name__, use_pages=True)


app.layout = html.Div(
    [
        html.H1("Jackal Analytics Dashboard"),
        dash.page_container,
    ]
)

server = app.server

if __name__ == "__main__":
    print("Restarted Server")
    app.run(debug=True if os.getenv("ENV") == "development" else False, host="0.0.0.0")
