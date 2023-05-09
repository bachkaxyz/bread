import json
import dash
from dash import html, dcc, Input, Output, callback
from dashboard.index import API_URL
import requests
import pandas as pd
import plotly.express as px, plotly.graph_objects as go

dash.register_page(__name__, path="/")

daily = requests.get(f"{API_URL}/txs/daily").json()
daily_df = pd.DataFrame(daily)

cum = requests.get(f"{API_URL}/txs/cumulative").json()
cum_df = pd.DataFrame(cum)

network_providers = requests.get(f"{API_URL}/storage/providers/network").json()
network_providers_df = pd.DataFrame(network_providers)
network_providers_df.set_index("timestamp", inplace=True)

prov_data = requests.get(f"{API_URL}/storage/providers").json()
prov_df = pd.DataFrame(prov_data)

used_space_df = pd.DataFrame.from_records(
    prov_df["usedspace"].apply(json.loads), index=prov_df["timestamp"]
)

prov_count = requests.get(f"{API_URL}/storage/providers/count").json()
prov_count_df = pd.DataFrame(prov_count, columns=["count", "timestamp"])

layout = html.Div(
    children=[
        dcc.Graph(
            figure=px.line(daily_df, x="day", y="tx_count", title="Daily Transactions")
        ),
        dcc.Graph(
            figure=px.line(
                cum_df,
                x="day",
                y="tx_count",
                title="Cumulative Transactions",
            )
        ),
        dcc.Graph(figure=px.line(network_providers_df, title="Network Space")),
        dcc.Graph(figure=px.line(used_space_df, title="Used Space")),
        dcc.Graph(figure=px.line(prov_count_df, title="Provider Count")),
    ]
)
