import json
import dash
from dash import html, dcc, Input, Output, callback
from dashboard.index import API_URL
import requests
import pandas as pd
import plotly.express as px, plotly.graph_objects as go, plotly.subplots as sp

dash.register_page(__name__, path="/storage")

network_providers = requests.get(f"{API_URL}/storage/providers/network").json()
network_providers_df = pd.DataFrame(network_providers)
network_providers_df.set_index("timestamp", inplace=True)
network_providers_df.sort_index(inplace=True)

# prov_data = requests.get(f"{API_URL}/storage/providers").json()
# prov_df = pd.DataFrame(prov_data)

# used_space_df = pd.DataFrame.from_records(
#     prov_df["usedspace"].apply(json.loads), index=prov_df["timestamp"]
# )

prov_count = requests.get(f"{API_URL}/storage/providers/count").json()

buy_storage = requests.get(f"{API_URL}/storage/buys").json()

cum_buy_storage = requests.get(f"{API_URL}/storage/buys/cumulative").json()

layout = html.Div(
    children=[
        dcc.Graph(
            figure=px.line(network_providers_df, title="Network Space").update_layout(
                xaxis_title="Time",
                yaxis_title="Space in Terabytes",
            )
        ),
        # dcc.Graph(figure=px.line(used_space_df, title="Used Space By Provider")),
        dcc.Graph(
            figure=px.line(
                prov_count, y="count", x="timestamp", title="Provider Count"
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="Number of Providers",
            )
        ),
        dcc.Graph(
            figure=px.line(
                buy_storage,
                y="transfer_amount",
                x="timestamp",
                title="JKL Spent on Storage",
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="JKL Spent on Storage",
            ),
        ),
        dcc.Graph(
            figure=px.line(
                cum_buy_storage,
                y="transfer_amount",
                x="timestamp",
                title="JKL Spent on Storage",
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="JKL Spent on Storage",
            ),
        ),
        dcc.Graph(
            figure=px.line(
                cum_buy_storage,
                y="message_sender",
                x="timestamp",
                title="Total Users",
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="Number of Users",
            ),
        ),
    ]
)
