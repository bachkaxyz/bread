import dash
from dash import html, dcc, Input, Output, callback, ctx
from dashboard.index import API_URL
import requests
import pandas as pd
import plotly.express as px, plotly.graph_objects as go

dash.register_page(__name__, path="/ibc", name="IBC")

daily_cum = requests.get(f"{API_URL}/ibc/transfers/cumulative/daily").json()
daily_volume = requests.get(f"{API_URL}/ibc/transfers/volume/daily").json()

layout = html.Div(
    children=[
        html.Div(
            children=[
                dcc.Graph(
                    figure=px.line(
                        daily_cum,
                        x="day",
                        y="cum_amount_over_direction",
                        title="Cumulative IBC Transfers",
                        color="transfer_denom",
                    ).update_layout(
                        xaxis_title="Time",
                        yaxis_title="Amount Transfered",
                    ),
                    id="ibc_cum",
                ),
                dcc.Graph(
                    figure=px.line(
                        daily_volume,
                        x="day",
                        y="total_amount_over_direction",
                        title="IBC Transfers Volume",
                        color="transfer_denom",
                    ).update_layout(
                        xaxis_title="Time",
                        yaxis_title="Amount Transfered",
                    ),
                    id="ibc_cum",
                ),
            ]
        )
    ]
)
