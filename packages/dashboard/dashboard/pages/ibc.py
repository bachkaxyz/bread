import dash
from dash import html, dcc, Input, Output, callback, ctx
from dashboard.index import API_URL
import requests
import pandas as pd
import plotly.express as px, plotly.graph_objects as go

dash.register_page(__name__, path="/ibc", name="IBC")

daily_cum = requests.get(f"{API_URL}/ibc/transfers/cumulative/daily").json()
daily_cum_df = pd.DataFrame(daily_cum)

layout = html.Div(
    children=[
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Button("Daily", id="ibc_cum_daily"),
                        html.Button("Hourly", id="ibc_cum_hourly"),
                    ]
                ),
                dcc.Graph(
                    figure=px.line(
                        daily_cum_df,
                        x="day",
                        y="cum_amount_over_direction",
                        title="Cumulative IBC Transfers",
                        color="transfer_denom",
                    ),
                    id="ibc_cum",
                ),
            ]
        )
    ]
)
