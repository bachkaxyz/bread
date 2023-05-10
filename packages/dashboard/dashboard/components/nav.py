from dash import html

from dashboard.setup import CHAIN_NAME


def NavBar():
    return html.Nav(
        children=[
            html.H1(f"{CHAIN_NAME.capitalize()} Analytics Dashboard"),
            html.Div(
                id="menu",
                children=[
                    NavItem("Transactions", "/txs"),
                    NavItem("Storage", "/storage"),
                ],
            ),
        ]
    )


def NavItem(name: str, link: str):
    return html.A(name, href=link, className="nav-item")
