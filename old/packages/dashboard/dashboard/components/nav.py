from dash import html
import dash

from dashboard.config import CHAIN_NAME


def NavBar():
    return html.Nav(
        children=[
            html.H1(f"{CHAIN_NAME.capitalize()} Analytics Dashboard"),
            html.Div(
                id="menu",
                children=[
                    NavItem(
                        name=page["name"],
                        link=page["path"],
                    )
                    for page in dash.page_registry.values()
                ],
            ),
        ]
    )


def NavItem(name: str, link: str):
    return html.A(name, href=link, className="nav-item")
