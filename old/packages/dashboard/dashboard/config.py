import os

API_URL = os.getenv("API_URL")
if API_URL is None:
    raise Exception("API_URL environment variable not set")

CHAIN_NAME = os.getenv("CHAIN_NAME", "")
if CHAIN_NAME is None:
    raise Exception("CHAIN_NAME environment variable not set")

PORT = os.getenv("DASHBOARD_PORT", "")
