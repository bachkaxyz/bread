import os

from api.database import database
from fastapi import FastAPI

from api.routers import txs
from dotenv import load_dotenv

app = FastAPI()

app.include_router(txs.router)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/")
async def root():
    return {"message": "Hello!"}
