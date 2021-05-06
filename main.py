# import from system
import json
from typing import List, Optional
import datetime

# import from packages
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# import from app
from DB.transactions import fetch_by_ticker, delete_observations
from DB.transactions import fetch_tbl, delete_tbl, create_tbl, modify_tbl
from DB.transactions import fetch_all_series
from DB.loaders.observations import add_observations
from DB.nucleos_calculos import add_cores

app = FastAPI()

origins = ["http://localhost:5000"]
# origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_headers = ['*'],
    allow_methods = ["GET"])


#classes
class Ind_obs(BaseModel):
    """
    model to modifying, creating observations for a particular
    indicator: refers to all observations at a particular date for one
    indicator [IPCA, IPCA15] at a date
    """
    date: datetime.date = Field(...)
    indicator: str = Field(...)


class Tabledb(BaseModel):
    """
    model for modifying tables
    """
    ticker: str = Field(..., regex="^tbl\..+" )
    description: Optional[str] = Field(None)
    series: Optional[List[str]]= Field(None, regex="^.+\..+")


# http methods
# series
@app.get("/inflation/api/v0.1")
async def index():
    "Landing endpoint"
    return "Hello, this is my api!"


# fetch resources
@app.get("/inflation/api/v0.1/")
async def get_series(tickers: List[str]=Query(..., regex="^.+\..+"),
                     date_ini:datetime.date=Query(None), #, regex="^\d{4}-\d{2}-\d{2}$"), 
                     date_end:datetime.date=Query(None), #, regex="^\d{4}-\d{2}-\d{2}$"), 
                     form:str="csv"):
    """Get observations for a List of tickers from database or for table,
    date gives the head lower limit for the for the time seires and
    from gives the string format (csv or json)
    """
    if (len(tickers) == 1 and tickers[0].split(".")[0] == "tbl"):
        df = fetch_by_ticker(fetch_tbl(tickers[0])["series"], 
                             date_ini= date_ini.isoformat() if date_ini else None, 
                             date_end= date_end.isoformat()  if date_end else None)
    else:
        df = fetch_by_ticker(tickers, 
                             date_ini=date_ini.isoformat() if date_ini else None,
                             date_end=date_end.isoformat() if date_end else None)
    if form == "csv":
        return Response(df.to_csv(), media_type="application/text")
    return Response(df.to_json(), media_type="application/text")


# management of resources
# series
@app.post("/inflation/api/v0.1/")
async def add_obs_indicator(indobs: Ind_obs):
    """
    add all observations for an indicator and a particular time as in indbos
    """
    add_observations(indobs.indicator, (indobs.date).strftime("%Y-%m-%d"))
    add_cores(indobs.indicator, (indobs.date).strftime("%Y-%m-%d"))
    return f"{indobs.indicator} at {indobs.date} successfully added" 


@app.delete("/inflation/api/v0.1/")
async def delete_obs_indicator(indobs: Ind_obs):
    """
    the deletes all observations for an indicator a particular time as in indbos
    """
    r = delete_observations(indobs.indicator, (indobs.date).strftime("%Y-%m-%d"))
    if r == "ok":
        return f"{indobs.indicator} at {indobs.date} successfully deleted"
    return f"{indobs.indicator} at {indobs.date} no longer at the database"


# tables
@app.get("/inflation/api/v0.1/tables")
async def get_table(ticker:str=Query(None)):
    """
    fetches all the info (tickers, description) for a 
    table with ticker
    """
    return fetch_tbl(ticker)


@app.post("/inflation/api/v0.1/tables")
async def create_table(table:Tabledb):
    """
    creates a table with ticker, description, series
    and adds into the database
    """
    status = create_tbl(table.ticker, table.description, table.series)
    if status:
        return f"Table {table.ticker} created!"
    return f"Table {table.ticker} is not in the database"


@app.put("/inflation/api/v0.1/tables")
async def madify_table(table:Tabledb):
    """
    modifies a table by changing either the 
    description or the series in the table
    """
    if (status:=modify_tbl(table.ticker, table.series)):
        return f"Table {table.ticker} modified!"
    return f"Table {table.ticker} not in the database"


@app.delete("/inflation/api/v0.1/tables")
async def delete_table(table:Tabledb):
    """
    deletes a table with a particular ticker
    """
    if (status:=delete_tbl(table.ticker)):
        return f"Table {table.ticker} deleted!"
    return f"Table {table.ticker} not in the database"
