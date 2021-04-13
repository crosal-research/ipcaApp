# import from system
import re, json
from typing import Optional, List
from datetime import datetime as dt
from functools import reduce

# import from packages
import pandas as pd
from pony import orm

#import from app
from DB.db import db
from DB.core_definitions import cores

# series
# upserts 
@orm.db_session
def add_series(ticker:str, description:str, 
               group:str, kind:str, indicator: str) -> None:
    """
    Adds a series to the database, with ticker, descript, group in the 
    cpi, kind (peso/variacao) and indicators (cpi, cpi-15).
    """
    Uticker = ticker.upper()
    Udescription = description.upper()
    Ugroup = group.upper()
    Ukind = kind.upper()
    Uindicator = indicator.upper()

    if (tck:= db.Series.get(ticker=Uticker)) is None:
        db.Series(ticker=Uticker, description=Udescription, 
                  group=group, kind=Ukind, indicator=Uindicator)
        print(f"Series {ticker} added to the Database")
    else:
        tck.description = Udescription
        tck.group = Ugroup
        tck.kind = Ukind
        tck.indicator = Uindicator
        print(f"Series {ticker} updated in the Database")


@orm.db_session        
def add_obs(ticker:str, data:dt, value:Optional[float]) -> None:
    """adds a particular observation for the series given by ticker, for
    a particular data.
    """
    Uticker = ticker.upper()
    if (srs:=db.Series.get(ticker=Uticker)) is not None:
        if (obs:=db.Observation.get(series=srs, data=data)) is None:
            db.Observation(series=srs, data=data, value=value)
        else:
            obs.value = value
    else:
        print(f"Ticker {Uticker} is not in the database")


# Queries
@orm.db_session
def fetch_info_by_ticker(ticker:str) -> pd.DataFrame:
    """
    Fetch the information for a particular item
    """
    obs = orm.select((s.ticker, 
                      s.description, 
                      s.group, 
                      s.kind, 
                      s.indicator) for s in db.Series if s.ticker==ticker.upper())
    return pd.DataFrame(list(obs), 
                        columns=["ticker", 
                                 "description", 
                                 "group", 
                                 "kind", 
                                 'indicator'])


@orm.db_session
def fetch_all(kind="VARIACAO", indicator="IPCA", date:Optional[str] = None):

    date = dt.fromisoformat(date) if date is not None else date
    
    if date is not None:
        dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation
                        if (o.series.kind==kind and 
                            o.series.indicator==indicator and 
                            o.data == date and 
                            o.series.group != "NUCLEO"))
    else:
        dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation
                        if (o.series.kind==kind and 
                            o.series.indicator==ticker and 
                            o.series.group != "NUCLEO"))
    return pd.DataFrame(dd, columns=["date", "change", 
                                     "ticker"]).set_index(["date"]).pivot(values="change", 
                                                                                 columns="ticker")


@orm.db_session
def fetch_group(group="GRUPO", kind="VARIACAO", indicator="IPCA",
                ticker=False, date:Optional[str] = None) -> pd.DataFrame:
    """
    group = [GERAL, GRUPO, SUBGRUPO, ITEM, SUBITEM, NUCLEO]
    kind = [VARIACA0, PESO]
    indicator =[IPCA, IPCA15]
    tickers = Bool is headers should be tickers 
    date = str 
    """
    date = dt.fromisoformat(date) if date is not None else date
    Ugroup = group.upper()
    Uindicator = indicator.upper()
    
    if not ticker:
        if date is not None:
            dd = orm.select((o.data, o.value, o.series.description) for o in db.Observation
                            if (o.series.group==Ugroup and 
                                o.series.kind==kind and 
                                o.series.indicator==Uindicator and 
                                o.data == date))
        else:
            dd = orm.select((o.data, o.value, o.series.description) for o in db.Observation
                            if (o.series.group==Ugroup and 
                                o.series.kind==kind and 
                                o.series.indicator==Uindicator))
        df = pd.DataFrame(dd, columns=["date", "change", 
                                       "description"]).set_index(["date"]).pivot(values="change", 
                                                                                 columns="description")
        if (len(df.columns) > 1):
            if Ugroup !="NUCLEO":
                df.columns = [re.search(r",\s\d+\.(.*)", c).group(1) for c in df.columns]
        else:
            df.columns = ["INDICE GERAL"]
        return df    
    else:
        if date is not None:
            dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation 
                            if (o.series.group==Ugroup and 
                                o.series.kind==kind and 
                                o.series.indicator==Uindicator and 
                                o.data == date))
        else:
            dd = orm.select((o.data, o.value, o.series.ticker) for o in db.Observation 
                            if (o.series.group==Ugroup and 
                                o.series.kind==kind and 
                                o.series.indicator==Uindicator))
        return pd.DataFrame(dd, columns=["date", "change", 
                                         "ticker"]).set_index(["date"]).pivot(values="change", 
                                                                                 columns="ticker")


@orm.db_session
def fetch_by_ticker(tickers: List[str], date: Optional[str]=None) -> pd.DataFrame:
    """
    index = list of indexes
    kind = [VARIACA0, PESO]
    indicator =[IPCA, IPCA15]
    """
    Utickers = [tck.upper() for tck in tickers]
    if date is None:
        print("Is none")
        obs = orm.select((o.data, o.value, o.series.ticker)
                             for o in db.Observation if o.series.ticker in Utickers)
    else:
        dat = dt.fromisoformat(date)
        obs = orm.select((o.data, o.value, o.series.ticker)
                         for o in db.Observation 
                         if ((o.series.ticker in Utickers) and (o.data >= dat)))
    df = pd.DataFrame(data=obs, 
                      columns=["date", 
                               "changes", 
                               "tickers"]).pivot(index="date", columns="tickers", values="changes")
    return df.reindex(Utickers, axis=1)


# deletes
@orm.db_session
def delete_observations(indicator:str, date:str):
    """
    deletes all the obervations of pertaining to a indicator 
    [IPCA, IPCA15] at a particular date
    """
    date = dt.fromisoformat(date)
    obs = orm.select(o for o in db.Observation 
                     if ((o.series.indicator == indicator) and 
                         (o.data == date)) and (o.data))
    if len(obs) >1:
        obs.delete(bulk=True)
        return "ok"
    else:
        return "not available"


# tables
# upserts
@orm.db_session
def create_tbl(tticker:str, description: str, 
               tickers:List[str]) -> None:
    """
    creates a table of name 'name' and with tickers
    """
    Utticker = tticker.upper()
    Utickers = [tck.upper() for tck in tickers]
    if not (tbl:= db.TableDb.get(tticker=Utticker)):
        tbl = db.TableDb(tticker=Utticker,
                         tbl_description=description.upper())
        for tck in Utickers:
            srs = db.Series.get(ticker=tck) 
            tbl.series.add(srs)
        return "ok"
    else:
        return None

@orm.db_session
def modify_tbl(tticker:str, tickers:List[str], 
               description:Optional[str]=None) -> None:
    """
    creates or update (if it is already in the database)
    a table of name 'name' and with tickers
    """
    Utticker = tticker.upper()
    Utickers = [tck.upper() for tck in tickers]
    tbl = db.TableDb.get(tticker=Utticker)
    if tbl:
        tbl.series.clear()
        for tck in Utickers:
            if (srs:=db.Series.get(ticker=tck)):
                tbl.series.add(srs)
        if description:
            tbl.tbl_description = description
        return "ok"
    else:
        return None

# delete
@orm.db_session
def delete_tbl(tticker:str) -> None:
    """
    deletes table 'name' from the database
    """
    if (tbl:=db.TableDb.get(tticker=tticker.upper())):
        tbl.delete()
        return "ok"
    else:
        return None


# fetch
@orm.db_session
def fetch_tbl(tticker:Optional[str]) -> None:
    if tticker:
        Utticker = tticker.upper()
        if (tbl:=db.TableDb.get(tticker=Utticker)):
            srs = [srs.ticker for srs in tbl.series]
            return {"ticker": Utticker, 
                    "description": tbl.tbl_description,
                    "series":  srs}
    else:
        tbls = orm.select(t for t in db.TableDb)
        return [{"ticker": t.tticker, 
                 "description": t.tbl_description, 
                 "series":[s.ticker for s in t.series]} for t in tbls]


# fetch indexes to be ingested by search engine    
@orm.db_session
def fetch_all_series() -> pd.DataFrame:
    srs = orm.select((s.ticker, 
                      s.description, 
                      s.kind,
                      s.group) for s in db.Series)
    lsrs = [{"ticker": s[0], 
             "description": s[1], 
             "group": s[3]} for s in srs]
    
    with open("./configuration.json") as fj:
        fpath = json.load(fj)["Search"]["data"]
    
    with open(fpath, "w") as f:
        json.dump(lsrs, f, ensure_ascii=False)


if __name__ == "__main__":
    fetch_all_series()
