############################################################
# last update: 13/02/2023
############################################################

# import from system
import re, time

import json, time
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor as Executor


# import from packges
import requests
import pandas as pd
import numpy as np
import pendulum


# import from app
from DB.transactions import add_obs


# information necessary to build the url to fetch the observation in block
data = {"IPCA":["7060/p/all/v/63,66/c315/all", # new
                "1419/p/all/v/63,66/c315/all"],# old
        "IPCA15":["7062/p/all/v/355,357/c315/all", # new
                  "1705/p/all/v/355,357/c315/all"]} # old


def _build_urls(indicator:str, limit:Optional[str]=None, new:bool=True) -> str:
    """
    Form indicator {IPCA, IPCA-15}, nth-limit last observations, 
    and whether we should fetch the old or new observations of the 
    indicator. Returns the url for that particular request.
    """
    ticker = data[indicator][0 if new else 1]
    tck_new = ticker if limit is None else  ticker.replace("all", 
                                                           f"{limit}", 1)
    def _urls(tck):
        us = tck.split(',')
        return [us[0] + us[1][2:], us[0][0:-2] + us[1]]

    return [f"http://api.sidra.ibge.gov.br/values/t/{u}/n1/1/f/a" for u in _urls(tck_new)] 



def _worker_process(resp: requests.Response) -> Optional[pd.DataFrame]:
    url = resp.url
    tbl = re.search(r"values/t/(\d{4})/", url).group(1)
    indic = {"7060":"IPCA", "1419":"IPCA", "7062":"IPCA15", "1705":"IPCA15"}[tbl]
    rp = b""
    for line in resp.iter_lines():
        rp = rp + line
    df = pd.DataFrame(json.loads(rp.decode("utf-8"))[1:]).loc[:,["D1C", "D2C", "D3C",  "V"]]
    dc = df.apply(lambda x: f"ibge.{indic}/p/all/v/{x[1]}/c315/{x[2]}".upper(), 
                  axis=1).to_frame()
    dc.columns = ["tickers"]
    dn = pd.concat([dc, df.loc[:, ['D1C', 'V']]], axis=1)
    dn["D1C"] = pd.to_datetime(dn["D1C"], format="%Y%m").astype(str)
    dn = dn.replace(to_replace="...", regex=False, value=pd.NA).dropna()
    dn["V"] = dn["V"].apply(lambda v: float(v))
    return dn


def _process(urls:List[str]) -> Optional[pd.DataFrame]:
    """
    From a specific url to acess IBGE's api, fetch the data in a
    dictionary form, and return a dataframe with the necessary information
    and filtered data.
    output:
    pd.DataFrameo
    - index = [1, 2, ...]
    - columns = [ticker, data, value]
    """
    l = 20
    attempt =  0
    while (attempt <= l):
        try:
            with requests.session() as session:
                # adapter = requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=4)
                session.mount('https://apisidra.ibge.gov.br/', requests.adapters.HTTPAdapter())
                with Executor(max_workers=2) as e:
                    resps = e.map(lambda u: session.get(u, stream=True), urls)
                dfs = [_worker_process(resp) for resp in resps]
                return pd.concat(dfs, axis=0)
        except:
            print("Data not yet available")
            time.sleep(0.5)
            attemp += 1
            if attemp > l:
                return None


def fetch(indicator:str, limit:Optional[str]=None, new:bool=True) -> pd.DataFrame:
    """
    Fetches the data for weigh and changes from the IBGE api
    for a specific inflation indicator. Limit is that last=limit observations
    new=True is to fetch the new indicator, from valid from 2020-10. 
    Returns pandas dataframe
    """
    urls = _build_urls(indicator, limit=limit, new=new)
    return _process(urls)


def _add_data_frame(df: pd.DataFrame) -> None:
    """
    takes a data frame with the information about observations on a 
    particular indicator {IPCA, IPCA15} and adds to the database.
    """
    for i in range(0, df.shape[0]):
        try:
            input = df.iloc[i,:].values
            add_obs(input[0], input[1], float(input[2])) # see where to fix date back to string as YYYY-MM-DD
        except Exception as e:
            print(f"failed to add series {df.iloc[i,:].values[0]}")
            print(e)


def add_observations(cpi:str, end:str, ini: Optional[str]=None) -> None:
    """
    cpi is the kind of indicator [IPCA, IPCA15], 
    end is the last time observation to be updated
    and ini is the first
    """
    import pendulum
    cpi = cpi.upper()
    end = pendulum.parse(end)
    init = pendulum.datetime(2012,1,1) if cpi == "IPCA" else pendulum.datetime(2012,2,1)
    if ini is not None:
        ini = pendulum.parse(ini) if pendulum.parse(ini) >= init else init
    else:
        ini = end
    period = pendulum.period(ini, end)
    months = [p.format("YYYYMM") for p in period.range('months')]

    for month in months:
        if cpi == "IPCA":
            if (pendulum.from_format(month, "YYYYMM") <= pendulum.datetime(2019,12,1)):
                df = fetch(cpi, limit=month, new=False)
            else:
                df = fetch(cpi, limit=month, new=True)
        else:
            if (pendulum.from_format(month, "YYYYMM") <= pendulum.datetime(2020,1,1)):
                df = fetch(cpi, limit=month, new=False)
            else:
                df = fetch(cpi, limit=month, new=True)
        _add_data_frame(df)
        print(f"Added for {cpi} and {month}")


if __name__ == "__main__":
    import sys
    print(f"adding {sys.argv[1]} starting from {sys.argv[3]} in the DataBase")
    add_observations(sys.argv[1], sys.argv[2], sys.argv[3])


    # t0=time.time()
    # dg = fetch('IPCA', limit='last', new=True)
    # t1=time.time()
    # print(t1-t0)
