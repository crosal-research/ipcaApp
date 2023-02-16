# import from system
from typing import Optional
import re

# import from packages
import pandas as pd
import numpy as np
from datetime import datetime as dt
from pony import orm

#import from app
from DB.transactions import fetch_group, fetch_all, add_obs
from DB.core_definitions import cores
from DB.core_definitions_2012 import cores2012


def p55(date, dv_: pd.DataFrame, dp_: pd.DataFrame) -> float:
    """
    Function to calculate the P55 core.
    input:
    - dv_: data frame with changes by subitem
    - dp_: data frame with weigh by subitem
    series do BC: BCB.28750
    """
    global df
    if (dv_.shape != dp_.shape):
        raise("P55: Dimensions of Weights and changes ought to be the same")
    dv = dv_.copy()
    dp = dp_.copy()
    dv.columns = [tck.split("/")[-1] for tck in dv.columns]
    dp.columns = [tck.split("/")[-1] for tck in dv.columns]
    df = (dv.loc[[date],:].T).merge(dp.loc[[date],:].T, 
                                  left_index=True, right_index=True, how="inner")
    df.columns = ["changes", "weights"]
    df.sort_values(by=["changes"], inplace=True)
    df["wcum"] = df["weights"].cumsum()
    return df[(df["wcum"] > 55)].iloc[0,[0]].values[0]


def difusao(date:str, dv_:pd.DataFrame) -> float: 
    """"
    Calculates the diffusion index for a particular 
    month, where dv is a dataframe with all price changes of
    subindexes for that month 
    - dv_: dataframe with changes
    """
    dv = dv_.copy()
    return ((dv[dv > 0].count(axis=1).values)/(dv.count(axis=1).values))[0]


def core_ma(date:str, dv_:pd.DataFrame, dp_:pd.DataFrame) -> float: 
    """
    dv and dp are dataframe with changes and weights for items
    columns of the dataframe outgh to be given by tickers
    """
    dv = dv_.copy()
    dp = dp_.copy()
    dv.columns = [tck.split("/")[-1] for tck in dv.columns]
    dp.columns = [tck.split("/")[-1] for tck in dv.columns]
    if (dv.shape != dp.shape):
        raise("Core MA: Dimensions of Weights and changes ought to be the same")
    df = (dv.loc[[date]].T).merge(dp.loc[[date]].T, 
                                left_index=True, 
                                  right_index=True, 
                                  how="inner")
    df.columns = ["changes", "weights"]
    df.sort_values(by=["changes"], inplace=True)
    df["wcum"] = df["weights"].cumsum()
    df_ = df[(df["wcum"] >= 20) & (df["wcum"] <= 80)]
    index_inf = df.index.get_loc(df_.index[0])-1
    df_.loc[df_.index[0]]['weights'] -=  20 - df.iloc[index_inf]['wcum'] 
    df_.loc[df_.index[-1]]['weights'] +=  60 - (df_['weights'].sum())  
    return df_.iloc[:, 0].mul(df_.iloc[:,1]).sum()/(df_.iloc[:, 1].sum())
 

def core_smooth(date:str, dv_:pd.DataFrame, dp_:pd.DataFrame) -> float:
    """
    calculates the smooth trimmed core for a particular data.
    dv and dp are the dataframes for changes and weights, where group
    is ITEM. columns of the dataframe outgh to be given by tickers
    """
    dv = dv_.copy()
    dp = dp_.copy()
    if date < "2013-01-01":
        return np.NaN
    tickers = [tck for tck  in dv.columns if int(tck.split("/")[-1]) in cores['SUAVIZADOS']]
    dr = dv.loc[:, tickers].rolling(window=12).apply(lambda x: (np.prod(1+x/100)), raw=True)
    dmom = pd.DataFrame(dr.applymap(lambda x: (x-1)/12)*100)
    for ind in tickers:
        dv.loc[date, ind] = dmom.loc[date, ind]
    return core_ma(date, dv, dp)


def core_adhoc(core: str, dv_: pd.DataFrame, 
               dp_: pd.DataFrame, date:str) -> float:
    """
    calculates any of the adhoc cores, taking as input 
    core (str), dataFrames with core components for changes and 
    weights for given period for all components and subcomponents 
    of the index
    """
    dv = dv_.copy()
    dp = dp_.copy()
    if core != "LIVRES":
        indexes = cores[core] if dt.fromisoformat(date) >=  dt.fromisoformat("2020-01-01") else cores2012[core]
        tickersv = [re.split("\d+$", dv.columns[0])[0] + str(s) for s in indexes]
        tickersp = [re.split("\d+$", dp.columns[0])[0] + str(s) for s in indexes]
        return np.average(dv.loc[:,  dv.columns.isin(tickersv)].dropna().T, 
                          weights=dp.loc[:, dp.columns.isin(tickersp)].dropna().T)
    else:
        indexes = cores["MONITORADOS"] if dt.fromisoformat(date) >=  dt.fromisoformat("2020-01-01") else cores2012["MONITORADOS"]
        tv = [re.split("\d+$", dv.columns[0])[0] + str(s) for s in indexes]
        tp = [re.split("\d+$", dp.columns[0])[0] + str(s) for s in indexes]
        tickersv = [re.split("\d+$", dv.columns[0])[0] + str(s) for s in indexes]
        tickersp = [re.split("\d+$", dp.columns[0])[0] + str(s) for s in indexes]
        sm = (dp.loc[:, dp.columns.isin(tickersp)].dropna().sum(axis=1)/100).values
        core_moni = (np.average(dv.loc[:, dv.columns.isin(tickersv)].dropna().T, 
                                weights=dp.loc[:, dp.columns.isin(tickersp)].dropna().T))
        return ((dv.loc[:, re.split("\d+$", dv.columns[0])[0] + str(7169)].values[0] - sm * core_moni)/(1-sm))[0]
        

def core_dp(date, dv_:pd.DataFrame, dp_:pd.DataFrame, 
            dg:pd.DataFrame)->float:
    """
    calculates doubles weighted core. dg is dataframe with 
    history of changes of the cpi and dv and dp are 
    the changes and weights for items, respectively
    """
    dv = dv_.copy()
    dp = dp_.copy()
    if date < "2015-01-01":
        return np.NaN
    d = dt.strptime(date, "%Y-%m-%d")
    begin = d + pd.DateOffset(years=-4) #intial period std
    end = d + pd.DateOffset(months=-1)  # final period for std
    # recalculate weights
    sipca = dv.loc[begin:end]
    dobs = dg.loc[begin:end]
    net = sipca.subtract(dobs.iloc[:,0], axis="index")
    std = 1/net.std()
    sm_std = std/std.sum()*100
    sm_std.index = dp.columns
    new_std = sm_std*(dp.loc[date])
    new_sm = (new_std /(new_std.sum())).dropna()
    return np.average(dv.loc[date].dropna(), 
                      weights=new_sm.T)


def core_dp(date, dv_:pd.DataFrame, dp_:pd.DataFrame, 
            dg:pd.DataFrame)->float:
    """
    calculates doubles weighted core. dg is dataframe with 
    history of changes of the cpi and dv and dp are 
    the changes and weights for items, respectively
    """
    dv = dv_.copy()
    dp = dp_.copy()
    if date < "2015-01-01":
        return np.NaN
    d = dt.strptime(date, "%Y-%m-%d")
    begin = dt.strftime(d + pd.DateOffset(years=-4), "%Y-%m-%d") #intial period std
    end = dt.strftime(d + pd.DateOffset(months=-1), "%Y-%m-%d")  # final period for std
    # recalculate weights
    sipca = dv.loc[begin:end]
    dobs = dg.loc[begin:end]
    net = sipca.subtract(dobs.iloc[:,0], axis="index")
    std = 1/net.std()
    sm_std = std/std.sum()*100
    sm_std.index = dp.columns
    new_std = sm_std*(dp.loc[date])
    new_sm = (new_std /(new_std.sum())).dropna()
    return np.average(dv.loc[date].dropna(), 
                      weights=new_sm.T)


def add_cores(cpi:str, end:str, ini: Optional[str]=None) -> None:
    """"
    Add to the database 
    """
    cpi = cpi.upper()
    dvi = fetch_group(group="ITEM", ticker=True, indicator=cpi) # all items for all dates
    dpi = fetch_group(group="ITEM", kind= "PESO", ticker=True, indicator=cpi) # all item for all dates
    dg = fetch_group(group="GERAL", indicator=cpi) #
    
    import pendulum
    end = pendulum.parse(end)
    if ini is None:
        ini = end
    else:
        if pendulum.parse(ini) >= pendulum.datetime(2015,1,1):
            ini = pendulum.parse(ini)
        else:
            ini = pendulum.datetime(2015,1,1)
    period = pendulum.period(ini, end)
    months = [p.format("YYYY-MM-DD") for p in period.range('months')]
    for month in months:
        dvs = fetch_all(kind="VARIACAO", indicator=cpi, date_ini=month, date_end=month) # needs to fetch all indices for addhoc
        dps = fetch_all(kind= "PESO", indicator=cpi, date_ini=month, date_end=month) # needs to fetch all indices for addhoc
        dvsub = fetch_group(group="SUBITEM", kind="VARIACAO", date_ini=month, 
                            date_end=month, indicator=cpi, ticker=True) # needs to fetch all indices
        dpsub = fetch_group(group="SUBITEM", kind="PESO", date_ini=month, 
                            date_end=month, indicator=cpi, ticker=True) # needs to fetch all indices

        #cores
        aggr_cores = [p55(month, dvsub, dpsub),
                      difusao(month, dvsub), 
                      core_ma(month, dvi, dpi), 
                      core_smooth(month, dvi, dpi), #history dependent
                      core_dp(month, dvi, dpi, dg), #history dependent
                      core_adhoc("EXO", dvs, dps, month),
                      core_adhoc("EX1", dvs, dps, month),
                      core_adhoc("EX2", dvs, dps, month),
                      core_adhoc("EX3", dvs, dps, month),
                      core_adhoc("MONITORADOS", dvs, dps, month),
                      core_adhoc("LIVRES", dvs, dps, month),
                      core_adhoc("TRADABLE", dvs, dps, month),
                      core_adhoc("DURAVEIS", dvs, dps, month),
                      core_adhoc("SERVICOS", dvs, dps, month),
                      core_adhoc("SERVICOS_CORE", dvs, dps, month),
                      core_adhoc("INDUSTRIAIS", dvs, dps, month)]

        cores_tickers = [f"{cpi}.core_p55", f"{cpi}.core_difusao", f"{cpi}.core_aparadas", f"{cpi}.core_aparadas_suavizadas", f"{cpi}.core_dp", 
                         f"{cpi}.core_EXO", f"{cpi}.core_EX1", f"{cpi}.core_EX2", f"{cpi}.core_EX3", 
                         f"{cpi}.core_monitorados", f"{cpi}.core_livres", f"{cpi}.core_tradables", f"{cpi}.core_duraveis", 
                         f"{cpi}.servicos", f"{cpi}.core_servicos", 
                         f"{cpi}.core_industriais"]

        df = pd.DataFrame(data=[aggr_cores],
                          columns=[c.upper() for c in cores_tickers], index=[month]).dropna(axis=1)
        for col in df.columns:
            add_obs(col, df.index[0], 
                    df.loc[:, col].values[0])
        
    print(f"Cores for {cpi} and {month} added to the database")
    

if __name__ == "__main__":
    import sys
    add_cores(sys.argv[1], sys.argv[2], sys.argv[3])
