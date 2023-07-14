# imports from sistem
import re, time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
 

#import from packages
from bs4 import BeautifulSoup as bs

# import from app
from DB.transactions import add_series


t0 = time.time()

data = {7060:{"v":[0, 3], "c":"", "c1": None}, #IPCA
        1419:{"v":[0, 3], "c":"", "c1": None}, #IPCA antigo
        7062:{"v":[0, 3], "c":"", "c1": None}, #IPCA-15 
        1705:{"v":[0, 3], "c":"", "c1": None}} #IPCA-15 antigo


def _group_des(description: str):
    """
    Help funcition to extract the right group the series belong to
    """
    ind = re.search(r",\s\b(\d{1,})\.\D+", description)
    if (ind is None):
        return "GERAL"
    return {1:"GRUPO", 2: "SUBGRUPO", 
            4: "ITEM", 7: "SUBITEM"}[len(ind.group(1))].upper()

def _peso_des(description:str):
    """
    Help funcition to extract the right kind the series belong to
    """
    if "PESO" in description.upper():
        return "PESO"
    return "VARIACAO"


# series to be added to database 
series: list = [] # ex:[ticker:description]

# see: https://github.com/urllib3/urllib3/issues/2653
ctx = create_urllib3_context()
ctx.load_default_certs()
ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
http = urllib3.PoolManager(ssl_context=ctx)

for t in data.keys():
    kind = len(data[t]) - 1
    url = f"http://api.sidra.ibge.gov.br/desctabapi.aspx?c={t}"
    reps = http.request("GET", url)
    html = reps.data
    soup = bs(html,"html.parser")
    group = soup.select("span#lblNomePesquisa")[0].get_text()

    #fetch variables
    if (v:=data[t]["v"]) is not None:
        if v == "":
            nvariables = range(0, int(re.search("\d+", 
                                   soup.select("span#lblVariaveis")[0].get_text()).group()))
        else:
            nvariables = v

    for v in nvariables:
        vnumber = soup.select(f"span#lstVariaveis_lblIdVariavel_{v}")[0].get_text()
        vdescription = soup.select(f"span#lstVariaveis_lblNomeVariavel_{v}")[0].get_text()
        
        #fetch categories c
        if (cl:= data[t]["c"]) is not None:
            classe = soup.select(f"span#lstClassificacoes_lblIdClassificacao_{0}")[0].get_text()
            if cl == "":
                nclasse = range(0,int(re.search("\d+",
                                                soup.select(f"span#lstClassificacoes_lblQuantidadeCategorias_{0}")[0].get_text()).group()))
            else:
                nclasse = cl
                
            for c in nclasse:
                i = {7060:"IPCA", 1419: "IPCA", 7062:"IPCA15", 1705: "IPCA15"}[t]
                ticker = f"IBGE.{i}/p/all/v/{vnumber}"
                tag_id = f"span#lstClassificacoes_lstCategorias_{0}_lblIdCategoria_{c}"
                tag_name = f"span#lstClassificacoes_lstCategorias_{0}_lblNomeCategoria_{c}"
                cdescription = soup.select(tag_name)[0].get_text()
                ind = soup.select(tag_id)[0].get_text()
                ticker = f"IBGE.{i}/p/all/v/{vnumber}/c{classe}/{ind}"
                description = f"{vdescription}, {cdescription}"
                group = _group_des(description)
                kind = _peso_des(description)

                #fetch categories c1
                if (cl1:= data[t]["c1"]) is not None:
                    classe1 = soup.select(f"span#lstClassificacoes_lblIdClassificacao_{1}")[0].get_text()
                    #fetch classes
                    if cl1 == "":
                        nclasse1 = range(0,int(re.search("\d+",
                                                         soup.select(f"span#lstClassificacoes_lblQuantidadeCategorias_{1}")[0].get_text()).group()))
                    else:
                        nclasse1 = cl1
                    for c1 in nclasse1:
                        tag_id = f"span#lstClassificacoes_lstCategorias_{1}_lblIdCategoria_{c1}"
                        tag_name = f"span#lstClassificacoes_lstCategorias_{1}_lblNomeCategoria_{c1}"
                        c1description = soup.select(tag_name)[0].get_text()
                        ind1 = soup.select(tag_id)[0].get_text()
                        description = f"{cdescription}, {c1description}, {vdescription}"
                        ticker = f"IBGE.{i}/p/all/v/{vnumber}/c{classe}/{ind}/c{classe1}/{ind1}"
                        group = _group_des(description)
                        kind = _peso_des(description)
                        series.append([ticker, description, group, kind, i]) 
                else:
                    series.append([ticker, description, group, kind, i])
        else:
            ticker = f"IBGE.{i}/p/all/v/{vnumber}"
            description = f"{vodescription}"
            group = _group_des(description)
            kind = _peso_des(description) 
            series.append([ticker, description, group, kind, i])

sd = {v[0]:[v[1], v[2], v[3], v[4]] for v in series}
srs = [[k, sd[k][0], sd[k][1], sd[k][2], sd[k][3]] for k in sd]

with ThreadPoolExecutor() as executor:
     executor.map(lambda s: add_series(*s), srs)
