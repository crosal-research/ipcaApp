# import from app
from DB.transactions import add_series


for cpi in ["IPCA", "IPCA15"]:
    nucleos = {f"{cpi}.core_EXO": 
              ["nucleo exclusão, EX0", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX1": 
              ["nucleo exclusão, EX1", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX2": 
              ["nucleo exclusão, EX2", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_EX3": 
              ["nucleo exclusão, EX3", 
               "nucleo",
               "variacao",
               f"{cpi}"],
               f"{cpi}.servicos": 
              [" Items servicos", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_servicos": 
              ["nucleo servicos", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_duraveis": 
              ["nucleo duravies", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_tradables": 
              ["nucleo comercializaveis", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_difusao": 
              ["nucleo difusao", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_p55": 
              ["nucleo p55, Banco Central", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_aparadas": 
              ["nucleo de media aparadas, Banco Central", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_aparadas_suavizadas": 
              ["nucleo de media aparadas suavizadas", 
               "nucleo",
               "variacao",
               f"{cpi}"],
              f"{cpi}.core_dp": 
              ["nucleo de dupla ponderacao", 
               "nucleo",
               "variacao",
               f"{cpi}"], 
              f"{cpi}.core_monitorados": 
              ["nucleo bens monitorados", 
               "nucleo",
               "variacao",
               f"{cpi}"],
               f"{cpi}.core_livres": 
              ["nucleo bens livres", 
               "nucleo",
               "variacao",
               f"{cpi}"], 
               f"{cpi}.core_industriais": 
              ["nucleo bens Industriais", 
               "nucleo",
               "variacao",
               f"{cpi}"]}
    for k in nucleos:
        add_series(*[n.upper() for n in [k, *nucleos[k]]])
