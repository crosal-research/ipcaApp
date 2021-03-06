# import from system
from datetime import datetime as dt
import json, os

# import from packages
from pony  import orm

db = orm.Database()

class Series(db.Entity):
    ticker = orm.Required(str, unique=True)
    description = orm.Required(str)
    group = orm.Required(str) #geral, grupo, subgrupo, item, subitem, nucleo, nucleo
    kind = orm.Required(str)  # peso, variaçao
    indicator = orm.Required(str) # ipca, ipca15
    observations = orm.Set('Observation')
    tables = orm.Set('TableDb')


class Observation(db.Entity):
    data = orm.Required(dt)
    series = orm.Required(Series)
    value  = orm.Optional(float)


class TableDb(db.Entity):
    tticker= orm.Required(str, unique=True)
    tbl_description = orm.Required(str)
    series = orm.Set(Series)


# bootstrap db:
with open("./configuration.json") as fp:
    config = json.load(fp)

if config["DB"]["provider"] == 'sqlite':
    dr = os.path.abspath(os.path.curdir)
    db.bind(provider=config["DB"]["provider"], 
            filename= dr + "/DB/storage/" + 'ipca.sqlite', create_db=True)    
else:
    db.bind(provider=config["DB"]["provider"],
            host=config["DB"]["host"],
            port=config["DB"]["port"],
            user=config["DB"]["user"], 
            password=config["DB"]["password"],
            database=config["DB"]["database"])    

db.generate_mapping(create_tables=True)    
