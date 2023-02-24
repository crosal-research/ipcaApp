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
    data = orm.Required(str)
    series = orm.Required(Series)
    value  = orm.Optional(float)
    orm.composite_key(data, series)


class TableDb(db.Entity):
    tticker= orm.Required(str, unique=True)
    tbl_description = orm.Required(str)
    series = orm.Set(Series)


# bootstrap db:
import pathlib
dr = str(pathlib.Path(__file__).parent.resolve())

with open(dr + "/../configuration.json") as fp:
    config = json.load(fp)
    db.bind(provider=config["DB"]["provider"], 
            filename= dr + "/storage/" + 'ipca.sqlite', create_db=True)    
    db.generate_mapping(create_tables=True)    
