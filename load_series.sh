#! /usr/bin/

date_initial="2012-01-01"    # initial date
date_final="2021-02-01"      # final date

add inflation series
for inf in "IPCA" "IPCA15"; do
    python3 -m DB.observations $inf $date_final $date_initial
done

# add core series
for inf in "IPCA" "IPCA15"; do
    python3 -m DB.nucleos_calculos $inf $date_final $date_initial
done

python3 -m DB.transactions
