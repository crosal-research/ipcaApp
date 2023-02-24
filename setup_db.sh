#!/usr/bin/bash
date_initial="2012-01-01"    # initial date
date_final="2023-01-01"      # final date
dir_name="$(dirname $realpath $0)"
cd "${dir_name}"

case $1 in
    "requirements" ) 
	# install requirments
	echo "Installs Requirements"
	pip3 install -r requirements.txt
	echo "Done install requirements"
	echo -e "-----\n"
		     ;;
    "build" ) 
	# sets up the database
	echo "Sets up the database"
	if ! [[ -d "${dir_name}/DB/storage/" ]]; then
	    echo "creating .${dir_name}/DB/storage directory"
	    sleep 1.0
	    mkdir "./DB/storage"
	    python3 "DB/db.by" 
	else
	    if ! [[ -f "${dir_name}/DB/storage/ipca.sqlite" ]]; then
		python3 -m "DB.db"
		echo "Done Creating DB"
		echo -e "-----\n"
	    else
		echo "DB has alreay been built"
	    fi
	fi
	    # build series
	    echo "Load series..."
	    python3 -m "DB.loaders.series"
	    echo "Done with series"
	    echo -e "--------\n"
	    sleep 2.0

	    	    #
	    # build cores
	    echo "Build up core series..."
	    python3 -m "DB.loaders.nucleos_series_add"
	    echo "Done with cores"
	    echo -e "--------\n"
	    

	    # Create Summary tables
	    echo "Build up built-in tables..."
	    python3 -m "DB.loaders.add_tables"
	    echo "Done with tables"
	    echo -e "--------\n"
	    echo "Done with setting up!"
	;;

    "loads" ) 
	# sets up series into the Database
	echo "Loanding inflation series..."
	if ! [[ -f "${dir_name}/DB/storage/ipca.sqlite" ]]; then
	    echo "Database is not available. Build it up first"
	else
	    # laod inflation series
	    for inf in "IPCA" "IPCA15"; do
		python3 -m "DB.loaders.observations" $inf $date_final $date_initial
	    done
	    # add core series
	    for inf in "IPCA" "IPCA15"; do
		python3 -m "DB.nucleos_calculos" $inf $date_final $date_initial
	    done
	    # python3 -m DB.transactions # just in case there is a search machine on
	fi
	;;

    "run" ) 
	# uvicorn main:app --host 127.0.0.1 --port 8000 --reload
	python3 -m bootstrap
	;;

    * ) echo "type either requirements, build, loads"
esac	



