#!/usr/bin/bash

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
	python3 -m DB.db
	echo "DB setup"
	echo -e "-----\n"

	   ;;
    "series" ) echo "load prices"
	       ;;
    "cores" ) echo "calculate cores"
	      ;;
    * ) echo "type either requirements, build, series, cores"
			 
esac	

# install requirments
# echo "Installs Requirements"
# pip3 install -4 requirements.txt
# echo "Done install requirements"
# echo -e "-----\n"



# sets up series into the Database
# echo "Building up inflation series..."
# python3 -m DB.loaders.series
# echo "Done with series"
# echo -e "--------\n"


# # sets up core series into the Database
# echo "Build up core series..."
# python3 -m DB.loaders.nucleos_series_add
# echo "Done with cores"
# echo -e "--------\n"
# echo "Done with setting up!"


# # sets up of basic tables into the Database
# echo "Build up built-in tables..."
# python3 -m DB.loaders.add_tables
# echo "Done with cores"
# echo -e "--------\n"
# echo "Done with setting up!"
