#! /usr/bin/

# sets up the database
echo "Sets up the database"
python3 -m DB.db
echo "DB setup"
echo -e "-----\n"

# # sets up series into the Database
# echo "Building up inflation series..."
# python3 -m DB.series
# echo "Done with series"
# echo -e "--------\n"


# # sets up core series into the Database
# echo "Build up core series..."
# python3 -m DB.nucleos_series_add
# echo "Done with cores"
# echo -e "--------\n"
# echo "Done with setting up!"

# # sets up of basic tables into the Database
# echo "Build up core series..."
# python3 -m DB.add_tables
# echo "Done with cores"
# echo -e "--------\n"
# echo "Done with setting up!"
