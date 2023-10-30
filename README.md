# Data Ingestion of Instrument Retailer Orders Data
This small project ingests orders data from an excel sheet into a database.

## User Guide

Use `make run` to run the main data ingestion job.

Use `make test` to run all of the unit tests.

Use `make connect_db` to connect to the db to make ad-hoc queries


## How It Works

The application is run inside a docker container, and the orders database is outputted in the /databases dir on the users' local machine. The main.py file is the entrypoint, which calls everything else. 

Here is the order of operations:
1. **Container Initialisation**: Set up the necessary environment.
2. **Data Quality Checks**: Validate the content of input_data.xlsx in memory.
3. **Database Setup**: Create the necessary DDL for the on-disk orders database.
4. **Data Insertion**: Populate the orders database with data.
5. **Container Termination**: Safely shut down and remove the container. The database is made accessbile on the users' local machine


## Data Model

Star schema was used as the data model. There's one *fact* table: **fact_orders**, and three *dimenstion* tables: **dim_delivery_details**, **dim_product_details**, **dim_payment_details**
