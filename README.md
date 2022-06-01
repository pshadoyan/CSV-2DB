
# CSV-2DB
A simple implementation of a csv-to-database pipeline using a PostgreSQL database hosted on Amazon RDS. 

### Steps:
 1. Clone this repository locally, `git clone git@github.com:pshadoyan/CSV-2DB.git`
 2. Adjust file path to `DATA.csv` in `secrets.py`
 3. Add the correct password to `secrets.py`
 4. Run the program with `python3 main.py`
 5. View database with a tool of your choice

### Dependencies
- pandas : pip install pandas
- psycopg2 : https://www.psycopg.org/docs/install.html
