import os
from dotenv import load_dotenv
import psycopg2


load_dotenv()


DB_NAME = os.getenv('LAMBDA_DB_NAME')
DB_USER = os.getenv('LAMBDA_DB_USER')
DB_PASSWORD = os.getenv('LAMBDA_DB_PASSWORD')
DB_HOST = os.getenv('LAMBDA_DB_HOST')


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                        host=DB_HOST)


cursor = conn.cursor()


cursor.execute("""
CREATE TABLE titanic_data(
                          id SERIAL PRIMARY KEY,
                          survived boolean,
                          pclass smallint,
                          name varchar(100) NOT NULL,
                          sex varchar(40) NOT NULL,
                          age real,
                          siblings_spouses_aboard smallint,
                          parents_children_aboard smallint,
                          fare real)
"""
                )


cursor.execute("""
            COPY titanic_data(survived, pclass, name, sex, age,
            siblings_spouses_aboard, parents_children_aboard, fare)
            FROM 'C:\\Users\\trist\\Desktop\\Lambda\\Unit_3\\DS-Unit-3-Sprint-2-SQL-and-Databases\\module2-sql-for-analysis\\titanic.csv'
            DELIMITER ','
            CSV HEADER;
"""
              )


results = cursor.execute('SELECT * FROM titanic_data')


print(results)


conn.commit()


cursor.close()


conn.close()
