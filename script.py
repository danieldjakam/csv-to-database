from numpy import NaN
import pandas as pd
import mysql.connector
from mysql.connector import Error
from decimal import Decimal

data = pd.read_csv(r'fid.csv')
df = pd.DataFrame(data)

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='fid',
                                         user='root',
                                         password='')

    
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE delivery (
                id int(16) primary key auto_increment,
                Bureau varchar(255),
                Unit varchar(255),
                OU varchar(255),
                Category varchar(255),
                SubCategory varchar(255),
                Type varchar(255),
                Amount DOUBLE,
                Comments varchar(255)
                )
            ''')
    # Insert DataFrame to Table
        for row in df.itertuples():
            comment = ''
            if row.Comments != NaN:
                comment = row.Comments
            

            mySql_insert_query = """INSERT INTO delivery (Bureau, Unit, OU, Category, SubCategory, Type, Amount, Comments) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            record = (row.Bureau, row.Unit, row.OU, row.Category, row.SubCategory, row.Type, float(row.Amount.replace(',' , '.')), comment)
            cursor.execute(mySql_insert_query, record)
        connection.commit()
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        connection.close()
        connection.close()
        print("MySQL connection is closed")