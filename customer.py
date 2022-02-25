import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd

# create class


class SysManagement:
    # init the class template host port user pass
    def _init_(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    # function to create database
    def create_db(self, db_name):
        # connect to the database using the connection string
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            # if connected then create databse using the connection
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE {}".format(db_name))
        # if error then print the error
        except Error as e:
            print("Error while connecting to MySQL", e)
        # preparing a cursor object
        # creating database

    # function to create table
    def create_table(self, db_name, table_name, df):
        # connect to the database using the connection string
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            # if the connection is successful
            if conn.is_connected():
                cursor = conn.cursor()
                # using the cursor to create the table
                cursor.execute("USE {}".format(db_name))
                cursor.execute("CREATE TABLE {}".format(table_name))
                # if error then print the error
        except Error as e:
            print("Error while connecting to MySQL", e)
        # this is the loop to insert the data dataframe into the table
        for i, row in df.iterrows():
            # insert into the table using the cursor with the dataframe take the table name and the dataframe
            var = "INSERT INTO {} VALUES {}".format(
                table_name.split(' ', 1)[0], tuple(row))
            cursor.execute(var)
            print('Record inserted')
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
    # functtion to load data from the table

    def load_data(self, db_name, table_name):
        # connect to the database using the connection string
        conn = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        try:
            # if the connection is successful
            if conn.is_connected():
                cursor = conn.cursor()
                # using the cursor to take all data from table
                cursor.execute(
                    "SELECT * FROM {}.{}".format(db_name, table_name))
                # fetch to result
                result = cursor.fetchall()
                return result
        except Error as e:
            print("Error while connecting to MySQL", e)

    # to convert from csv to dataframe
    def import_csv(self, path):
        return pd.read_csv(path, index_col=False, delimiter=',')


# main function call the class
system = SysManagement()
system.host = 'localhost'
system.port = '3306'
system.user = 'root'
system.password = ''
# store the csv file in the variable
gold_karat_df = system.import_csv('gold_karat.csv')
titanic_df = system.import_csv('titanic.csv')

# customer.create_db('ittrial2022')
# create the table in the database and the dataframe to insert to the table Gold_Karat table
system.create_table(
    'ittrial2022', 'Gold_Karat (karat VARCHAR(255), Age VARCHAR(255), Sell_Price VARCHAR(255))', gold_karat_df)
# create the table in the database and the dataframe to insert to the table Titanic table
system.create_table('ittrial2022', 'Titanic (PassengerId VARCHAR(255), Survived VARCHAR(255), Pclass VARCHAR(255), Name VARCHAR(255), Sex VARCHAR(255), Age VARCHAR(255),SibSp VARCHAR(255),Parch VARCHAR(255),Ticket VARCHAR(255),Fare VARCHAR(255),Cabin  VARCHAR(255),Embarked VARCHAR(255))', titanic_df.fillna(0))
# load data
print(system.load_data('ittrial2022', 'Gold_Karat'))
