import pyodbc
import sqlalchemy
from dotenv import load_dotenv
import os
# use the environment variables
load_dotenv()

class PrepareDatabase:
    # you can change you the server,or database to the name in your machine.
    def __init__(self,server = os.getenv("server") , database = "Y_Etl_Course" , driver = '{SQL Server}' ,
                 user = os.getenv("user_name") , pwd = os.getenv("password")   ):
        self.server = server
        self.database = database
        self.driver= driver
        self.user = user
        self.pwd = pwd
        # connect to the master database to check for the existence of a the database that we want to work with which has the name of 
        # the default value of the database parameter in the __init__ method
        self.connection = self.get_conn()
        # see if the database that we want to work with already exists
        self.search_database()
        # get the sql alchemy engine which is needed to insert the data from a pandas dataframe to mssql
        self.engine = self.get_sql_alchemy_conn()

    def build_conn_string(self , database = 'master'):
        """This method build the connection string for a certain database having a default value for the database,
          which is master that always exists for mssql"""
        connection_string = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={database};UID={self.user};PWD={self.pwd}'
        return connection_string

    def get_conn(self,database = 'master'):
        """This method connects to a desired database and returns the connection object from which we can create a cursor to execute sql."""
        try:
            conn = pyodbc.connect(self.build_conn_string(database=database) , autocommit = True)
            # setting the connection attribute of this class to the recently created database.
            self.connection = conn
            
            return conn
        except Exception as e:
            print(e)
            raise

    def search_database(self):
        """This method seaches for the database that we want to work with and calls the method needed to create this database if not existed."""
        # if the database does not exist we want to create it.
        sql = f"""select name from sys.databases where name = '{self.database}' """
        cursor = self.connection.cursor()
        cursor.execute(sql)
        # the number of the returned result set
        returned_row_counts = len(cursor.fetchall()) 
        if returned_row_counts:
            print(f"THE DATABASE ({self.database}) EXISTS.")
        else:   
            # create the database with neccesary schemas.
            self.create_database()
            self.create_neccessay_schemas()
    
    def create_database(self):
        """This method creates a database"""
        create_database_statement = F"""CREATE DATABASE {self.database}"""
        self.connection.cursor().execute(create_database_statement)
        print(f"{self.database} WAS SUCCESSFULLY CREATED.")
        # connect to the recently created database.
        conn = self.get_conn(database= self.database)
        return conn
    
    def get_sql_alchemy_conn(self):
        """This method connects to sql alchemy and returns an engine which is needed for pandas 
        to_sql method to insert the date from a dataframe to a database"""
        try:
            engine = sqlalchemy.create_engine(f"mssql+pyodbc://{self.user}:{self.pwd}@{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server")
            return engine
        except Exception as e:
            print(e)
            raise

    def create_neccessay_schemas(self):
        """This method creates the needed schemas for this demo"""
        schemas_list = ["STAGING","ZERO","DWH"]
        for schema in schemas_list:
            sql = f"create schema {schema}"
            try:
                self.connection.cursor().execute(sql)
                print(f"Schema {schema} has been created successfully.")
            except Exception as e:
                print(e)
                raise



        


