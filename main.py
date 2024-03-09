from dev.asker import Asker
from dev.database import PrepareDatabase
from dev.parser import Parser
from dev.inserter import Inserter
def main():
    # establish the connection to the target database.
    database = PrepareDatabase()
    
    # Ingesting data form the api source.
    result = Asker()

    # filter the data and neglect the data that we are not interested in.
    parser = Parser(response=result.json_response)

    # insert the data to the database
    Inserter(df=parser.pareser_response , engine=database.engine)

if __name__ =="__main__":

    main()



