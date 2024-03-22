from dev.asker import Asker
from dev.database import PrepareDatabase
from dev.parser import Parser
from dev.inserter import Inserter
from prefect import flow

@flow()
def main():

    # establish the connection to the target database.
    database = PrepareDatabase()
    
    # Ingesting data form the api source.
    result = Asker()

    # filter the data and neglect the data that we are not interested in.
    parser = Parser(response=result.json_response)

    # insert the data to the database
    Inserter(df=parser.paresed_response , engine=database.engine)

if __name__ =="__main__":
    # main()
    main.serve(name="CoinDeskDeployment",
    cron="* * * * *",
    tags=["testing", "tutorial"],
    description="Given a GitHub repository, logs repository statistics for that repo.",
    version="tutorial/deployments")


