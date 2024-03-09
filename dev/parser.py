import pandas as pd
from uuid import uuid4

class Parser:
    def __init__(self, response , uuid_from_prefect = str(uuid4())):
        self.response = response
        self.uuid_from_prefect = uuid_from_prefect
        self.pareser_response = self.parse()
    def parse(self):
        parsed_data = []
        # check datatype
        if isinstance(self.response ,dict ):
            
            update_date = self.response['time']['updatedISO']

            for currency , details in self.response['bpi'].items():
                values_to_be_inserted = parsed_data.append([currency , details['rate_float'] , update_date , self.uuid_from_prefect  ])
                # print(values_to_be_inserted)
                # yield values_to_be_inserted
            df = pd.DataFrame(parsed_data, columns =['currency', 'details', 'update_date' , 'uuid'])
            print(df)
            return df

