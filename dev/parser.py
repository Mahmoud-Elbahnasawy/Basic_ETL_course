import pandas as pd
from uuid import uuid4

class Parser:
    def __init__(self, response , uuid_from_prefect = str(uuid4())):
        self.response = response
        self.uuid_from_prefect = uuid_from_prefect
        self.paresed_response = self.parse()
    def parse(self):
        parsed_data = []
        # check datatype
        if isinstance(self.response ,dict ):
            
            update_date = (self.response.get('time')).get('updatedISO')

            for currency , details in self.response.get('bpi').items():
                parsed_data.append([currency , details['rate_float'] , update_date , self.uuid_from_prefect  ])
                # print(values_to_be_inserted)

            df = pd.DataFrame(parsed_data, columns =['currency', 'details', 'update_date' , 'uuid'])
            print(df)
            return df

