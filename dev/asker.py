import requests

class Asker():
    def __init__(self,url = "https://api.coindesk.com/v1/bpi/currentprice.json"):
        self.url = url
        # get the response
        self.json_response = self.ask()
    def ask(self):
        response = requests.get(self.url)
        # to check that the server responded properly
        response.raise_for_status()
        # print the data in a pretty format
        return response.json()






