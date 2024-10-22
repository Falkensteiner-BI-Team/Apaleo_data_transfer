import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt

# FMT_Reporting MySQL Server

def get_perproperty(property):
    gross_transactions = APIClient('https://api.apaleo.com/booking/v1/reservations?expand=timeSlices,services&pageSize=5000', get_token()).get_data()
    print(gross_transactions)

    def into_json(filename, file):
        # Convert the data to JSON and save to a file
        with open(filename, 'w') as json_file:
            json.dump(file, json_file, indent=4)

    into_json('market_Segment_2.json', gross_transactions)

get_perproperty("FCZ")
