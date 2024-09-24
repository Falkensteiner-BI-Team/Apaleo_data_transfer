import userdata
import mysql.connector
from APIClient import *
import MySQLdb
import datetime


###### script must be run once a year .. at the first day of the next year #
# FMT_Reporting MySQL Server

connection_target_azure = MySQLdb.connect(
      host=userdata.azure_host(),
      database=userdata.azure_database(),
      user=userdata.azure_user(),
      password=userdata.azure_password(),
      charset='utf8',  # Optional: Specify character encoding
      ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}  # Path to CA certificate
  )


cursor_target_azure = connection_target_azure.cursor()

Insert_qry = "insert into `fmtg-api-middleware`.resid_numid_mapping(Apaleo_res_ID,Status) values(%s,%s)"
def CreatIDSFB(start_date, end_date):
    properties = ['AA',	'AD',	'AS',	'BA',	'BE',	'BL',	'BW',	'CA',	'CB',	'CL',	'CP',	'CR',	'CZ',	'DI',	'DO',	'EH',	'EW',	'FB',	'FC',	'FH',	'FK',	'HS',	'IA',	'JA',	'JE',	'KO',	'KP',	'LE',	'LK',	'MB',	'MG',	'MO',	'MP',	'PE',	'PP',	'PS',	'PV',	'QM',	'SA',	'SE',	'SG',	'SP',	'ST',	'SV',	'TC',	'WM',	'WU']

    # Iterate over every property and print every day in 2024
    for property in properties:
        current_date = start_date  # Reset current_date for each property
        while current_date <= end_date:
            id = f"{property}F&B{current_date}"
            print(id)

            cursor_target_azure.execute(Insert_qry, (id,"CheckedOut",))
            current_date += datetime.timedelta(days=1)





def CreatIDSSPA(start_date, end_date):
    properties = ['AA',	'AD',	'AS',	'BA',	'BE',	'BL',	'BW',	'CA',	'CB',	'CL',	'CP',	'CR',	'CZ',	'DI',	'DO',	'EH',	'EW',	'FB',	'FC',	'FH',	'FK',	'HS',	'IA',	'JA',	'JE',	'KO',	'KP',	'LE',	'LK',	'MB',	'MG',	'MO',	'MP',	'PE',	'PP',	'PS',	'PV',	'QM',	'SA',	'SE',	'SG',	'SP',	'ST',	'SV',	'TC',	'WM',	'WU']

    # Iterate over every property and print every day in 2024
    for property in properties:
        current_date = start_date  # Reset current_date for each property
        while current_date <= end_date:
            id = f"{property}SPA{current_date}"
            print(id)
            cursor_target_azure.execute(Insert_qry, (id, "CheckedOut",))
            current_date += datetime.timedelta(days=1)




if __name__ == "__main__":
    # Define the start and end dates for the current year
    current_year= datetime.datetime.now().year
    print(current_year)

    start_date = datetime.date(current_year, 1, 1)
    end_date = datetime.date(current_year, 12, 31)
    CreatIDSFB(start_date, end_date)


    connection_target_azure.commit()
    connection_target_azure.close()


