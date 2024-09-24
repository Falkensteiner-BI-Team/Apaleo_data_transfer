import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta, timezone
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),password=userdata.mysql_password())
cursor_target = connection_target.cursor()

def log_message(message, file_path='Apaleo_log_test.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')


#log_message("Folios - folios confirmed update started")

today = str(dt.date.today())

#confirmed_services = APIClient('https://api.apaleo.com/booking/v1/reservations/?dateFilter=Modification&from=' + str(dt.date.today()) + 'T00:00:00Z&status=Confirmed&expand=timeSlices,services', get_token()).get_data()







def Insert_Future_Confirmed_Res():
   confirmed_services = APIClient('https://api.apaleo.com/booking/v1/reservations/?status=Confirmed&expand=timeSlices,services', get_token()).get_data()
   qry_insert = """insert into V2I_Folios_Apaleo_test(FA_reservationid, FA_date, FA_servicename, FA_n_amount, FA_g_amount, FA_taacode,FA_source,FA_updated)VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
   qry_delete = """delete from V2I_Folios_Apaleo_test where FA_reservationid = %s and FA_date= %s and FA_servicename = %s """

   if confirmed_services is not None:
       try:
           for reservation in confirmed_services['reservations']:
               reservation_id = reservation.get("id")

               for service in reservation.get("timeSlices"):
                   service_date = service.get("serviceDate")
                   service_name = service.get("unitGroup").get("code")
                   n_amount = service.get("baseAmount").get("netAmount")
                   g_amount = service.get("baseAmount").get("grossAmount")

                   cursor_target.execute(qry_delete, (reservation_id, service_date, service_name))
                   cursor_target.execute(qry_insert, (reservation_id,service_date, service_name, n_amount, g_amount, "0", "timeslices_room",today))

                   if "includedServices" in service:
                       for additional_service in service.get("includedServices"):
                           additional_service_name = additional_service.get("service").get("name")
                           additional_service_n_amount = additional_service.get("amount").get("netAmount")
                           additional_service_g_amount = additional_service.get("amount").get("grossAmount")
                           additional_service_taa = additional_service.get("service").get("code")

                           #cursor_target.execute(qry_delete, (reservation_id,service_date, additional_service_name))
                           cursor_target.execute(qry_insert, (reservation_id, service_date, additional_service_name, additional_service_n_amount,additional_service_g_amount, additional_service_taa, "timeslices_services",today))

           connection_target.commit()
       except mysql.connector.Error as err:

           error_message = f"Error: {err}"
           print(error_message)
           log_message(error_message)

           connection_target.rollback()


#Insert_Future_Confirmed_Res()




#log_message("Folios - folios confirmed update finished")

log_message("Folios - folios Inhouse and CheckedOut update started")

#get_reservations = APIClient('https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from=' + str(dt.date.today())+ 'T00:00:00Z&status=InHouse,CheckedOut', get_token()).get_data()

def Insert_Inhouse_CheckedOut_Res(start, end):
    # Specify the date and time you want to use for the filter

    get_reservations = APIClient('https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from=' + start + 'T00:00:00Z&to=' + end + 'T00:00:00Z&status=CheckedOut&propertyIds=FKP', get_token()).get_data()


    qry_delete = """delete from V2I_Folios_Apaleo_test_ where FA_reservationid = %s and FA_date = %s and FA_servicename = %s"""
    qry_insert = """insert into V2I_Folios_Apaleo_test_(FA_reservationid, FA_date, FA_servicename, FA_n_amount, FA_g_amount, FA_taacode,FA_source)VALUES(%s,%s,%s,%s,%s,%s,%s)"""

    # Collect reservation IDs into a list
    reservation_ids = [reservation.get('id') for reservation in get_reservations["reservations"]]

    # Convert the list of reservation IDs into a comma-separated string
    reservation_ids_str = ','.join(reservation_ids)

    print(reservation_ids_str)



    get_folios = APIClient('https://api.apaleo.com/finance/v1/folios?reservationIds='+reservation_ids_str+'&expand=charges',get_token()).get_data()
    print(get_folios)
    if get_folios is not None:

        try:
            for folio in get_folios["folios"]:
                #print(folio)
                if "charges" in folio:
                    for charge in folio["charges"]:
                            params = (
                                '-'.join(charge["id"].split('-')[:2]),
                                datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d'),
                                charge["name"],
                                charge["amount"]["netAmount"],
                                charge["amount"]["grossAmount"],
                                "0",
                                "folios")
                            print(params)
                            cursor_target.execute(qry_delete, (params[0], params[1], params[2],))
                            cursor_target.execute(qry_insert, params)
                connection_target.commit()


        except mysql.connector.Error as err:

                error_message = f"Error: {err}"
                print(error_message)
                log_message(error_message)

                connection_target.rollback()






'''
date_ranges_june = [("2024-06-01", "2024-06-05"),("2024-06-05", "2024-06-10"),("2024-06-10","2024-06-15"),("2024-06-15","2024-06-20"),("2024-06-20","2024-06-25"),
               ("2024-06-25","2024-06-30")]




for date in date_ranges_june:
    print("from:"+ date[0]+ "to: " + date[1])
    Insert_Inhouse_CheckedOut_Res(date[0], date[1])

'''


#date_ranges_july = [("2024-07-01", "2024-07-05"),("2024-07-05", "2024-07-10"),("2024-07-10","2024-07-15"),("2024-07-15","2024-07-20"),("2024-07-20","2024-07-25"),
         #      ("2024-07-25","2024-08-01")]


date_ranges_july = [("2024-07-01", "2024-07-05"),("2024-07-05", "2024-07-10")]

for date in date_ranges_july:
    print("from:" + date[0] + "to: " + date[1])
    Insert_Inhouse_CheckedOut_Res(date[0],date[1])


log_message("Folios - folios Inhouse and CheckedOut update finished")


log_message("Folios - folios confirmed update started")

#Insert_Future_Confirmed_Res()


log_message("Folios - folios confirmed update finished")


