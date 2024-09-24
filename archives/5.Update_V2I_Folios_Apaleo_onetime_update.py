import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta, timezone
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()


def log_message(message, file_path='Apaleo_log_test.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')

today = str(dt.date.today())

log_message("Folios - folios confirmed update started")

print(dt.date.today() - dt.timedelta(days=3))


def Insert_Confirmed_Res():
    confirmed_services = APIClient(
        'https://api.apaleo.com/booking/v1/reservations/?status=Confirmed&propertyIds=FKP,FCZ,FHS,FSA&expand=timeSlices,services',
        get_token()).get_data()

    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid,  FA_servicename,  FA_taacode, FA_n_amount,FA_g_amount, FA_status, FA_source,FA_impdate,FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    reservation_ids = [reservation.get('id') for reservation in confirmed_services["reservations"]]
    print(tuple(reservation_ids))


    delete_qry1 = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_status = 'confirmed' AND FA_date < '{today}';"""
    delete_qry2 = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_status = 'confirmed' AND FA_reservationid IN {tuple(reservation_ids)};"""

    cursor_target.execute(delete_qry1)
    cursor_target.execute(delete_qry2)

    if confirmed_services is not None:
        try:
            for reservation in confirmed_services['reservations']:
                reservation_id = reservation.get("id")

                for service in reservation.get("timeSlices"):
                    service_date = service.get("serviceDate")
                    service_name = service.get("unitGroup").get("code")
                    n_amount = service.get("baseAmount").get("netAmount")
                    g_amount = service.get("baseAmount").get("grossAmount")

                    cursor_target.execute(qry_insert, (
                        service_date, reservation_id, service_name, "0", n_amount, g_amount, "confirmed",
                        "timeslices_room",
                        today, "0"))

                    if "includedServices" in service:
                        for additional_service in service.get("includedServices"):
                            additional_service_name = additional_service.get("service").get("name")
                            additional_service_n_amount = additional_service.get("amount").get("netAmount")
                            additional_service_g_amount = additional_service.get("amount").get("grossAmount")
                            additional_service_taa = additional_service.get("service").get("code")

                            cursor_target.execute(qry_insert, (
                                service_date, reservation_id, additional_service_name, additional_service_taa,
                                additional_service_n_amount, additional_service_g_amount, "confirmed",
                                "timeslices_services", today,additional_service_taa))

            connection_target.commit()

        except mysql.connector.Error as err:
            error_message = f"Error: {err}"
            print(error_message)
            log_message(error_message)

            connection_target.rollback()
            pass


#Insert_Confirmed_Res()

log_message("Folios - folios confirmed update finished")



def Insert_Inhouse_Res(start, end):
    get_reservations = APIClient(
        'https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from='  + start + 'T00:00:00Z&to=' + end + 'T00:00:00Z&status=Inhouse&propertyIds=FKP,FCZ,FHS,FSA',
        get_token()).get_data()

    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:
        reservation_ids = [reservation.get('id') for reservation in get_reservations["reservations"]]
        reservation_ids_str = ','.join(reservation_ids)
        print(reservation_ids_str)
        print(len(reservation_ids))

        if reservation_ids:
            if len(reservation_ids) == 1:
                delete_qry = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_reservationid = '{reservation_ids[0]}';"""
            else:
                delete_qry = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_reservationid IN {tuple(reservation_ids)};"""

            cursor_target.execute(delete_qry)

        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?reservationIds=' + reservation_ids_str + '&expand=charges',
            get_token()).get_data()

        if get_folios:
            try:
                for folio in get_folios["folios"]:
                    if "charges" in folio:
                        for charge in folio["charges"]:
                            amount_n = charge["amount"]["netAmount"]
                            amount_g = charge["amount"]["grossAmount"]

                            if "movedReason" not in charge:
                                print(f"No movedReason for charge ID {charge['id']}")
                                if "routedTo" not in charge:
                                    print(f"Not routedTo {charge['id']}")

                                    params = (
                                        datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d'),
                                        '-'.join(charge["id"].split('-')[:2]),
                                        charge["id"],
                                        charge["name"],
                                        "0",
                                        amount_n,
                                        amount_g,
                                        "inhouse",
                                        "folios",
                                        today,
                                        "0"
                                    )
                                    cursor_target.execute(qry_insert, params)
                                else:
                                    print(f"Charge ID {charge['id']} was routedTo: {charge['routedTo']}")
                            else:
                                print(f"Charge ID {charge['id']} has movedReason: {charge['movedReason']}")

                connection_target.commit()

            except mysql.connector.Error as err:
                error_message = f"Error: {err}"
                print(error_message)
                log_message(error_message)
                connection_target.rollback()

    except TypeError as err:
        error_message = f"Error: {err}"
        log_message(error_message)
        pass




def Insert_CheckedOut_Res(start,end):
    get_reservations = APIClient(
        'https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from=' + start + 'T00:00:00Z&to=' + end + 'T00:00:00Z&status=CheckedOut&propertyIds=FKP,FCZ,FHS,FSA',
        get_token()).get_data()

    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:
        reservation_ids = [reservation.get('id') for reservation in get_reservations["reservations"]]
        reservation_ids_str = ','.join(reservation_ids)
        print(reservation_ids_str)
        print(len(reservation_ids))

        delete_qry = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_reservationid IN {tuple(reservation_ids)};"""
        cursor_target.execute(delete_qry)

        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?reservationIds=' + reservation_ids_str + '&expand=charges',
            get_token()).get_data()

        if get_folios:
            try:
                for folio in get_folios["folios"]:
                    if "charges" in folio:
                        for charge in folio["charges"]:
                            amount_n = charge["amount"]["netAmount"]
                            amount_g = charge["amount"]["grossAmount"]

                            if "movedReason" not in charge:
                                print(f"No movedReason for charge ID {charge['id']}")
                                if "routedTo" not in charge:
                                    print(f"Not routedTo {charge['id']}")

                                    params = (
                                        datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d'),
                                        '-'.join(charge["id"].split('-')[:2]),
                                        charge["id"],
                                        charge["name"],
                                        "0",
                                        amount_n,
                                        amount_g,
                                        "checkedout",
                                        "folios",
                                        today,
                                        "0"
                                    )
                                    cursor_target.execute(qry_insert, params)
                                else:
                                    print(f"Charge ID {charge['id']} was routedTo: {charge['routedTo']}")
                            else:
                                print(f"Charge ID {charge['id']} has movedReason: {charge['movedReason']}")

                connection_target.commit()

            except mysql.connector.Error as err:
                error_message = f"Error: {err}"
                print(error_message)
                log_message(error_message)
                connection_target.rollback()

    except TypeError as err:
        error_message = f"Error: {err}"
        log_message(error_message)
        pass




def Insert_Canceled_NoShow_Res():
    confirmed_services = APIClient(
        'https://api.apaleo.com/booking/v1/reservations/?status=Canceled,NoShow&propertyIds=FKP,FCZ,FHS,FSA&expand=timeSlices,services',get_token()).get_data()

    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid,  FA_servicename,  FA_taacode, FA_n_amount,FA_g_amount, FA_status, FA_source,FA_impdate,FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


    try:
        reservation_ids = [reservation.get('id') for reservation in confirmed_services["reservations"]]
        print(tuple(reservation_ids))


        delete_qry = f"""DELETE FROM V2I_Folios_Apaleo WHERE FA_reservationid IN {tuple(reservation_ids)};"""


        cursor_target.execute(delete_qry)

        if confirmed_services is not None:
            try:
                for reservation in confirmed_services['reservations']:
                    reservation_id = reservation.get("id")

                    for service in reservation.get("timeSlices"):
                        service_date = service.get("serviceDate")
                        service_name = service.get("unitGroup").get("code")
                        n_amount = service.get("baseAmount").get("netAmount")
                        g_amount = service.get("baseAmount").get("grossAmount")

                        cursor_target.execute(qry_insert, (
                            service_date, reservation_id, service_name, "0", n_amount, g_amount, "canceled_noshow",
                            "timeslices_room",
                            today, "0"))

                        if "includedServices" in service:
                            for additional_service in service.get("includedServices"):
                                additional_service_name = additional_service.get("service").get("name")
                                additional_service_n_amount = additional_service.get("amount").get("netAmount")
                                additional_service_g_amount = additional_service.get("amount").get("grossAmount")
                                additional_service_taa = additional_service.get("service").get("code")

                                cursor_target.execute(qry_insert, (
                                    service_date, reservation_id, additional_service_name, additional_service_taa,
                                    additional_service_n_amount, additional_service_g_amount, "canceled_noshow",
                                    "timeslices_services", today, additional_service_taa))

                connection_target.commit()

            except mysql.connector.Error as err:
                error_message = f"Error: {err}"
                print(error_message)
                log_message(error_message)

                connection_target.rollback()
                pass

    except TypeError as err:
        error_message = f"Error: {err}"
        log_message(error_message)
        pass






date_ranges_may = [("2024-05-01", "2024-05-05"), ("2024-05-05", "2024-05-10"), ("2024-05-10", "2024-05-15"),
                    ("2024-05-15", "2024-05-20"), ("2024-05-20", "2024-05-25"),
                    ("2024-05-25", "2024-05-30"), ("2024-05-30", "2024-06-01")]


date_ranges_june = [("2024-06-01", "2024-06-05"), ("2024-06-05", "2024-06-10"), ("2024-06-10", "2024-06-15"),
                    ("2024-06-15", "2024-06-20"), ("2024-06-20", "2024-06-25"),
                    ("2024-06-25", "2024-06-30"), ("2024-06-30", "2024-07-01")]

date_ranges_july = [("2024-07-01", "2024-07-05"), ("2024-07-05", "2024-07-10"), ("2024-07-10", "2024-07-15"),
                    ("2024-07-15", "2024-07-20"), ("2024-07-20", "2024-07-25"),
                    ("2024-07-25", "2024-07-30"),("2024-07-30", "2024-08-01")]





log_message("Folios - folios inhouse update started")


for date in date_ranges_may:
    print(date)
    Insert_Inhouse_Res(date[0], date[1])

for date in date_ranges_june:
    print(date)
    Insert_Inhouse_Res(date[0], date[1])

for date in date_ranges_july:
    print(date)
    Insert_Inhouse_Res(date[0], date[1])

log_message("Folios - folios inhouse update finished")




log_message("Folios - folios checkedout update started")

for date in date_ranges_may:
    print(date)
    Insert_CheckedOut_Res(date[0], date[1])

for date in date_ranges_june:
    print(date)
    Insert_CheckedOut_Res(date[0], date[1])

for date in date_ranges_july:
    print(date)
    Insert_CheckedOut_Res(date[0], date[1])

log_message("Folios - folios checkedout update finished")



log_message("Folios - folios canceled and noshow update started")

Insert_Canceled_NoShow_Res()

log_message("Folios - folios canceled and noshow update finished")


