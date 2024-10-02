import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta, timezone
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()


def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')


today = str(dt.date.today())

log_message("Folios - folios confirmed update started")

print(dt.date.today() - dt.timedelta(days=3))


def Insert_Confirmed_Res():
    confirmed_services = APIClient(
        'https://api.apaleo.com/booking/v1/reservations/?dateFilter=Modification&from=' + str(
            dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z&status=Confirmed&expand=timeSlices,services',
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


Insert_Confirmed_Res()

log_message("Folios - folios confirmed update finished")

log_message("Folios - folios inhouse update started")



def Insert_Inhouse_Res():
    get_reservations = APIClient(
        'https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from=' + str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z&status=Inhouse',
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
                                        '-'.join(folio["id"].split('-')[:2]),
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


Insert_Inhouse_Res()
log_message("Folios - folios inhouse update finished")






log_message("Folios - folios groups checkedout update started")


def Insert_CheckedOut_Group_Booking():

    qry_insert = """INSERT INTO V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:


        delete_qry = """DELETE FROM V2I_Folios_Apaleo WHERE FA_date = %s AND FA_serviceid = %s;"""


        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?type=Booking&expand=charges&updatedFrom='+ str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z',
            get_token()).get_data()

        if get_folios:
            try:
                for folio in get_folios["folios"]:
                    if "charges" in folio:
                        for charge in folio["charges"]:
                            amount_n = charge["amount"]["netAmount"]
                            amount_g = charge["amount"]["grossAmount"]
                            print(charge['id'])
                            print('-'.join(charge["id"].split('-')[:2]))

                            if "movedReason" not in charge:
                                print(f"No movedReason for charge ID {charge['id']}")
                                if "routedTo" not in charge:
                                    print(f"Not routedTo {charge['id']}")
                                    reservationid = '-'.join(charge["id"].split('-')[:2])
                                    servicedate =datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d')
                                    if reservationid[-1].isdigit() and servicedate <= today:

                                        params = (
                                            servicedate,
                                            reservationid,
                                            charge["id"],
                                            charge["name"],
                                            "0",
                                            amount_n,
                                            amount_g,
                                            "checkedout",
                                            "folios_Booking",
                                            today,
                                            "0"
                                        )

                                        cursor_target.execute(delete_qry, (params[0],params[2],))
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


Insert_CheckedOut_Group_Booking()

log_message("Folios - folios groups checkedout update finished")




log_message("Folios - folios checkedout update started")

def Insert_CheckedOut_Res():
    get_reservations = APIClient(
        'https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from='+ str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z&status=CheckedOut',
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
                                        '-'.join(folio["id"].split('-')[:2]),
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


Insert_CheckedOut_Res()
log_message("Folios - folios checkedout update finished")



log_message("Folios - folios canceled and noshow update started")

def Insert_Canceled_NoShow_Res():
    confirmed_services = APIClient(
        'https://api.apaleo.com/booking/v1/reservations/?dateFilter=Modification&from=' + str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z&status=Canceled,NoShow&expand=timeSlices,services',get_token()).get_data()

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


Insert_Canceled_NoShow_Res()


log_message("Folios - folios canceled and noshow update finished")



def Insert_Confirmed_Group_Booking_Folios():
    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_roomnights,FA_taacode) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s)"""
    qry_delete = """delete from V2I_Folios_Apaleo where FA_reservationid = %s"""
    bookings = APIClient(
        'https://api.apaleo.com/booking/v1/blocks?expand=timeSlices&from=' +str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z',
        get_token()).get_data()

    for block in bookings["blocks"]:
        cursor_target.execute(qry_delete, (block['id'],))
        timeslices = block['timeSlices']
        for slice in timeslices:
            print(slice)
            blockedUnits = slice['blockedUnits']
            pickedUnits = slice['pickedUnits']
            unit_dif = blockedUnits - pickedUnits
            print(unit_dif)
            net_room = int(slice['baseAmount']['netAmount'] * unit_dif)
            gross_room = int(slice['baseAmount']['grossAmount'] * unit_dif)
            if unit_dif > 0:
                params_room = (
                    datetime.fromisoformat(slice['from']).strftime('%Y-%m-%d'),
                    block['id'],
                    net_room,
                    gross_room,
                    "inhouse/confirmed",
                    "blocks_room",
                    today,
                    unit_dif,
                    101000
                )

                cursor_target.execute(qry_insert, params_room)

                if int(slice['totalGrossAmount']['amount']) > int(slice['baseAmount']['grossAmount']):
                    net_other = ((int(slice['totalGrossAmount']['amount']) * unit_dif) - gross_room)*0.90
                    gross_other = (int(slice['totalGrossAmount']['amount']) * unit_dif) - gross_room


                    params_other = (
                        datetime.fromisoformat(slice['from']).strftime('%Y-%m-%d'),
                        block['id'],
                        net_other,
                        gross_other,
                        "inhouse/confirmed",
                        "blocks_F&B",
                        today,
                        unit_dif,
                        102021
                    )
                    cursor_target.execute(qry_insert, params_other)


    connection_target.commit()

Insert_Confirmed_Group_Booking_Folios()



log_message("Folios - folios group booking update finished")

def Insert_External_Folios():


    qry_insert = """INSERT INTO V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:


        delete_qry = """DELETE FROM V2I_Folios_Apaleo WHERE FA_date = %s AND FA_serviceid = %s;"""


        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?type=external&expand=charges&updatedFrom='+ str(dt.date.today() - dt.timedelta(days=3)) +'T00:00:00Z',
            get_token()).get_data()

        if get_folios:
            try:
                for folio in get_folios["folios"]:
                    if "charges" in folio:
                        for charge in folio["charges"]:
                            amount_n = charge["amount"]["netAmount"]
                            amount_g = charge["amount"]["grossAmount"]
                            print(charge['id'])
                            print('-'.join(charge["id"].split('-')[:2]))

                            if "movedReason" not in charge:
                                print(f"No movedReason for charge ID {charge['id']}")
                                if "routedTo" not in charge:
                                    print(f"Not routedTo {charge['id']}")
                                    reservationid = folio["id"]
                                    servicedate =datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d')
                                    if reservationid[-1].isdigit() and servicedate <= today:

                                        params = (
                                            servicedate,
                                            reservationid,
                                            charge["id"],
                                            charge["name"],
                                            "0",
                                            amount_n,
                                            amount_g,
                                            "checkedout",
                                            "folios_external",
                                            today,
                                            "0"
                                        )

                                        cursor_target.execute(delete_qry, (params[0],params[2],))
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




##########################################################################pre-processing########################################################################

def servicename_cleaning():
    update_qry = """
        UPDATE 
        V2I_Folios_Apaleo
    SET 
        FA_taaname = REGEXP_REPLACE(FA_servicename, ' IB [0-9]+', '')
    WHERE 
        FA_servicename LIKE '% IB %';
        """

    cursor_target.execute(update_qry)



    update_qry2 = """
        UPDATE 
        V2I_Folios_Apaleo
    SET 
        FA_taaname = TRIM(SUBSTRING(FA_servicename, 1, LENGTH(FA_servicename) - 7))
    WHERE 
        RIGHT(FA_servicename, 6) REGEXP '^[0-9]+$'
        AND CAST(RIGHT(FA_servicename, 6) AS UNSIGNED) > 0;
        """

    cursor_target.execute(update_qry2)


    update_qry3 = """UPDATE V2I_Folios_Apaleo
                        SET FA_taaname = CASE
                        WHEN FA_servicename = 'P2C' THEN 'Summit Suite'
                        WHEN FA_servicename = 'J2C' THEN 'Junior Suite'
                        WHEN FA_servicename = 'G2D' THEN 'Grand Deluxe Double Room'
                        WHEN FA_servicename = 'S2C' THEN 'Loft Suite'
                        WHEN FA_servicename = 'F4C' THEN 'Family Room Comfort'
                        WHEN FA_servicename = 'E2D' THEN 'Double Room Deluxe'
                        WHEN FA_servicename = 'C2D' THEN 'Double Room Comfort'
                        WHEN FA_servicename = 'F3D' THEN 'Family Room Superior'
                        WHEN FA_servicename = 'F4D' THEN 'Family Room Superior'
                        WHEN FA_servicename = 'F4E' THEN 'Family Room Deluxe'
                        WHEN FA_servicename = 'S4E' THEN 'Family Suite Sonnenalpe'
                        WHEN FA_servicename = 'S4D' THEN 'Family Suite Superior'
                        WHEN FA_servicename = 'S4C' THEN 'Family Suite Comfort'
                        ELSE FA_servicename
                         
                        END; """

    cursor_target.execute(update_qry3)


    update_qry4 = """
    UPDATE V2I_Folios_Apaleo
    SET FA_taaname = FA_servicename 
    WHERE FA_taaname IS NULL;                    
    """

    cursor_target.execute(update_qry4)

    update_qry5 = """ DELETE FROM V2I_Folios_Apaleo WHERE FA_servicename LIKE '%Tip%' OR FA_servicename LIKE '%Deposit%' """
    cursor_target.execute(update_qry5)





    with open('servicenames.txt', 'r') as file:
        find_replace_strings = [line.strip() for line in file if line.strip()]
        for find_string in find_replace_strings:
            update_query = """
                UPDATE V2I_Folios_Apaleo
                SET FA_taaname = %s
                WHERE FA_servicename LIKE %s;
                """

            # Use a tuple to safely pass parameters to execute method
            update_data = (find_string, f"%{find_string}%")

            cursor_target.execute(update_query, update_data)


    connection_target.commit()

servicename_cleaning()

log_message("Folios - servicename cleaning done")


def update_TAA():

    update_taas = """
    UPDATE V2I_Folios_Apaleo
LEFT JOIN V2D_TAA_Apaleo 
    ON V2I_Folios_Apaleo.FA_taaname = V2D_TAA_Apaleo.TA_name
SET V2I_Folios_Apaleo.FA_taacode = 
    CASE
        WHEN V2D_TAA_Apaleo.TA_code IS NOT NULL THEN V2D_TAA_Apaleo.TA_code
        ELSE V2I_Folios_Apaleo.FA_taacode  -- Keep it unchanged
    END
WHERE V2I_Folios_Apaleo.FA_taacode = '0';

    
    """

    cursor_target.execute(update_taas)

    with open('KP_taas.txt', 'r') as file:
        find_replace_strings = [line.strip() for line in file if line.strip()]
        for find_string in find_replace_strings:
            update_query = """
                UPDATE V2I_Folios_Apaleo
                SET FA_taacode = CONCAT("1",FA_taacode) 
                WHERE FA_taacode = %s;
                """

            # Use a tuple to safely pass parameters to execute method
            param = (f"{find_string}",)
            print(param)

            cursor_target.execute(update_query, param)

    update_external_taas = """UPDATE V2I_Folios_Apaleo
                                SET FA_taacode = CASE 
                                                    WHEN FA_reservationid LIKE '%-FB2%' THEN 102206
                                                    WHEN FA_reservationid LIKE '%-SP2%' THEN 106025
                                                 END
                                WHERE FA_reservationid LIKE '%-FB2%' OR FA_reservationid LIKE '%-SP2%';
                                 """

    cursor_target.execute(update_external_taas)
    connection_target.commit()


update_TAA()

log_message("Folios - taas mapped")