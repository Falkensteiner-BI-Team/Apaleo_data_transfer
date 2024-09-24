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


log_message("Folios - folios update started")



def Insert_API_Results():
    # Specify the date and time you want to use for the filter
    get_reservations = APIClient('https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from=' + str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z&status=InHouse,CheckedOut', get_token()).get_data()
    for reservation in get_reservations["reservations"]:



        if reservation['property']['id'] in ["FCZ", "FSA", "FHS","FKP"]:
                    get_folios = APIClient('https://api.apaleo.com/finance/v1/folios?reservationIds='+str(reservation.get('id'))+'&expand=charges',get_token()).get_data()

                    # Construct the parameter tuple for the query
                    if get_folios is not None:
                        for folio in get_folios["folios"]:
                            if "charges" in folio:
                                for charge in folio["charges"]:

                                    params = (
                                        '-'.join(charge["id"].split('-')[:2]),
                                        reservation['property']['id'],
                                        datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d'),
                                        charge["name"],
                                        charge["amount"]["netAmount"],
                                        charge["amount"]["grossAmount"],
                                        "folios"
                                    )

                                    qry_delete = """delete from V2I_Folios_Apaleo where FA_reservationid = %s and FA_property = %s and FA_date = %s and FA_servicename = %s"""
                                    cursor_target.execute(qry_delete, (params[0],params[1],params[2],params[3],))
                                    connection_target.commit()

                                    qry_insert = """insert into V2I_Folios_Apaleo(FA_reservationid, FA_property, FA_date, FA_servicename, FA_n_amount, FA_g_amount, FA_source)VALUES(%s,%s,%s,%s,%s,%s,%s)"""

                                    cursor_target.execute(qry_insert, params)


                                    connection_target.commit()





Insert_API_Results()

print("modified reservations complete")
log_message("Folios - modified reservations complete")


def Insert_Confirmed_RoomRev():

    select_confirmed = """SELECT GFR_bookingid, GFR_code2 
    FROM V2I_GFR_Apaleo 
    WHERE GFR_datumimp = (
        SELECT MAX(GFR_datumimp)
        FROM V2I_GFR_Apaleo
    )
    AND GFR_status = 'confirmed'  
    AND GFR_updated = DATE_SUB(CURDATE(), INTERVAL 3 DAY);"""



    cursor_target.execute(select_confirmed,)

    for bookingid in cursor_target.fetchall():
        room_rev = APIClient('https://api.apaleo.com/booking/v1/reservations/' + str(bookingid[0]), get_token()).get_data()
        if str(room_rev.get('status')).lower() == "confirmed":
            arrival_date = datetime.fromisoformat(room_rev.get('arrival')[:10]).date()
            departure_date = datetime.fromisoformat(room_rev.get('departure')[:10]).date()

            days_between = (departure_date - arrival_date).days


            for i in range(days_between):
                day_date = arrival_date + timedelta(days=i)

                params = (
                    str(bookingid[0]),
                    room_rev.get('property').get('id'),
                    day_date,
                    room_rev.get('unitGroup').get('code'),
                    (room_rev.get('totalGrossAmount').get('amount') / days_between) * 0.87 if room_rev.get('property').get('id') == 'FCZ' else (room_rev.get('totalGrossAmount').get('amount') / days_between) * 0.90,
                    (room_rev.get('totalGrossAmount').get('amount') / days_between),
                    "reservations"
                )

                qry_delete = """delete from V2I_Folios_Apaleo where FA_reservationid=%s AND FA_property = %s AND FA_date = %s AND FA_servicename = %s """
                cursor_target.execute(qry_delete, (params[0], params[1], params[2], params[3],))

                connection_target.commit()

                qry_insert = """insert into V2I_Folios_Apaleo(FA_reservationid, FA_property, FA_date, FA_servicename, FA_n_amount, FA_g_amount, FA_source)VALUES(%s,%s,%s,%s,%s,%s,%s)"""

                cursor_target.execute(qry_insert, params)


                connection_target.commit()

Insert_Confirmed_RoomRev()


print("confirmed reservations complete")
log_message("Folios - confirmed reservations complete")




def servicename_cleaning():
    update_qry = """
        UPDATE 
        V2I_Folios_Apaleo
    SET 
        FA_servicename = REGEXP_REPLACE(FA_servicename, ' IB [0-9]+', '')
    WHERE 
        FA_servicename LIKE '% IB %';
        """

    cursor_target.execute(update_qry)
    connection_target.commit()


    update_qry2 = """
        UPDATE 
        V2I_Folios_Apaleo
    SET 
        FA_servicename = TRIM(SUBSTRING(FA_servicename, 1, LENGTH(FA_servicename) - 7))
    WHERE 
        RIGHT(FA_servicename, 6) REGEXP '^[0-9]+$'
        AND CAST(RIGHT(FA_servicename, 6) AS UNSIGNED) > 0;
        """

    cursor_target.execute(update_qry2)
    connection_target.commit()

    with open('../servicenames.txt', 'r') as file:
        find_replace_strings = [line.strip() for line in file if line.strip()]
        for find_string in find_replace_strings:
            update_query = """
                UPDATE V2I_Folios_Apaleo
                SET FA_servicename = %s
                WHERE FA_servicename LIKE %s;
                """

            # Use a tuple to safely pass parameters to execute method
            update_data = (find_string, f"%{find_string}%")

            cursor_target.execute(update_query, update_data)
            connection_target.commit()


servicename_cleaning()

log_message("Folios - servicename cleaning done")

# WHERE V2I_Folios_Apaleo_test.FA_taacode = '0';'
def update_TAA():
    update_qry = """
    UPDATE V2I_Folios_Apaleo 
    LEFT JOIN V2D_TAA_Apaleo 
    ON V2I_Folios_Apaleo.FA_servicename = V2D_TAA_Apaleo.TA_name
    SET V2I_Folios_Apaleo.FA_taacode = V2D_TAA_Apaleo.TA_code;
    """

    update_null_taas = """UPDATE V2I_Folios_Apaleo SET V2I_Folios_Apaleo.FA_taacode = 0 WHERE V2I_Folios_Apaleo.FA_taacode IS NULL"""



    update_special_taas = """
    UPDATE V2I_Folios_Apaleo 
    LEFT JOIN V2D_TAA_Apaleo 
    ON CONCAT(V2I_Folios_Apaleo.FA_property, '-', V2I_Folios_Apaleo.FA_servicename) = V2D_TAA_Apaleo.TA_name
    SET V2I_Folios_Apaleo.FA_taacode = V2D_TAA_Apaleo.TA_code
    WHERE V2I_Folios_Apaleo.FA_taacode = "0";
    """



    update_local_tax = """UPDATE V2I_Folios_Apaleo SET V2I_Folios_Apaleo.FA_servicename = 'Local Tax' WHERE LOWER(V2I_Folios_Apaleo.FA_servicename) LIKE '%local tax%'  """

    update_Voucher0 = """UPDATE V2I_Folios_Apaleo SET V2I_Folios_Apaleo.FA_servicename = 'Voucher 0%' WHERE LOWER(V2I_Folios_Apaleo.FA_servicename) LIKE '%voucher 0%'  """



    cursor_target.execute(update_local_tax)
    connection_target.commit()
    #print("update local tax done")

    cursor_target.execute(update_Voucher0)
    connection_target.commit()
    #print("update voucher")

    cursor_target.execute(update_qry)
    connection_target.commit()
    #print("update taa codes")


    cursor_target.execute(update_special_taas)
    connection_target.commit()
    #print("update special taas")

    cursor_target.execute(update_null_taas)
    connection_target.commit()




update_TAA()

print("taas updated...")

log_message("Folios - taas updated")

def Insert_Future_Confirmed_Res():
    select_confirmed = """SELECT GFR_bookingid, GFR_code2 
    FROM V2I_GFR_Apaleo 
    WHERE GFR_datumimp = (
        SELECT MAX(GFR_datumimp)
        FROM V2I_GFR_Apaleo
    )
    AND GFR_status = 'confirmed'  
    AND GFR_updated = DATE_SUB(CURDATE(), INTERVAL 3 DAY);"""


    cursor_target.execute(select_confirmed,)
    for bookingid in cursor_target.fetchall():

        corresponding_services = APIClient(
            'https://api.apaleo.com/booking/v1/reservations/' + str(bookingid[0]) + '/services', get_token()).get_data()

        if corresponding_services is not None:
            for service in corresponding_services['services']:
                for date in service['dates']:

                    params = (
                            bookingid[0],
                            'F' + str(bookingid[1]),
                            date['serviceDate'],
                            service['service']['name'],
                            date['amount']['netAmount'],
                            date['amount']['grossAmount'],
                            service['service']['code']


                    )

                    qry_delete = """delete from V2I_Folios_Apaleo where FA_reservationid = %s and FA_property = %s and FA_date= %s and FA_servicename = %s """
                    cursor_target.execute(qry_delete, (params[0], params[1], params[2], params[3],))
                    connection_target.commit()

                    qry_insert = """insert into V2I_Folios_Apaleo(FA_reservationid, FA_property, FA_date, FA_servicename, FA_n_amount, FA_g_amount, FA_taacode)VALUES(%s,%s,%s,%s,%s,%s, %s)"""

                    cursor_target.execute(qry_insert, params)

                    #print(params)
                    connection_target.commit()

Insert_Future_Confirmed_Res()


print("additional services complete")
log_message("Folios - additional services complete")





def removing_Deposit():
    delete_qry = """ DELETE FROM V2I_Folios_Apaleo WHERE FA_servicename LIKE '%Tip%' OR FA_servicename LIKE '%Deposit%' """
    cursor_target.execute(delete_qry, )
    connection_target.commit()

removing_Deposit()

log_message("Folios- removed deposits")




servicename_fixing_qry = """UPDATE V2I_Folios_Apaleo
SET FA_servicename = CASE
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
 
END;"""

cursor_target.execute(servicename_fixing_qry,)




duplication_qry = """

WITH CTE AS (
  SELECT
    id,
    FA_reservationid,
    FA_date,
    FA_property,
    FA_servicename,
    FA_n_amount,
    FA_source,
    ROW_NUMBER() OVER (
      PARTITION BY FA_reservationid, FA_date, FA_property, FA_servicename, FA_n_amount
      ORDER BY 
        CASE 
          WHEN FA_source = 'folios' THEN 1 
          ELSE 2 
        END, 
        FA_reservationid
    ) AS row_num
  FROM V2I_Folios_Apaleo
)
DELETE FROM V2I_Folios_Apaleo
WHERE id IN (
  SELECT id FROM CTE WHERE row_num > 1
);

 """

cursor_target.execute(duplication_qry,)

connection_target.commit()


print("deduplication complete")
log_message("Folios - deduplication complete")