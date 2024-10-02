from msilib.schema import Property

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

log_message("Folios - folios past group booking update started")

print(dt.date.today() - dt.timedelta(days=3))

def Insert_External_Folios():


    qry_insert = """INSERT INTO V2I_Folios_Apaleo_test(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:


        delete_qry = """DELETE FROM V2I_Folios_Apaleo_test WHERE FA_date = %s AND FA_serviceid = %s;"""


        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?type=external&expand=charges&updatedFrom=2024-09-01T00:00:00Z',
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


#Insert_External_Folios()



def Insert_Confirmed_Group_Booking_GFR(import_date):


    qry_insert = """INSERT INTO `V2I_GHR_Apaleo_test`(
    `GHR_bookingid`,
    `GHR_reservationid`,
    `GHR_code2`,
    `GHR_datumres`,
    `GHR_datumvon`,
    `GHR_datumbis`,
    `GHR_unit_group_code`,
     `GHR_datumcxl`,   
     `GHR_updated`,
     `GHR_rateplan`,
     `GHR_typ`,
     `GHR_reschar`,
     `GHR_status`,
     `GHR_datumimp`,
     `GHR_sysimport`,
     `GHR_Adults`,
     `GHR_zimmer`,
     `GHR_market_segment`)Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    qry_delete = f""" DELETE from `V2I_GHR_Apaleo_test` where `GHR_reservationid` = %s"""

    get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?type=external&expand=charges&updatedFrom=2024-09-01T00:00:00Z',
            get_token()).get_data()

    if get_folios:

            for folio in get_folios["folios"]:
                property = folio["id"].split('-')[0]
                bookingid = folio["id"]
                reservationid = property + "-"+bookingid
                print(folio)

                params_external = (
                    bookingid,
                    reservationid,
                    property[-2:],
                    datetime.fromisoformat(folio['created']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(folio['created']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(folio['created']).strftime('%Y-%m-%d'),
                    "PM",
                    '1900-01-01',
                    datetime.fromisoformat(folio['updated']).strftime('%Y-%m-%d'),
                    "INTERNAL",
                    0,
                    0,
                    "checkedout",
                    str(import_date),
                    "guesthistoryreservationapaleo_" + str(import_date),
                    1,
                    0,
                    "INDIVIDUAL",

                )
                cursor_target.execute(qry_delete, (params_external[1],))
                cursor_target.execute(qry_insert, params_external)

    connection_target.commit()


Insert_Confirmed_Group_Booking_GFR(today)

import_date = dt.date.today()
import_date_str = import_date.strftime('%Y-%m-%d')

def Preprocess_and_Update(column, mappedcolumn, mappingtable, keycolumn, keycolumnapaleo, datumimp, default_value):
    update_query = f"""
    UPDATE `V2I_GHR_Apaleo_test` AS v
    LEFT JOIN {mappingtable} AS m ON v.{keycolumn} = m.{keycolumnapaleo}
    SET v.{column} = COALESCE(m.{mappedcolumn}, {default_value})
    WHERE DATE(v.GHR_datumimp) = '{datumimp}'

    """
    cursor_target.execute(update_query, )





Preprocess_and_Update("GHR_leistacc", "Seq_ID", "V2D_res_num_mapping", "GHR_reservationid", "Apaleo_res_ID", import_date_str, "NULL")
log_message("GHR - GHR_leistacc updated ")
connection_target.commit()


Preprocess_and_Update("GHR_mpehotel", "PAS_Protel_ID", "V2D_Property_Attributes", "GHR_code2", "PAS_code2", import_date_str, "NULL" )
log_message("GHR - GHR_mpehotel updated ")
connection_target.commit()


Preprocess_and_Update("GHR_katnr", "PRC_katnr", "V2D_ProtelRoomCategories", "GHR_unit_group_code", "PRC_kat", import_date_str, 0)
log_message("GHR - GHR_katnr updated ")
connection_target.commit()


cursor_target.execute(f"""UPDATE  V2I_GHR_Apaleo SET GHR_market_segment= 'MICEGR' WHERE GHR_rateplan LIKE '%GR%' AND (GHR_market_segment = 'INDIVIDUAL' OR GHR_market_segment IS NULL) AND DATE(GHR_datumimp)  = '{import_date_str}' """,)
cursor_target.execute(f"""UPDATE  V2I_GHR_Apaleo SET GHR_market_segment= 'LEISUREGR' WHERE GHR_rateplan LIKE '%CR%' AND (GHR_market_segment = 'INDIVIDUAL' OR GHR_market_segment IS NULL) AND DATE(GHR_datumimp) = '{import_date_str}' """,)
log_message("GHR - GHR  market segment updated")
connection_target.commit()


Preprocess_and_Update("GHR_market", "MK_Nr", "V2D_Apaleo_Market", "GHR_market_segment", "Apaleo_Mkt_code", import_date_str,"NULL")
log_message("GHR - GHR_market updated ")
connection_target.commit()


Preprocess_and_Update("GHR_sharenr","Seq_ID", "V2D_res_num_mapping", "GHR_bookingid_sharer", "Apaleo_res_ID", import_date_str, "NULL")
log_message("GHR - GHR_sharenr updated ")
connection_target.commit()


