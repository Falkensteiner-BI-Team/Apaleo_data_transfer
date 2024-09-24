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

def Insert_CheckedOut_Group_Booking():


    qry_insert = """INSERT INTO V2I_Folios_Apaleo_test(FA_date, FA_reservationid, FA_serviceid, FA_servicename, FA_taacode, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_taacode0)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    try:


        delete_qry = """DELETE FROM V2I_Folios_Apaleo_test WHERE FA_date = %s AND FA_serviceid = %s;"""


        get_folios = APIClient(
            'https://api.apaleo.com/finance/v1/folios?type=Booking&expand=charges&updatedFrom=' + str(dt.date.today() - dt.timedelta(days=15)) + 'T00:00:00Z',
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

                                    params = (
                                        datetime.fromisoformat(charge["serviceDate"]).strftime('%Y-%m-%d'),
                                        '-'.join(charge["id"].split('-')[:2]),
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
