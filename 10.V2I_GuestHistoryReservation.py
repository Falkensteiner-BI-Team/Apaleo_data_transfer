import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()


def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')


log_message("GuestHistoryReservation - GuestHistoryReservation started...")


insert_query = """INSERT INTO V2I_GuestHistoryReservation(GHR_leistacc,GHR_mpehotel,GHR_datumres,GHR_datumvon, GHR_datumbis, GHR_reschar, GHR_kattyp, GHR_katnr, GHR_zimmernr,GHR_market,GHR_source,GHR_hear,GHR_come,GHR_spirit,GHR_kundennr,GHR_kunden_DOB,GHR_repeater,GHR_kunden_visit, GHR_firmennr,GHR_reisenr, GHR_gruppennr,GHR_sourcenr,GHR_kontinnr,GHR_nat_zipcodekey,GHR_res_zipcodekey,GHR_zimmer,GHR_datumcxl,GHR_sharenr,GHR_crsnr,GHR_sysimport)
VALUES(%s,%s,%s,%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


select_query = """SELECT * FROM V2V_GHR_Apaleo WHERE GHR_sysimport >= CONCAT('guesthistoryreservationapaleo_', DATE_SUB(CURDATE(), INTERVAL 3 DAY)) AND  GHR_leistacc IS NOT NULL AND GHR_mpehotel NOT IN (49,19)"""

delete_query = """DELETE FROM V2I_GuestHistoryReservation WHERE GHR_leistacc =%s"""
cursor_target.execute(select_query, )

for row in cursor_target.fetchall():
    cursor_target.execute(delete_query, (row[0],))
    connection_target.commit()
    cursor_target.execute(insert_query, row)

    connection_target.commit()

log_message("GuestHistoryReservation - GuestHistoryReservation finished...")


