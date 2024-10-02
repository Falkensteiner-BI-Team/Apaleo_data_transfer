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


log_message("GuestFutureReservation - GuestFutureReservation started...")

insert_query = """INSERT INTO V2I_GuestFutureReservation(GFR_leistacc,GFR_mpehotel,GFR_datumimp,GFR_datumres,GFR_datumvon, GFR_datumbis, GFR_reschar, GFR_kattyp, GFR_katnr, GFR_zimmernr,GFR_market,GFR_source,GFR_hear,GFR_come,GFR_spirit,GFR_kundennr,GFR_firmennr,GFR_reisenr, GFR_gruppennr,GFR_sourcenr,GFR_kontinnr,GFR_nat_zipcodekey,GFR_res_zipcodekey,GFR_zimmer,GFR_datumcxl,GFR_sharenr,GFR_crsnr,GFR_sysimport)
VALUES(%s,%s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


select_query = """SELECT GFR_leistacc,GFR_mpehotel,GFR_datumimp,GFR_datumres,GFR_datumvon, GFR_datumbis, GFR_reschar, GFR_kattyp, GFR_katnr, GFR_zimmernr,GFR_market,GFR_source,GFR_hear,GFR_come,GFR_spirit,GFR_kundennr,GFR_firmennr,GFR_reisenr, GFR_gruppennr,GFR_sourcenr,GFR_kontinnr,GFR_nat_zipcodekey,GFR_res_zipcodekey,GFR_zimmer,GFR_datumcxl,GFR_sharenr,GFR_crsnr, GFR_sysimport FROM V2I_GFR_Apaleo WHERE GFR_sysimport >= CONCAT('guestfuturereservationapaleo_', DATE_SUB(CURDATE(), INTERVAL 10 DAY)) AND  GFR_leistacc IS NOT NULL"""


delete_query = """DELETE FROM V2I_GuestFutureReservation WHERE GFR_leistacc = %s AND GFR_sysimport = %s"""



cursor_target.execute(select_query, )

for row in cursor_target.fetchall():
    #print(row[27])
    cursor_target.execute(delete_query, (row[0],row[27], ))
    cursor_target.execute(insert_query, row)

    connection_target.commit()


##### deleting protel imports
select_query = """SELECT DISTINCT(GFR_datumimp) FROM V2I_GFR_Apaleo WHERE GFR_datumimp >= DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND  GFR_leistacc IS NOT NULL"""

cursor_target.execute(select_query, )

for row in cursor_target.fetchall():
    ghd_date = row[0]
    #ghd_filename = f"guestfuturereservation_{ghd_date}.7z"
    #print(ghd_filename)
    cursor_target.execute("""DELETE  FROM V2I_GuestFutureReservation WHERE GFR_mpehotel IN (49,50,26,27,13,33,28,15) AND GFR_sysimport LIKE "%guestfuturereservation_2%" AND GFR_datumimp >= DATE_SUB(CURDATE(), INTERVAL 12 DAY)""",)
    connection_target.commit()



log_message("GuestFutureReservation - GuestFutureReservation finished...")