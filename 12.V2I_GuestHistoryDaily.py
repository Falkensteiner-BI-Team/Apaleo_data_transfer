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


log_message("GuestHistoryDaily - GuestHistoryDaily started...")

insert_query = """INSERT INTO V2I_GuestHistoryDaily(GHD_leistacc,GHD_mpehotel,GHD_datum,GHD_zimmernr,GHD_roomnights,GHD_resstatus,GHD_typ,GHD_preistypgr,GHD_preistyp,GHD_anzerw,GHD_anzkin1,GHD_anzkin2,GHD_anzkin3,GHD_anzkin4,GHD_zbett, GHD_n_logis,GHD_n_fb,GHD_n_bqt,GHD_n_spa,GHD_n_ski,GHD_n_other,GHD_n_logis_EUR,GHD_n_fb_EUR,GHD_n_bqt_EUR,GHD_n_spa_EUR,GHD_n_ski_EUR,GHD_n_other_EUR,GHD_sysimport)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


select_query = """SELECT GHD_leistacc,GHD_mpehotel,GHD_datum,GHD_zimmernr,GHD_roomnights,GHD_resstatus,GHD_typ,GHD_preistypgr,GHD_preistyp,GHD_anzerw,GHD_anzkin1,GHD_anzkin2,GHD_anzkin3,GHD_anzkin4,GHD_zbett,GHD_n_logis,GHD_n_fb,GHD_n_bqt,GHD_n_spa,GHD_n_ski,GHD_n_other,GHD_n_logis_EUR,GHD_n_fb_EUR,GHD_n_bqt_EUR,GHD_n_spa_EUR,GHD_n_ski_EUR,GHD_n_other_EUR,GHD_sysimport
 FROM V2I_GHD_Apaleo WHERE GHD_datumimp >= DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND  GHD_leistacc IS NOT NULL AND GHD_mpehotel  NOT IN (49,19)"""


delete_query = """DELETE FROM V2I_GuestHistoryDaily WHERE  GHD_leistacc=%s"""
cursor_target.execute(select_query, )


for row in cursor_target.fetchall():
    #print(row[2])
    cursor_target.execute(delete_query, (row[0],))
    connection_target.commit()

cursor_target.execute(select_query, )
for row in cursor_target.fetchall():
    cursor_target.execute(insert_query, row)
    connection_target.commit()


log_message("GuestHistoryDaily - GuestHistoryDaily finished...")
