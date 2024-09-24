import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()


insert_query = """INSERT INTO V2I_GuestHistoryDaily_Detailed(GHD_leistacc,GHD_mpehotel,GHD_datum,GHD_zimmernr,GHD_roomnights,GHD_resstatus,GHD_typ,GHD_preistypgr,GHD_preistyp,GHD_anzerw,GHD_anzkin1,GHD_anzkin2,GHD_anzkin3,GHD_anzkin4,GHD_zbett,GHD_kbett,GHD_TAA,GHD_n_,GHD_g_,GHD_n_EUR,GHD_g_EUR,GHD_sysimport)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


select_query = """SELECT GHD_leistacc,GHD_mpehotel,GHD_datum,GHD_zimmernr,GHD_roomnights,GHD_resstatus,GHD_typ,GHD_preistypgr,GHD_preistyp,GHD_anzerw,GHD_anzkin1,GHD_anzkin2,GHD_anzkin3,GHD_anzkin4,GHD_zbett,GHD_kbett,GHD_TAA,GHD_n_,GHD_g_,GHD_n_EUR,GHD_g_EUR,GHD_sysimport
 FROM V2I_GHDD_Apaleo WHERE GHD_sysimport >= CONCAT('guesthistorydaily_detailed_apaleo_', DATE_SUB(CURDATE(), INTERVAL 3 DAY),'.7z') AND  GHD_leistacc IS NOT NULL"""

delete_query = """DELETE FROM V2I_GuestHistoryDaily_Detailed WHERE GHD_leistacc = %s AND GHD_datum = %s AND GHD_TAA=%s"""



cursor_target.execute(select_query, )
for row in cursor_target.fetchall():
    #print((row[0],str(row[2]),row[16]))
    cursor_target.execute(delete_query, (row[0],str(row[2]),row[16]))
    cursor_target.execute(insert_query, row)
connection_target.commit()
