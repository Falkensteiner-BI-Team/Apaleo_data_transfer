import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt



# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(), password=userdata.mysql_password())
cursor_target = connection_target.cursor()

insert_query = """INSERT INTO V2I_GuestFutureDaily(GFD_leistacc,GFD_mpehotel,GFD_datumimp,GFD_datum,GFD_zimmernr,GFD_roomnights,GFD_resstatus,GFD_typ,GFD_preistypgr,GFD_preistyp,GFD_anzerw,GFD_anzkin1,GFD_anzkin2,GFD_anzkin3,GFD_anzkin4,GFD_zbett,GFD_kbett,GFD_n_logis,GFD_n_fb,GFD_n_other,GFD_n_logis_EUR,GFD_n_fb_EUR,GFD_n_other_EUR,GFD_sysimport,GFD_katnr)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


select_query = """SELECT GFD_leistacc,GFD_mpehotel,GFD_datumimp,GFD_datum,GFD_zimmernr,GFD_roomnights,GFD_resstatus,GFD_typ,GFD_preistypgr,GFD_preistyp,GFD_anzerw,GFD_anzkin1,GFD_anzkin2,GFD_anzkin3,GFD_anzkin4,GFD_zbett,GFD_kbett,GFD_n_logis,GFD_n_fb,GFD_n_other,GFD_n_logis_EUR,GFD_n_fb_EUR,GFD_n_other_EUR,GFD_sysimport,GFD_katnr

FROM V2I_GFD_Apaleo WHERE GFD_datumimp >=DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND  GFD_leistacc IS NOT NULL"""

delete_query = """delete from V2I_GuestFutureDaily WHERE GFD_leistacc = %s AND GFD_datum = %s AND GFD_datumimp = %s"""

cursor_target.execute(select_query,)

for row in cursor_target.fetchall():
    #print(row)
    cursor_target.execute(delete_query, (row[0], row[3], row[2],))
    cursor_target.execute(insert_query,row)
    connection_target.commit()



##### deleting protel imports
select_query = """SELECT DISTINCT(GFD_datumimp) FROM V2I_GFD_Apaleo WHERE GFD_datumimp >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND  GFD_leistacc IS NOT NULL"""

cursor_target.execute(select_query, )

for row in cursor_target.fetchall():
    ghd_date = row[0]
    ghd_filename = f"guestfuturedaily_{ghd_date}.7z"
    print(ghd_filename)
    cursor_target.execute("""DELETE FROM  V2I_GuestFutureDaily WHERE GFD_mpehotel  IN (49,50,26,27,13,33,28) AND GFD_sysimport LIKE "%guestfuturedaily_2%" AND GFD_datumimp >= DATE_SUB(CURDATE(), INTERVAL 12 DAY)""",)
    connection_target.commit()

