import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt


def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')





# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()


log_message("GHDD  - started")

select_query = """
WITH AggregatedChildGroups AS (
    SELECT        
        c.CG_reservationid,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin1' THEN 1 ELSE 0 END) AS GHD_anzkin1,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin2' THEN 1 ELSE 0 END) AS GHD_anzkin2,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin3' THEN 1 ELSE 0 END) AS GHD_anzkin3,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin4' THEN 1 ELSE 0 END) AS GHD_anzkin4,
        MAX(CASE WHEN c.CG_Group = 'GHD_kbett' THEN 1 ELSE 0 END) AS GHD_kbett
    FROM
        V2I_GHR_Apaleo g
    LEFT JOIN
        V2I_ChildGroups c ON g.GHR_reservationid = c.CG_reservationid
    GROUP BY
        c.CG_reservationid
)

SELECT 
    subquery_alias.*,
    
     CASE
            WHEN subquery_alias.FA_date = main.GHR_datumbis THEN 0
            ELSE 1 END AS GHD_roomnights,
    
    -- main.GHR_datumvon,
    -- main.GHR_datumbis,
    main.GHR_mpehotel,
    main.GHR_Adults,
    main.GHR_leistacc,
    COALESCE(child.GHD_anzkin1, 0) AS GHD_anzkin1,
    COALESCE(child.GHD_anzkin2, 0) AS GHD_anzkin2,
    COALESCE(child.GHD_anzkin3, 0) AS GHD_anzkin3,
    COALESCE(child.GHD_anzkin4, 0) AS GHD_anzkin4,
    COALESCE(child.GHD_kbett, 0) AS GHD_kbett,
    CASE
            WHEN subquery_alias.FA_date = main.GHR_datumvon THEN 1
            WHEN subquery_alias.FA_date  = main.GHR_datumbis THEN 3
            ELSE 2
        END AS GHR_resstatus,
    main.GHR_typ

FROM 
    (SELECT
        r.FA_reservationid AS id,
        r.FA_date,
        r.FA_taacode, 
        SUM(r.FA_n_amount) AS total_n_amount,
        SUM(r.FA_g_amount) AS total_g_amount
    FROM 
        V2I_Folios_Apaleo r
    WHERE 
        r.FA_taacode IS NOT NULL and r.FA_source = "folios" and r.FA_impdate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY 
        r.FA_reservationid, r.FA_taacode, r.FA_date) subquery_alias 
        
LEFT JOIN 
    V2I_GHR_Apaleo main ON subquery_alias.id = main.GHR_bookingid
    
LEFT JOIN
AggregatedChildGroups child on subquery_alias.id = child.CG_reservationid ;

"""

insert_query= """INSERT INTO V2I_GHDD_Apaleo(GHD_reservationid,GHD_datum,GHD_TAA,GHD_n_,GHD_g_,GHD_roomnights,GHD_mpehotel,GHD_anzerw, GHD_leistacc, GHD_anzkin1, GHD_anzkin2,GHD_anzkin3,GHD_anzkin4, GHD_kbett, GHD_resstatus,GHD_typ, GHD_sysimport) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s,%s,%s, %s) """


delete_query = """DELETE FROM V2I_GHDD_Apaleo WHERE GHD_reservationid = %s and GHD_datum = %s and GHD_TAA = %s"""


cursor_target.execute(select_query,)
log_message("GHDD  - Fetching results...")

results = cursor_target.fetchall()

log_message("GHDD  - insert- delete.. ")


for result in results:
    cursor_target.execute(delete_query, (result[0],result[1],result[2],))

    cursor_target.execute(insert_query, result+ ("guesthistorydaily_detailed_apaleo_" + str(dt.date.today())+".7z",))
connection_target.commit()


log_message("GHDD  - finished")
