import mysql.connector
from APIClient import *
from datetime import datetime
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()




def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')

'''
CREATE TABLE IF NOT EXISTS Numbers (
    number INT PRIMARY KEY
);
INSERT INTO Numbers (number)
SELECT (a.n + b.n * 10 + c.n * 1000000) AS number
FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c;
'''

select_query = """
WITH DetailedGHR AS (
    SELECT 
    main.GHR_bookingid,
		main.GHR_reservationid,
        main.GHR_leistacc,
        main.GHR_mpehotel,
        main.GHR_datumimp,
        main.GHR_Adults,
        DATE_ADD(main.GHR_datumvon, INTERVAL Numbers.number DAY) AS GHD_datum,
        main.GHR_zimmernr,
        CASE
             WHEN main.GHR_zimmer = 0 THEN 0
                  ELSE CASE
                        WHEN DATE_ADD(main.GHR_datumvon, INTERVAL Numbers.number DAY) = main.GHR_datumbis THEN 0
                         ELSE 1
                  END
              END AS GHD_roomnights,
        CASE
            WHEN DATE_ADD(main.GHR_datumvon, INTERVAL Numbers.number DAY) = main.GHR_datumvon THEN 1
            WHEN DATE_ADD(main.GHR_datumvon, INTERVAL Numbers.number DAY) = main.GHR_datumbis THEN 3
            ELSE 2
        END AS GHD_resstatus,
        main.GHR_typ,
        main.GHR_datumvon,
        main.GHR_datumbis,
        main.GHR_datumcxl
    FROM 
        V2I_GHR_Apaleo AS main
    JOIN 
        Numbers ON Numbers.number <= DATEDIFF(main.GHR_datumbis, main.GHR_datumvon)
    WHERE 
         main.GHR_datumimp > DATE_SUB(CURDATE(), INTERVAL 3 DAY)

),

AggregatedChildGroups AS (
    SELECT
        -- g.GHR_leistacc,
        g.GHR_reservationid,
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
        g.GHR_reservationid
), 

Folios AS(

SELECT 
    revenue.id,
    revenue.FA_date,
    CAST(SUM(revenue.total_n_amount) AS DECIMAL(18,8)) AS total_n_amount,
    CAST(SUM(revenue.total_g_amount) AS DECIMAL(18,8)) AS total_g_amount,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'Logis' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS logis,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'F&B' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS fb,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'Spa' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS spa,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'Ski' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS ski,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 NOT IN ('Logis', 'F&B', 'Spa','Ski') OR t.TAA_GroupOrd1 IS NULL THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS Other

FROM (
    SELECT
        r.FA_reservationid AS id,
        r.FA_date,
        r.FA_taacode, 
        CAST(SUM(r.FA_n_amount) AS DECIMAL(18,8) ) AS total_n_amount,
        CAST(SUM(r.FA_g_amount) AS DECIMAL(18,8)) AS total_g_amount
    FROM 
        V2I_Folios_Apaleo r
    WHERE 
        r.FA_taacode IS NOT NULL and r.FA_source IN ("folios","folios_booking","folios_external")
    GROUP BY 
        r.FA_reservationid, r.FA_taacode, r.FA_date
) revenue
LEFT OUTER JOIN V2D_TAA t ON revenue.FA_taacode = t.TAA_TAA
GROUP BY
    revenue.id, revenue.FA_date
)



SELECT
    d.GHR_reservationid,
    d.GHR_leistacc,
    d.GHR_mpehotel,
    d.GHR_datumimp,
    d.GHD_datum,
    d.GHR_zimmernr,
    d.GHD_roomnights,
    d.GHD_resstatus,
    d.GHR_typ,
    -- d.GHR_datumvon,
   --  d.GHR_datumbis,
    d.GHR_Adults,
    d.GHR_datumcxl,
    a.GHD_anzkin1,
    a.GHD_anzkin2,
    a.GHD_anzkin3,
    a.GHD_anzkin4,
    a.GHD_kbett,
    COALESCE(f.logis, 0) AS logis,
    COALESCE(f.fb, 0) AS fb,
    COALESCE(f.spa, 0) AS spa,
    COALESCE(f.ski, 0) AS ski,
    COALESCE(f.other, 0) AS other, 
    CONCAT('guesthistorydailyapaleo_', GHR_datumimp)
FROM
    DetailedGHR d
LEFT JOIN
    AggregatedChildGroups a ON d.GHR_reservationid = a.GHR_reservationid
LEFT JOIN
	Folios f ON d.GHR_bookingid = f.id and d.GHD_datum = f.FA_date
"""



Insert_qry = """insert into V2I_GHD_Apaleo(GHD_reservationid, GHD_leistacc,GHD_mpehotel,GHD_datumimp, GHD_datum,GHD_zimmernr,GHD_roomnights,GHD_resstatus,GHD_typ,GHD_anzerw,GHD_datumcxl, GHD_anzkin1, GHD_anzkin2,GHD_anzkin3,GHD_anzkin4,GHD_kbett,GHD_n_logis, GHD_n_fb, GHD_n_spa, GHD_n_ski,GHD_n_other, GHD_sysimport)VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s)"""

delete_qry = """delete from V2I_GHD_Apaleo where GHD_reservationid=%s and GHD_datum =%s"""

log_message("GHD - GHD started...")

cursor_target.execute(select_query, )

log_message("GHD - GHD select query done...")
results = cursor_target.fetchall()


log_message("GHD - GHD inserting...")

for result in results:
    cursor_target.execute(delete_qry, (result[0], result[4],))
    connection_target.commit()

    cursor_target.execute(Insert_qry, result)
    connection_target.commit()

log_message("GHD - GHD Finished...")
