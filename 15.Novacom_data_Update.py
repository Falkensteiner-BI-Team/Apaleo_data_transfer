import mysql.connector
import userdata
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()

import_date = dt.date.today()
import_date_string = f"guesthistorydailynovacom_{import_date}"



def onetime_novacom_ghd_insert(nov_property, launch_date):
    select_qry = f"""
    SELECT PAS_Protel_ID, NovD_DAT, SUM(NovD_netto)
    FROM V2I_NovacomDaily
    LEFT JOIN V2D_Property_Attributes 
    ON NovD_Firma = PAS_NovacomID
    
    WHERE NovD_Zahlung1 NOT IN (10, 11)
    AND NovD_Firma = '{nov_property}'
    AND NovD_DAT >= '{launch_date}'
    GROUP BY PAS_Protel_ID, NovD_DAT;
    """

    insert_qry = f"""
    INSERT INTO V2I_GHD_Novacom(GHD_mpehotel, GHD_Datum, GHD_n_fb, GHD_sysimport)
    VALUES (%s, %s, %s, '{import_date_string}')
    """

    delete_qry = """delete from V2I_GHD_Novacom where GHD_mpehotel =%s and GHD_Datum =%s"""

    cursor_target.execute(select_qry, )

    for row in cursor_target.fetchall():
        cursor_target.execute(delete_qry, (row[0], row[1]))
        cursor_target.execute(insert_qry, row)
    connection_target.commit()


#onetime_novacom_ghd_insert('1102', '2024-05-07')  # SA

#onetime_novacom_ghd_insert('5108', '2024-06-26')  # KP

#onetime_novacom_ghd_insert('3108', '2024-04-09')  # CZ


def onetime_novacom_ghr_insert(nov_property, launch_date):
    select_qry = f"""
    SELECT PAS_Protel_ID, NovD_DAT, SUM(NovD_netto)
    FROM V2I_NovacomDaily
    LEFT JOIN V2D_Property_Attributes 
    ON NovD_Firma = PAS_NovacomID

    WHERE NovD_Zahlung1 NOT IN (10, 11)
    AND NovD_Firma = '{nov_property}'
    AND NovD_DAT >= '{launch_date}'
    GROUP BY PAS_Protel_ID, NovD_DAT;
    """

    insert_qry = f"""
    INSERT INTO V2I_GHR_Novacom(GHR_mpehotel, GHR_datumvon,GHR_datumbis, GHR_sysimport)
    VALUES (%s, %s, %s, '{import_date_string}')
    """

    delete_qry = """delete from V2I_GHD_Novacom where GHD_mpehotel =%s and GHD_Datum =%s"""

    cursor_target.execute(select_qry, )

    for row in cursor_target.fetchall():
        cursor_target.execute(delete_qry, (row[0], row[1]))
        cursor_target.execute(insert_qry, row)
    connection_target.commit()

