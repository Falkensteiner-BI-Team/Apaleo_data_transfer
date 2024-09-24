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



import_date = dt.date.today()
today = str(import_date)

print(import_date)
import_date_str = import_date.strftime('%Y-%m-%d')
log_message("Folios - folios confirmed update started")

print(dt.date.today() - dt.timedelta(days=3))


def Insert_Confirmed_Group_Booking():
    qry_insert = """insert into V2I_Folios_Apaleo_test(FA_date, FA_reservationid, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_roomnights,FA_taacode) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s)"""
    qry_delete = """delete from V2I_Folios_Apaleo_test where FA_reservationid = %s"""
    bookings = APIClient(
        'https://api.apaleo.com/booking/v1/blocks?expand=timeSlices&from=' +str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z',
        get_token()).get_data()

    for block in bookings["blocks"]:
        cursor_target.execute(qry_delete, (block['id'],))
        timeslices = block['timeSlices']
        for slice in timeslices:
            print(slice)
            blockedUnits = slice['blockedUnits']
            pickedUnits = slice['pickedUnits']
            unit_dif = blockedUnits - pickedUnits
            print(unit_dif)
            net_room = int(slice['baseAmount']['netAmount'] * unit_dif)
            gross_room = int(slice['baseAmount']['grossAmount'] * unit_dif)
            if unit_dif > 0:
                params_room = (
                    datetime.fromisoformat(slice['from']).strftime('%Y-%m-%d'),
                    block['id'],
                    net_room,
                    gross_room,
                    "inhouse/confirmed",
                    "blocks_room",
                    today,
                    unit_dif,
                    101000
                )

                cursor_target.execute(qry_insert, params_room)

                if int(slice['totalGrossAmount']['amount']) > int(slice['baseAmount']['grossAmount']):
                    net_other = ((int(slice['totalGrossAmount']['amount']) * unit_dif) - gross_room)*0.90
                    gross_other = (int(slice['totalGrossAmount']['amount']) * unit_dif) - gross_room


                    params_other = (
                        datetime.fromisoformat(slice['from']).strftime('%Y-%m-%d'),
                        block['id'],
                        net_other,
                        gross_other,
                        "inhouse/confirmed",
                        "blocks_F&B",
                        today,
                        unit_dif,
                        102021
                    )
                    cursor_target.execute(qry_insert, params_other)


    connection_target.commit()

Insert_Confirmed_Group_Booking()

select_qry = """ WITH Folios AS(
SELECT 
    revenue.id,
    revenue.FA_date,
		revenue.FA_impdate, 
		revenue.FA_roomnights,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'Logis' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS logis,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'F&B' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS fb,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 NOT IN ('Logis', 'F&B') OR t.TAA_GroupOrd1 IS NULL THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS Other,
    CONCAT('guestfuturedailyapaleo_', CURDATE())
FROM (
    SELECT
        r.FA_reservationid AS id,
        r.FA_date,
        r.FA_impdate,
        r.FA_taacode,
		  r.FA_roomnights, 
        CAST(SUM(r.FA_n_amount) AS DECIMAL(18,8)) AS total_n_amount,
        CAST(SUM(r.FA_g_amount) AS DECIMAL(18,8)) AS total_g_amount
    FROM 
        V2I_Folios_Apaleo_test r
    WHERE 
        r.FA_taacode IS NOT NULL AND r.FA_impdate >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
    GROUP BY 
        r.FA_reservationid, r.FA_taacode, r.FA_date,  r.FA_impdate, r.FA_roomnights
) revenue
LEFT OUTER JOIN V2D_TAA t ON revenue.FA_taacode = t.TAA_TAA
GROUP BY
    revenue.id, revenue.FA_date,revenue.FA_impdate, revenue.FA_roomnights) SELECT * FROM Folios """


insert_qry = """
insert into V2I_GFD_Apaleo_test
(GFD_reservationid, 
GFD_datum,
GFD_datumimp,
GFD_roomnights,
GFD_n_logis,
GFD_n_fb,
GFD_n_other,
GFD_sysimport)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s) 
"""





cursor_target.execute(select_qry, )

results = cursor_target.fetchall()

for result in results:

    cursor_target.execute(
        """delete from V2I_GFD_Apaleo_test WHERE GFD_reservationid=%s AND GFD_datum=%s AND GFD_datumimp=%s""", (result[0], result[1], result[2],))

    connection_target.commit()

    cursor_target.execute(insert_qry, result)
    connection_target.commit()


def Preprocess_and_Update(column, mappedcolumn, mappingtable, keycolumn, keycolumnapaleo,datumimp,default_value):
    update_query = f"""
        UPDATE `V2I_GFD_Apaleo_test` AS v
        LEFT JOIN {mappingtable} AS m ON v.{keycolumn} = m.{keycolumnapaleo}
        SET v.{column} =  COALESCE(m.{mappedcolumn}, {default_value})
        WHERE DATE(v.GFD_datumimp) = '{datumimp}'
        """

    cursor_target.execute(update_query, )

#Preprocess_and_Update("GFD_leistacc", "Seq_ID", "V2D_res_num_mapping", "GFD_reservationid", "Apaleo_res_ID", import_date_str, "NULL")
log_message("GFR - GFR  leistaccs updated")

connection_target.commit()