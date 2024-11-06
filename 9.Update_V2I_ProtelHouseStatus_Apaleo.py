import mysql.connector
from APIClient import *
from datetime import datetime
import datetime as dt


def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')


log_message("PHS - protel house started")

connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()


def update_Inventory_Apaleo(import_date):
    properties = ["FCZ", "FSA", "FKP", "FST", "FHS","FBL", "FSG","FSV","FCA","FCR","FED","FFK","FMO"]

    for property in properties:
        get_oos = APIClient(
            'https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfService',
            get_token()).get_data()

        get_ooo = APIClient(
            'https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfOrder',
            get_token()).get_data()

        get_ooi = APIClient(
            'https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfInventory',
            get_token()).get_data()

        insert_qry_ic = """INSERT INTO  V2I_Inventory_Apaleo( IC_property,IC_date, IC_ooo, IC_oos,IC_ooi) VALUES(%s, %s, %s, %s, %s)"""

        delete_qry_ic = """DELETE FROM V2I_Inventory_Apaleo WHERE IC_property= %s AND IC_date= %s"""

        cursor_target.execute(delete_qry_ic, (property, import_date))

        cursor_target.execute(insert_qry_ic,
                              (property, import_date, get_ooo["count"], get_oos["count"], get_ooi["count"]))
    connection_target.commit()


log_message("PHS - Inventory started.. ")
date = import_date = dt.date.today()
update_Inventory_Apaleo(date)
log_message("PHS - Inventory ended... ")
# GHR_res char NOT IN (3,2)- we re excluding no-show/canceled ones

select_query = """

SELECT 
    GHD_mpehotel,  
    GHD_datum, 
   SUM(CASE WHEN GHR_reschar NOT IN (3,2) THEN GHD_roomnights ELSE 0 END) AS rooms_occupied,
    COUNT(CASE WHEN GHD_resstatus = 1 AND GHR_reschar NOT IN (3,2) THEN 1 END) AS arrivals,
    COUNT(CASE WHEN GHD_resstatus = 3 AND GHR_reschar NOT IN (3,2) THEN 1 END) AS departures,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) AND GHR_unit_group_type="bedroom" THEN GHD_anzerw ELSE 0 END) AS persons_inhouse,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) THEN GHD_anzkin1 ELSE 0 END) AS child1_inhouse,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) THEN GHD_anzkin2 ELSE 0 END) AS child2_inhouse,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) THEN GHD_anzkin3 ELSE 0 END) AS child3_inhouse,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) THEN GHD_anzkin4 ELSE 0 END) AS child4_inhouse,
    SUM(CASE WHEN GHD_resstatus IN (1,2) AND GHR_reschar NOT IN (3,2) THEN GHD_kbett ELSE 0 END) AS cot_inhouse,
    SUM(CASE WHEN GHR_kattyp = 4 AND GHR_reschar NOT IN (3,2) THEN GHD_roomnights ELSE 0 END) AS PHS_rooms_houseuse,
    SUM(CASE WHEN GHR_kattyp = 4 AND GHR_reschar NOT IN (3,2) THEN GHD_anzerw ELSE 0 END) AS PHS_persons_houseuse,
    SUM(CASE WHEN GHR_kattyp = 1 AND GHR_reschar NOT IN (3,2) THEN GHD_roomnights ELSE 0 END) AS PHS_rooms_complimentary,
    SUM(CASE WHEN GHR_reschar = 3 AND GHD_resstatus = 1 THEN GHD_roomnights ELSE 0 END) AS PHS_rooms_noshow 
FROM 
    V2I_GHD_Apaleo  
LEFT JOIN 
    V2D_Property_Attributes 
ON  
    GHD_mpehotel = PAS_Protel_ID
LEFT JOIN 
    V2I_GHR_Apaleo
    ON GHD_reservationid = GHR_reservationid 
WHERE 
    GHD_datumcxl = '1900-01-01' 
    AND GHD_datumimp >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)

GROUP BY 
    GHD_mpehotel, GHD_datum
ORDER BY 
    GHD_mpehotel, GHD_datum;

"""
# -- AND GHD_datumimp >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)

insert_query = """INSERT INTO V2I_ProtelHouseStatus_Apaleo(PHS_mpehotel,PHS_date,PHS_rooms_occupied,PHS_rooms_arrival,PHS_rooms_departure,PHS_persons_inhouse,PHS_child1_inhouse,PHS_child2_inhouse,PHS_child3_inhouse,PHS_child4_inhouse,PHS_cot_inhouse,PHS_rooms_houseuse,
PHS_persons_houseuse,PHS_rooms_complimentary,PHS_rooms_noshow) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

delete_query = """DELETE from V2I_ProtelHouseStatus_Apaleo WHERE  PHS_mpehotel =%s AND PHS_date = %s"""

# run select statement
log_message("PHS - calling select statement")

cursor_target.execute(select_query, )

log_message("PHS - fetching results")

for row in cursor_target.fetchall():
    cursor_target.execute(delete_query, (row[0], row[1],))
    print((row[0], row[1]))
    cursor_target.execute(insert_query, row)
connection_target.commit()


log_message("PHS - updating  results")

update_qry1 = """UPDATE V2I_ProtelHouseStatus_Apaleo LEFT JOIN V2D_Property_Attributes ON  PHS_mpehotel = PAS_Protel_ID  SET PHS_amount_room = PAS_amount_room WHERE PHS_mpehotel = PAS_Protel_ID"""
update_qry2 = """UPDATE V2I_ProtelHouseStatus_Apaleo LEFT JOIN V2D_Property_Attributes ON  PHS_mpehotel = PAS_Protel_ID  SET PHS_code3 =   CONCAT('F',PAS_code2) WHERE PHS_mpehotel = PAS_Protel_ID"""
update_qry3 = """UPDATE V2I_ProtelHouseStatus_Apaleo  LEFT JOIN V2I_Inventory_Apaleo ON  PHS_code3 = IC_property SET PHS_oos = IC_oos WHERE PHS_code3 = IC_property AND PHS_date = IC_date"""
update_qry4 = """UPDATE V2I_ProtelHouseStatus_Apaleo  SET PHS_rooms_available = PHS_amount_room - PHS_oos - PHS_rooms_complimentary -  PHS_rooms_houseuse - PHS_rooms_occupied """



cursor_target.execute(update_qry1,)
cursor_target.execute(update_qry2,)
cursor_target.execute(update_qry3,)
cursor_target.execute(update_qry4,)

connection_target.commit()



select_query2 = """ SELECT PHS_mpehotel,PHS_date,PHS_rooms_occupied,PHS_rooms_available,PHS_rooms_arrival,PHS_rooms_departure,PHS_persons_inhouse,PHS_child1_inhouse,PHS_child2_inhouse,PHS_child3_inhouse,PHS_child4_inhouse,PHS_cot_inhouse,PHS_oos, PHS_rooms_houseuse, PHS_persons_houseuse,PHS_rooms_complimentary,PHS_rooms_noshow FROM V2I_ProtelHouseStatus_Apaleo"""

insert_query2 = """insert into V2I_ProtelHouseStatus(PHS_mpehotel,PHS_date,PHS_rooms_occupied,PHS_rooms_available,PHS_rooms_arrival,PHS_rooms_departure,PHS_persons_inhouse,PHS_child1_inhouse, PHS_child2_inhouse,PHS_child3_inhouse,PHS_child4_inhouse, PHS_cot_inhouse,PHS_oos, PHS_rooms_houseuse, PHS_persons_houseuse,PHS_rooms_complimentary,PHS_rooms_noshow) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

delete_query2 = """delete from V2I_ProtelHouseStatus where PHS_mpehotel = %s AND PHS_date = %s """

cursor_target.execute(select_query2,)
log_message("PHS - updating main table... ")

for row in cursor_target.fetchall():
    cursor_target.execute(delete_query2, (row[0],row[1],))
    cursor_target.execute(insert_query2, row)
connection_target.commit()
