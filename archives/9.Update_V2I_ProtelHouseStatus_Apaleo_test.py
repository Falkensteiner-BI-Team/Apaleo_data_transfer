import mysql.connector
from APIClient import *
from datetime import datetime
import datetime as dt

import_date = dt.date.today()


connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()

# ["C2D","E2D","F4C","F3D","F4D","F4E","S4C","S4D","S4E","PMGM","MEET","SPA"],

properties = ["FCZ", "FSA", "FKP", "FST"]

for property in properties:
    get_oos = APIClient('https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfService',get_token()).get_data()


    get_ooo = APIClient(
        'https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfOrder',
        get_token()).get_data()



    get_ooi = APIClient(
        'https://api.apaleo.com/inventory/v1/units/$count?propertyId=' + property + '&maintenanceType=OutOfInventory',
        get_token()).get_data()



    insert_qry_ic= """INSERT INTO  V2I_Inventory_Apaleo( IC_property,IC_date, IC_ooo, IC_oos,IC_ooi) VALUES(%s, %s, %s, %s, %s)"""

    delete_qry_ic = """DELETE FROM V2I_Inventory_Apaleo WHERE IC_property= %s AND IC_date= %s"""

    cursor_target.execute(delete_qry_ic, (property, import_date))


    cursor_target.execute(insert_qry_ic,(property,import_date, get_ooo["count"], get_oos["count"],get_ooi["count"]))
connection_target.commit()


