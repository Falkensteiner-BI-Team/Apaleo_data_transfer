import mysql.connector
from APIClient import *
from datetime import datetime
import datetime as dt


def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')


connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()


def update_Inventory_Apaleo(import_date):
    properties = ["FCZ", "FSA", "FKP", "FST", "FHS","FBL"]

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