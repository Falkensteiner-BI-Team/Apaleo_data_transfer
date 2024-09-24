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
import_date_str = import_date.strftime('%Y-%m-%d')

today = str(import_date)

log_message("Folios - folios confirmed update started")

print(dt.date.today() - dt.timedelta(days=3))


def Insert_Confirmed_Group_Booking_Folios():
    qry_insert = """insert into V2I_Folios_Apaleo(FA_date, FA_reservationid, FA_n_amount, FA_g_amount, FA_status, FA_source, FA_impdate, FA_roomnights,FA_taacode) VALUES(%s,%s, %s, %s, %s, %s, %s, %s, %s)"""
    qry_delete = """delete from V2I_Folios_Apaleo where FA_reservationid = %s"""
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

Insert_Confirmed_Group_Booking_Folios()







rateplan_typ_mapping = {

        'COMPLIMENTARY': 1,
        'COMP': 1,
        'COMP City': 1,
        'COMP HB': 1,
        'ZERO': 2,
        'HOUSE': 4,
        'COMP_HB': 1
    }
reschar_mapping = {
    'checkedout': 0,
    'inhouse': 0,
    'canceled': 2,
    'noshow': 3,
    'confirmed': 0,
    'tentative' : 4,
'definite':0


}

def Insert_Confirmed_Group_Booking_GFR(import_date):
    qry_insert = """INSERT INTO `V2I_GFR_Apaleo`(
    `GFR_bookingid`,
    `GFR_reservationid`,
    `GFR_code2`,
    `GFR_datumres`,
    `GFR_datumvon`,
    `GFR_datumbis`,
    `GFR_unit_group_code`,
    `GFR_zimmernr`,
     `GFR_datumcxl`,   
     `GFR_updated`,
     `GFR_rateplan`,
     `GFR_typ`,
     `GFR_reschar`,
     `GFR_status`,
     `GFR_datumimp`,
     `GFR_sysimport`)Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    qry_delete = f""" DELETE from `V2I_GFR_Apaleo` where `GFR_reservationid` = %s and `GFR_datumimp`= %s """
    bookings = APIClient(
        'https://api.apaleo.com/booking/v1/blocks?expand=timeSlices&from=' +str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z',
        get_token()).get_data()

    for block in bookings["blocks"]:
        cursor_target.execute(qry_delete, (str(block["property"]["id"])+'-'+str(block['id']),str(import_date),))
        timeslices = block['timeSlices']
        for slice in timeslices:
            print(slice)

            blockedUnits = slice['blockedUnits']
            pickedUnits = slice['pickedUnits']
            unit_dif = blockedUnits - pickedUnits
            print(unit_dif)
            if unit_dif > 0:

                params_room = (
                    str(block["property"]["id"])+'-'+str(block['id']),
                    str(block["property"]["id"])+'-'+str(block['id']),
                    block["property"]["id"][-2:],
                    datetime.fromisoformat(block['from']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(block['from']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(block['to']).strftime('%Y-%m-%d'),
                    block['unitGroup']['code'],
                    unit_dif,
                    '1900-01-01',
                    datetime.fromisoformat(block['modified']).strftime('%Y-%m-%d'),
                    block['ratePlan']['code'],
                    rateplan_typ_mapping.get(block['ratePlan']['code'], 0),
                    reschar_mapping.get(block['status'].lower(), None),
                    block['status'].lower(),
                    str(import_date),
                    "guestfuturereservationapaleo_" + str(import_date)
                )

                cursor_target.execute(qry_insert, params_room)



    connection_target.commit()

Insert_Confirmed_Group_Booking_GFR(today)



def Preprocess_and_Update(column, mappedcolumn, mappingtable, keycolumn, keycolumnapaleo,datumimp,default_value):
    update_query = f"""
        UPDATE `V2I_GFR_Apaleo` AS v
        LEFT JOIN {mappingtable} AS m ON v.{keycolumn} = m.{keycolumnapaleo}
        SET v.{column} =  COALESCE(m.{mappedcolumn}, {default_value})
        WHERE DATE(v.GFR_datumimp) = '{datumimp}'
        """

    cursor_target.execute(update_query, )


log_message("GFR - GFR  preprocessing and updates")

Preprocess_and_Update("GFR_leistacc", "Seq_ID", "V2D_res_num_mapping", "GFR_reservationid", "Apaleo_res_ID", import_date_str, "NULL")
log_message("GFR - GFR  leistaccs updated")

connection_target.commit()

Preprocess_and_Update("GFR_mpehotel", "PAS_Protel_ID", "V2D_Property_Attributes", "GFR_code2", "PAS_code2",import_date_str,"NULL")
log_message("GFR - GFR  mpehotel updated")

connection_target.commit()
Preprocess_and_Update("GFR_katnr", "Rooms", "Protel_HotelInventory", "GFR_unit_group_code", "RoomType", import_date_str, 0)
log_message("GFR - GFR  katnr updated")

