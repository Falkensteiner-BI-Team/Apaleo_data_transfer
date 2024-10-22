import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt


# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()

def log_message(message, file_path='Apaleo_log.txt'):
    with open(file_path, 'a') as file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file.write(f'{timestamp} - {message}\n')



log_message("GFR - GFR  update started")


def Insert_API_Results(importdate):
    get_reservations = APIClient('https://api.apaleo.com/booking/v1/reservations?dateFilter=Modification&from='+str(dt.date.today() - dt.timedelta(days=3)) +'T00:00:00Z', get_token()).get_data()

    channel_code_mapping = {
        'direct': 145,
        'ibe': 2,
        'bookingcom': 144,
        'channelmanager': 141,
         'expedia': 143,
        'homelike': 8,
        'hrs': 142,
        'altovita': 8,
        'desvu': 8
    }

    reschar_mapping = {
        'checkedout': 0,
        'inhouse': 0,
        'canceled': 2,
        'noshow': 3,
        'confirmed':0

    }
    rateplan_typ_mapping = {

        'COMPLIMENTARY': 1,
        'COMP': 1,
        'COMP City': 1,
        'COMP HB': 1,
        'ZERO': 2,
        'HOUSE': 4,
        'COMP_HB': 1
    }

    for reservation in get_reservations["reservations"]:

        if reservation['property']['id'] not in ["BER", "LND", "MUC", "PAR", "VIE"]:
            departure = datetime.fromisoformat(reservation.get("departure")).strftime('%Y-%m-%d')
            today = date.today().strftime('%Y-%m-%d')
            if departure >= today:
                # Construct the parameter tuple for the query
                #print(datetime.fromisoformat(departure).strftime('%Y-%m-%d'))
                params = (
                    str(reservation.get('id')),
                    str(reservation.get('property').get('id')) + "-" + str(reservation.get('id')),
                    reservation.get('property').get('id')[-2:],
                    str(importdate),

                    datetime.fromisoformat(reservation.get("created")).strftime('%Y-%m-%d') if reservation.get(
                        "created") else None,
                    datetime.fromisoformat(reservation.get("arrival")).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(reservation.get("departure")).strftime('%Y-%m-%d'),
                    reservation.get("status").lower(),
                    reschar_mapping.get(reservation.get("status").lower(), None),

                    reservation.get("unitGroup", {}).get("code")[:-1] if reservation.get("unitGroup",{}).get("code") in ("PBL", "PGO", "PDI") else reservation.get("unitGroup", {}).get("code"),
                    reservation.get("unit", {}).get('name'),
                    reservation.get("marketSegment", {}).get("code") if reservation.get("marketSegment", {}).get("code") else "INDIVIDUAL",
                    channel_code_mapping.get(reservation.get("channelCode").lower(), None),
                    reservation.get("primaryGuest", {}).get("birthDate"),
                    reservation.get("company", {}).get("id"),

                    reservation.get("primaryGuest").get("nationalityCountryCode", "XX") + "_XXX_XXX",

                    reservation.get("primaryGuest").get("nationalityCountryCode", "XX") + "_" + reservation.get(
                        "primaryGuest").get("address", {}).get("postalCode", "XXX") + "_" + reservation.get(
                        "primaryGuest").get("address", {}).get("city", "XXX"),

                    datetime.fromisoformat(reservation.get("cancellationTime")).strftime('%Y-%m-%d') if reservation.get(
                        "cancellationTime") else "1900-01-01",

                    str(reservation.get('property').get('id')) + "-" + reservation.get("bookingId") + "-1",
                    reservation.get("externalCode", None),
                    "guestfuturereservationapaleo_" + str(importdate),
                    str(reservation.get("childrenAges")) if reservation.get("childrenAges") else 0,
                    reservation.get("adults", {}),
                    datetime.fromisoformat(reservation.get("modified")).strftime('%Y-%m-%d'),
                    reservation.get("ratePlan").get("code"),
                    rateplan_typ_mapping.get(reservation.get("ratePlan").get("code"), 0),


                )

                qry_insert = """INSERT INTO `V2I_GFR_Apaleo`(`GFR_bookingid`,`GFR_reservationid`,`GFR_code2`,`GFR_datumimp`,`GFR_datumres`,`GFR_datumvon`,`GFR_datumbis`,`GFR_status`, `GFR_reschar`, `GFR_unit_group_code`, `GFR_unitname`,`GFR_market_segment`,`GFR_channelCode`,`GFR_kunden_DOB`,`GFR_company`,`GFR_nat_zipcodekey`,`GFR_res_zipcodekey`,`GFR_datumcxl`, `GFR_bookingid_sharer`,`GFR_crsnr`, `GFR_sysimport`,`GFR_ChildAges`,`GFR_Adults`,`GFR_updated`,`GFR_rateplan`,`GFR_typ`) 
                                                            Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                                            """




                qry_delete = f""" DELETE from `V2I_GFR_Apaleo` where `GFR_reservationid` = %s and `GFR_datumimp`= %s """

                cursor_target.execute(qry_delete, (params[1], params[3]))
                cursor_target.execute(qry_insert, params)
                #print(params)

                connection_target.commit()




def Insert_Confirmed_Group_Booking_GFR(import_date):
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
        'tentative': 4,
        'definite': 0,
        'optional': 4

    }

    qry_insert = """INSERT INTO `V2I_GFR_Apaleo`(
    `GFR_bookingid`,
    `GFR_reservationid`,
    `GFR_code2`,
    `GFR_datumres`,
    `GFR_datumvon`,
    `GFR_datumbis`,
    `GFR_unit_group_code`,
    `GFR_roomnights`,
     `GFR_datumcxl`,   
     `GFR_updated`,
     `GFR_rateplan`,
     `GFR_typ`,
     `GFR_reschar`,
     `GFR_status`,
     `GFR_datumimp`,
     `GFR_sysimport`,
     `GFR_Adults`)Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    qry_delete = f""" DELETE from `V2I_GFR_Apaleo` where `GFR_reservationid` = %s and `GFR_datumimp`= %s """
    bookings = APIClient(
        'https://api.apaleo.com/booking/v1/blocks?expand=timeSlices&from=' + str(dt.date.today() - dt.timedelta(days=3)) + 'T00:00:00Z',
        get_token()).get_data()

    for block in bookings["blocks"]:

       # timeslices = block['timeSlices']
       # for slice in timeslices
        #print(block['timeSlices'][0]['blockedUnits'])

        blockedUnits = block['timeSlices'][0]['blockedUnits']
        pickedUnits = block['timeSlices'][0]['pickedUnits']
        unit_dif = blockedUnits - pickedUnits

        if unit_dif > 0:
                params_room = (
                    str(block['id']),
                    str(block["property"]["id"])+'-'+str(block['id']),
                    block["property"]["id"][-2:],
                    datetime.fromisoformat(block['created']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(block['from']).strftime('%Y-%m-%d'),
                    datetime.fromisoformat(block['to']).strftime('%Y-%m-%d'),
                    block['unitGroup']['code'],
                    unit_dif,
                    '1900-01-01',
                    datetime.fromisoformat(block['modified']).strftime('%Y-%m-%d'),
                    block['ratePlan']['code'],
                    rateplan_typ_mapping.get(block['ratePlan']['code'], 0),
                    4,
                    'optional',
                    str(import_date),
                    "guestfuturereservationapaleo_" + str(import_date),
                    unit_dif
                )
                print(params_room[14])
                print(params_room[1])
                cursor_target.execute(qry_delete,(params_room[1],params_room[14]))
                cursor_target.execute(qry_insert, params_room)



    connection_target.commit()




#import_date = dt.date.today()- dt.timedelta(days=1)
#import_date = dt.date(2024, 8, 21)
import_date = dt.date.today()
log_message("GFR - GFR  Inserting Api results...")

Insert_API_Results(import_date)
log_message("GFR - GFR  Inserting Api results from reservations done!")

today = str(import_date)
Insert_Confirmed_Group_Booking_GFR(today)

log_message("GFR - GFR  Inserting Api results from blocks done!")

import_date_str = import_date.strftime('%Y-%m-%d')



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
connection_target.commit()

cursor_target.execute(f"""UPDATE  V2I_GFR_Apaleo SET GFR_market_segment= 'MICEGR' WHERE GFR_rateplan LIKE '%GR%' AND (GFR_market_segment = 'INDIVIDUAL' OR GFR_market_segment IS NULL) AND DATE(GFR_datumimp) = '{import_date_str}' """,)
cursor_target.execute(f"""UPDATE  V2I_GFR_Apaleo SET GFR_market_segment= 'LEISUREGR' WHERE GFR_rateplan LIKE '%CR%' AND (GFR_market_segment = 'INDIVIDUAL' OR GFR_market_segment IS NULL) AND DATE(GFR_datumimp) = '{import_date_str}' """,)
log_message("GFR - GFR  market segment updated")
connection_target.commit()

Preprocess_and_Update("GFR_market", "MK_Nr", "V2D_Apaleo_Market", "GFR_market_segment", "Apaleo_Mkt_code", import_date_str,"NULL")
log_message("GFR - GFR  markets updated")

connection_target.commit()
Preprocess_and_Update("GFR_sharenr", "Seq_ID", "V2D_res_num_mapping", "GFR_bookingid_sharer", "Apaleo_res_ID",import_date_str, "NULL")
log_message("GFR - GFR  sharenr updated")
connection_target.commit()

log_message("GFR - GFR  preprocessing done ")

log_message("GFR - GFR updating room number")

def Update_roomnr(datumimp):
    update_query = f"""
        UPDATE `V2I_GFR_Apaleo` AS v
        LEFT JOIN `V2D_ApaleoRoomCategories` AS m ON v.GFR_unit_group_code = m.Unit_group_code
        SET v.GFR_zimmer = CASE 
        WHEN m.Types = "bedroom"  THEN 1
        ELSE 0 
        END
        WHERE DATE(v.GFR_datumimp) = '{datumimp}'
        """

    cursor_target.execute(update_query, )

    connection_target.commit()


Update_roomnr(import_date_str)

log_message("GFR - GFR  room update done")