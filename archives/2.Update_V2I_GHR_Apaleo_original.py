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




log_message("GHR - GHR  update started")

def Insert_API_Results(import_date):
    get_reservations = APIClient('https://api.apaleo.com/booking/v1/reservations', get_token()).get_data()

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
        'checkedout' : 0,
        'inhouse' : 0,
        'canceled' : 2,
        'noshow':3
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

        arrival = datetime.fromisoformat(reservation.get("arrival")).strftime('%Y-%m-%d')
        today = date.today().strftime('%Y-%m-%d')

        if reservation['property']['id'] not in ["BER", "LND", "MUC", "PAR", "VIE"]:
            if arrival > date(2024, 5, 1).strftime('%Y-%m-%d'):

                if arrival < today:
                    #print(datetime.fromisoformat(arrival).strftime('%Y-%m-%d'))
                    #print(reservation)
                    # Construct the parameter tuple for the query
                    params = (
                        str(reservation.get('id')),
                        str(reservation.get('property').get('id')) + "-" + str(reservation.get('id')),
                        reservation.get('property').get('id')[-2:],
                        str(import_date),
                        datetime.fromisoformat(reservation.get("created")).strftime('%Y-%m-%d') if reservation.get(
                            "created") else None,
                        datetime.fromisoformat(reservation.get("arrival")).strftime('%Y-%m-%d'),
                        datetime.fromisoformat(reservation.get("departure")).strftime('%Y-%m-%d'),
                        reservation.get("status").lower(),

                        reschar_mapping.get(reservation.get("status").lower(), None),

                        #reservation.get("unitGroup", {}).get('code'),
                        reservation.get("unitGroup", {}).get("code")[:-1] if reservation.get("unitGroup",
                                                                                                    {}).get(
                            "code") in ("PBL", "PGO", "PDI") else reservation.get("unitGroup", {}).get("code"),

                        reservation.get("unit", {}).get('name'),
                        reservation.get("marketSegment", {}).get("code") if reservation.get("marketSegment", {}).get("code") else "INDIVIDUAL",
                        channel_code_mapping.get(reservation.get("channelCode").lower(), None),
                        reservation.get("primaryGuest", {}).get("birthDate"),
                        reservation.get("company", {}).get("id"),

                        reservation.get("primaryGuest").get("nationalityCountryCode", "XX") + "_XXX_XXX",



                        reservation.get("primaryGuest").get("nationalityCountryCode","XX") + "_" + reservation.get("primaryGuest").get("address", {}).get("postalCode", "XXX") + "_" + reservation.get("primaryGuest").get("address",{}).get("city", "XXX"),

                        datetime.fromisoformat(reservation.get("cancellationTime")).strftime('%Y-%m-%d') if reservation.get("cancellationTime") else "1900-01-01",

                        reservation.get("bookingId") + "-1",
                        reservation.get("externalCode", None),
                        "guesthistoryreservationapaleo_" + str(import_date),
                        str(reservation.get("childrenAges")) if reservation.get("childrenAges") else 0,
                        reservation.get("adults", {}),
                        datetime.fromisoformat(reservation.get("modified")).strftime('%Y-%m-%d'),
                        reservation.get("ratePlan").get("code"),
                        rateplan_typ_mapping.get(reservation.get("ratePlan").get("code"), 0),

                    )

                    qry_insert = """
                            INSERT INTO `V2I_GHR_Apaleo`(`GHR_bookingid`,`GHR_reservationid`,`GHR_code2`,`GHR_datumimp`,`GHR_datumres`,`GHR_datumvon`,`GHR_datumbis`,`GHR_status`, `GHR_reschar`, `GHR_unit_group_code`, `GHR_unitname`,`GHR_market_segment`,`GHR_channelCode`,`GHR_kunden_DOB`,`GHR_company`,`GHR_nat_zipcodekey`,`GHR_res_zipcodekey`,`GHR_datumcxl`, `GHR_bookingid_sharer`,`GHR_crsnr`, `GHR_sysimport`,`GHR_ChildAges`,`GHR_Adults`,`GHR_updated`, `GHR_rateplan`,`GHR_typ`) 
                            Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """

                    qry_delete = """ DELETE from `V2I_GHR_Apaleo` where `GHR_reservationid` = %s """

                    cursor_target.execute(qry_delete, (params[1],))
                    connection_target.commit()
                    cursor_target.execute(qry_insert, params)
                    connection_target.commit()
                    #print(params[0])


#import_date = dt.date.today() - dt.timedelta(days=1)
import_date = dt.date.today()
Insert_API_Results(import_date)
import_date_str = import_date.strftime('%Y-%m-%d')
log_message("GHR - Inserted API results..")

def Preprocess_and_Update(column, mappedcolumn, mappingtable, keycolumn, keycolumnapaleo, datumimp, default_value):
    update_query = f"""
    UPDATE `V2I_GHR_Apaleo` AS v
    LEFT JOIN {mappingtable} AS m ON v.{keycolumn} = m.{keycolumnapaleo}
    SET v.{column} = COALESCE(m.{mappedcolumn}, {default_value})
    WHERE DATE(v.GHR_datumimp) = '{datumimp}'
    
    """
    cursor_target.execute(update_query, )





Preprocess_and_Update("GHR_leistacc", "Seq_ID", "V2D_res_num_mapping", "GHR_reservationid", "Apaleo_res_ID", import_date_str, "NULL")
log_message("GHR - GHR_leistacc updated ")
connection_target.commit()

Preprocess_and_Update("GHR_mpehotel", "PAS_Protel_ID", "V2D_Property_Attributes", "GHR_code2", "PAS_code2", import_date_str, "NULL" )

log_message("GHR - GHR_mpehotel updated ")
connection_target.commit()


Preprocess_and_Update("GHR_katnr", "PRC_katnr", "V2D_ProtelRoomCategories", "GHR_unit_group_code", "PRC_kat", import_date_str, 0)
log_message("GHR - GHR_katnr updated ")
connection_target.commit()

Preprocess_and_Update("GHR_market", "MK_Nr", "V2D_Apaleo_Market", "GHR_market_segment", "Apaleo_Mkt_code", import_date_str,"NULL")
log_message("GHR - GHR_market updated ")
connection_target.commit()

Preprocess_and_Update("GHR_sharenr", "GHR_leistacc", "V2I_GHR_Apaleo", "GHR_bookingid_sharer", "GHR_bookingid", import_date_str, "NULL")
log_message("GHR - GHR_sharenr updated ")
connection_target.commit()


def Update_roomnr(datumimp):
    update_query_ = f"""
        UPDATE `V2I_GHR_Apaleo` AS v
        LEFT JOIN `V2D_ApaleoRoomCategories` AS m ON v.GHR_unit_group_code = m.Unit_group_code
        SET v.GHR_zimmer = 
        CASE 
        WHEN m.Types = "bedroom"  THEN 1
        ELSE 0 
        END
        WHERE  DATE(v.GHR_datumimp) = '{datumimp}'
        """

    cursor_target.execute(update_query_ , )

    connection_target.commit()


Update_roomnr(import_date_str)

log_message("GHR - GHR_zimmer  updated ")
connection_target.commit()
