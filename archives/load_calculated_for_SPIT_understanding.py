import pyodbc
import userdata
import mysql.connector
from datetime import datetime as dt, timedelta, date
from Python_log import *

S = """V2I_GuestFutureReservation"""

FUTURE_RESERVATION = """FMT_Reporting.V2I_GuestFutureReservation"""
start = time.time()
log_counter = 0

# connection_origin = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.user(), password=userdata.password())
# cursor_origin = connection_origin.cursor()

connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.user(),
                                            password=userdata.password())
cursor_target = connection_target.cursor()


####################################################################################################
# start Script

def load_calculated_for_SPIT_date(imp_date):
    global log_counter

    # res_date = dt.strptime(imp_date,"%Y-%m-%d")+ timedelta(-1)
    # res_date = res_date.strftime("%Y-%m-%d")

    # Query for inserting to target Database
    qry_getresdate = """ SELECT
	IF ((SELECT SUM(GFD_n_logis_EUR) FROM FMT_Reporting.V2I_GuestFutureDaily WHERE GFD_datumimp = '{imp_date}') IS NULL,
			NULL,
			DATE_SUB('{imp_date}', INTERVAL 1 DAY));
		"""

    qry_delete = """DELETE FROM V2C_CalculatedSPITData
		WHERE '{res_date}' IS NOT NULL 
		AND CSD_res_date = '{res_date}';
		"""

    qry_insert = """
		INSERT INTO V2C_CalculatedSPITData(CSD_res_date, CSD_rev_year, CSD_rev_month, CSD_market, CSD_mpehotel, CSD_reschar, CSD_past_otb, CSD_rev_room, CSD_rev_fnb, CSD_rev_Other, CSD_room_nights, CSD_reservations,CSD_all_guest_nights)

        SELECT 
            '{res_date}' AS CSD_res_date,
            rev.Rev_Year AS CSD_rev_year, 
            rev.Rev_Month AS CSD_rev_month, 
            rev.GHR_market AS CSD_market,
            rev.GHD_mpehotel AS CSD_mpehotel,
            rev.GHR_reschar AS CSD_res_char,
            'past' AS CSD_past_otb,
            rev.Room AS CSD_rev_room,
            rev.FnB AS CSD_rev_fnb,
            rev.Other AS CSD_rev_Other,
            res.Room_nights AS CSD_room_nights,
            res.Reservations AS CSD_reservations,
            res.All_guest_nights AS CSD_all_guest_nights

        FROM
            (
            SELECT 
                -- '{res_date}' AS Reservation_date,
                Year(ghd.GHD_datum) AS Rev_Year, 
                Month(ghd.GHD_datum) AS Rev_Month, 
                ghr.GHR_market,
                ghd.GHD_mpehotel,
                ghr.GHR_reschar,
                SUM(ghd.GHD_n_logis_EUR) AS Room,
                SUM(ghd.GHD_n_fnb_EUR) AS FnB,
                SUM(ghd.GHD_n_other_EUR) AS Other

            FROM FMT_Reporting.V2I_GuestHistoryReservation ghr

            INNER JOIN 
                (SELECT 
                    GHD_mpehotel,
                    GHD_datum,
                    GHD_leistacc,
                    GHD_n_logis_EUR,
                    GHD_n_fb_EUR + GHD_n_bqt_EUR AS GHD_n_fnb_EUR,
                    GHD_n_spa_EUR + GHD_n_ski_EUR + GHD_n_other_EUR AS GHD_n_other_EUR

                FROM FMT_Reporting.V2I_GuestHistoryDaily 
                WHERE GHD_datum <= '{res_date}'
                    AND YEAR(GHD_datum) >= YEAR('{res_date}') - 4
                ) ghd


            ON ghd.GHD_leistacc = ghr.GHR_leistacc

            WHERE ghr.GHR_reschar NOT IN (2,3)
                AND ghr.GHR_datumres <= '{res_date}'

            GROUP BY ghr.GHR_market, ghd.GHD_mpehotel, Year(ghd.GHD_datum), Month(ghd.GHD_datum), ghr.GHR_reschar
            ) rev

        LEFT JOIN

            (
            SELECT 
                -- '{res_date}' AS Reservation_date,
                Year(ghd.GHD_datum) AS Rev_Year, 
                Month(ghd.GHD_datum)AS Rev_Month, 
                ghr.GHR_market,
                ghd.GHD_mpehotel, 
                ghr.GHR_reschar,
                SUM(ghd.GHD_roomnights) AS Room_nights,
                COUNT(DISTINCT IF(GHD_resstatus = 1, ghr.GHR_leistacc, NULL)) AS Reservations,
                SUM(IFNULL(ghd.GHD_anzerw,0) + IFNULL(ghd.GHD_anzkin1,0) + IFNULL(ghd.GHD_anzkin2,0) + IFNULL(ghd.GHD_anzkin3,0) + IFNULL(ghd.GHD_anzkin4,0)) AS All_guest_nights

            FROM FMT_Reporting.V2I_GuestHistoryReservation ghr  


            INNER JOIN 
                (SELECT 
                    GHD_mpehotel,
                    GHD_datum,
                    GHD_leistacc,
                    GHD_resstatus,
                    GHD_roomnights,
                    GHD_anzerw,
                    GHD_anzkin1,
                    GHD_anzkin2,
                    GHD_anzkin3,
                    GHD_anzkin4
                FROM FMT_Reporting.V2I_GuestHistoryDaily
                WHERE GHD_resstatus NOT IN (3,-1) AND GHD_typ  <> 4
                    AND GHD_datum <= '{res_date}'
                    AND YEAR(GHD_datum) >= YEAR('{res_date}') - 4
                ) ghd

            ON ghd.GHD_leistacc = ghr.GHR_leistacc

            WHERE ghr.GHR_reschar NOT IN (2,3) 
                AND ghr.GHR_datumcxl = '1900-01-01'
                AND ghr.GHR_zimmer = 1
                AND ghr.GHR_datumres <= '{res_date}'        

            GROUP BY ghr.GHR_market, ghd.GHD_mpehotel, Year(ghd.GHD_datum), Month(ghd.GHD_datum), ghr.GHR_reschar
            ) res

        ON 
            -- rev.Reservation_date = res.Reservation_date
        --     AND 
            rev.Rev_Year = res.Rev_Year
            AND rev.Rev_Month = res.Rev_Month
            AND rev.GHR_market = res.GHR_market
            AND rev.GHD_mpehotel = res.GHD_mpehotel
            AND rev.GHR_reschar = res.GHR_reschar

        UNION

        SELECT 
            '{res_date}' AS CSD_res_date,
            future_rev.Rev_Year AS CSD_rev_year, 
            future_rev.Rev_Month AS CSD_rev_month, 
            future_rev.GFR_market AS CSD_market,
            future_rev.GFD_mpehotel AS CSD_mpehotel,
            future_rev.GFR_reschar AS CSD_reschar,
            'otb' AS CSD_past_otb,
            future_rev.Future_Room AS CSD_rev_room,
            future_rev.Future_FnB AS CSD_rev_fnb,
            future_rev.Future_Other AS CSD_rev_other,
            future_res.Future_Room_nights AS CSD_room_nights,
            future_res.Future_Reservations AS CSD_reservations,
            future_res.Future_All_guest_nights AS CSD_Future_all_guest_nights

        FROM

            (
            SELECT 
                -- '{res_date}' AS Reservation_date,
                Year(gfd.GFD_datum) AS Rev_Year, 
                Month(gfd.GFD_datum)AS Rev_Month, 
                gfr.GFR_market,
                gfd.GFD_mpehotel,
                gfr.GFR_reschar,
                SUM(gfd.GFD_n_logis_EUR) AS Future_Room,
                SUM(gfd.GFD_n_fb_EUR) AS Future_FnB,
                SUM(gfd.GFD_n_other_EUR) AS Future_Other

            FROM %s gfr

            INNER JOIN 
                (SELECT 
                    GFD_mpehotel,
                    GFD_datum,
                    GFD_datumimp,
                    GFD_leistacc,
                    GFD_n_logis_EUR,
                    GFD_n_fb_EUR,
                    GFD_n_other_EUR
                FROM FMT_Reporting.V2I_GuestFutureDaily 
                WHERE GFD_datumimp = DATE_ADD('{res_date}', INTERVAL 1 DAY)
                    AND YEAR(GFD_datum) <= YEAR('{res_date}') + 2
                ) gfd


            ON gfd.GFD_leistacc = gfr.GFR_leistacc
                AND gfd.GFD_datumimp = gfr.GFR_datumimp

            WHERE gfr.GFR_reschar NOT IN (2,3)
                AND gfr.GFR_datumimp = DATE_ADD('{res_date}', INTERVAL 1 DAY)
                AND gfr.GFR_datumres <= '{res_date}'

            GROUP BY gfr.GFR_market, gfd.GFD_mpehotel, Year(gfd.GFD_datum), Month(gfd.GFD_datum), gfr.GFR_reschar
            ) future_rev

        LEFT JOIN

            (
            SELECT 
                -- '{res_date}' AS Reservation_date,
                Year(gfd.GFD_datum) AS Rev_Year, 
                Month(gfd.GFD_datum)AS Rev_Month, 
                gfr.GFR_market,
                gfd.GFD_mpehotel, 
                gfr.GFR_reschar,
                SUM(gfd.GFD_roomnights) AS Future_Room_nights,
                COUNT(DISTINCT IF(GFD_resstatus = 1, gfr.GFR_leistacc, NULL)) AS Future_Reservations,
                SUM(IFNULL(gfd.GFD_anzerw,0) + IFNULL(gfd.GFD_anzkin1,0) + IFNULL(gfd.GFD_anzkin2,0) + IFNULL(gfd.GFD_anzkin3,0) + IFNULL(gfd.GFD_anzkin4,0)) AS Future_All_guest_nights

            FROM FMT_Reporting.%s gfr  


            INNER JOIN 
                (SELECT 
                    GFD_mpehotel,
                    GFD_datum,
                    GFD_datumimp,
                    GFD_leistacc,
                    GFD_resstatus,
                    GFD_roomnights,
                    GFD_anzerw,
                    GFD_anzkin1,
                    GFD_anzkin2,
                    GFD_anzkin3,
                    GFD_anzkin4
                FROM FMT_Reporting.V2I_GuestFutureDaily
                WHERE GFD_resstatus NOT IN (3,-1) AND GFD_typ  <> 4
                    AND GFD_datumimp = DATE_ADD('{res_date}', INTERVAL 1 DAY)
                    AND YEAR(GFD_datum) <= YEAR('{res_date}') + 2
                ) gfd

            ON gfd.GFD_leistacc = gfr.GFR_leistacc
                AND gfd.GFD_datumimp = gfr.GFR_datumimp

            WHERE gfr.GFR_reschar NOT IN (2,3) 
                AND gfr.GFR_datumcxl = '1900-01-01'
                AND gfr.GFR_zimmer = 1
                AND gfr.GFR_datumimp = DATE_ADD('{res_date}', INTERVAL 1 DAY)
                AND gfr.GFR_datumres <= '{res_date}'

            GROUP BY gfr.GFR_market, gfd.GFD_mpehotel, Year(gfd.GFD_datum), Month(gfd.GFD_datum), gfr.GFR_reschar
            ) future_res

        ON 
            -- future_rev.Reservation_date = future_res.Reservation_date
            -- AND 
            future_rev.Rev_Year = future_res.Rev_Year
            AND future_rev.Rev_Month = future_res.Rev_Month
            AND future_rev.GFR_market = future_res.GFR_market
            AND future_rev.GFD_mpehotel = future_res.GFD_mpehotel
            AND future_rev.GFR_reschar = future_res.GFR_reschar


		""" % (FUTURE_RESERVATION, S)

    cursor_target.execute(qry_getresdate.format(imp_date=imp_date))
    # connection_target.commit()
    res_date = cursor_target.fetchone()[0]
    if res_date == None:
        print('resdate: ', res_date)
    else:
        print('resdate: ', res_date)
        cursor_target.execute(qry_delete.format(res_date=res_date))
        connection_target.commit()

        cursor_target.execute(qry_insert.format(res_date=res_date))
        connection_target.commit()
        log_counter += 1


if __name__ == "__main__":

    ## daily loading
    today = dt.today()
    startdate = today + timedelta(-3)

    last_year = dt.today().replace(year=dt.today().year - 1)
    ly_startdate = startdate.replace(year=startdate.year - 1)

    lly_date = dt.today().replace(year=dt.today().year - 2)

    llly_date = dt.today().replace(year=dt.today().year - 3)
    ### load daterange

    print(today, startdate, last_year, ly_startdate)


    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(n)


    for single_date in daterange(startdate, today):
        print(single_date.strftime("%Y-%m-%d"))
        load_calculated_for_SPIT_date(single_date.strftime("%Y-%m-%d"))

    for single_date in daterange(ly_startdate, last_year):
        print(single_date.strftime("%Y-%m-%d"))
        load_calculated_for_SPIT_date(single_date.strftime("%Y-%m-%d"))

    print(lly_date.strftime("%Y-%m-%d"))
    load_calculated_for_SPIT_date(lly_date.strftime("%Y-%m-%d"))

    print(llly_date.strftime("%Y-%m-%d"))
    load_calculated_for_SPIT_date(llly_date.strftime("%Y-%m-%d"))

    # end Script
    ####################################################################################################
    # write to log

    log_to_mysql(start, log_counter, 'loads precalc data to V2C_CalculatedSPITData.')
