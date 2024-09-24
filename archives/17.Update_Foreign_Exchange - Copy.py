import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt


# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()



update_GuestHistoryDaily_Detailed = """
UPDATE
	FMT_Reporting.V2I_GuestHistoryDaily_Detailed ghd

LEFT JOIN FMT_Reporting.V2D_Property_Attributes pas
	ON ghd.GHD_mpehotel = pas.PAS_Protel_ID

LEFT JOIN FMT_Reporting.V2I_FXRates fx
	ON YEAR(ghd.GHD_datum) = fx.FX_year 
    AND MONTH(ghd.GHD_datum) = fx.FX_month 
    AND pas.PAS_cur3 = fx.FX_cur3

SET
	ghd.GHD_n_EUR = ghd.GHD_n_ / fx.FX_avg,
    ghd.GHD_g_EUR = ghd.GHD_g_ / fx.FX_avg
    
WHERE ghd.GHD_sysimport LIKE 'guesthistorydailyapaleo_%'; """

update_GuestHistoryDaily = """

UPDATE
	FMT_Reporting.V2I_GuestHistoryDaily ghd

LEFT JOIN FMT_Reporting.V2D_Property_Attributes pas
	ON ghd.GHD_mpehotel = pas.PAS_Protel_ID

LEFT JOIN FMT_Reporting.V2I_FXRates fx
	ON YEAR(ghd.GHD_datum) = fx.FX_year 
    AND MONTH(ghd.GHD_datum) = fx.FX_month 
    AND pas.PAS_cur3 = fx.FX_cur3

SET
	ghd.GHD_n_logis_EUR = ghd.GHD_n_logis / fx.FX_avg,
    ghd.GHD_n_fb_EUR = ghd.GHD_n_fb / fx.FX_avg,
    ghd.GHD_n_bqt_EUR = ghd.GHD_n_bqt / fx.FX_avg,
    ghd.GHD_n_spa_EUR = ghd.GHD_n_spa / fx.FX_avg,
    ghd.GHD_n_ski_EUR = ghd.GHD_n_ski / fx.FX_avg,
    ghd.GHD_n_other_EUR = ghd.GHD_n_other / fx.FX_avg
    

WHERE ghd.GHD_sysimport LIKE 'guesthistorydailyapaleo_%'; """

cursor_target.execute(update_GuestHistoryDaily_Detailed, )
connection_target.commit()

cursor_target.execute(update_GuestHistoryDaily,)
connection_target.commit()


