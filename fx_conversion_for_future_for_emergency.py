#copy paste the content in mysql workbench or heidisql you can change the dates accordingly

'''
UPDATE
	FMT_Reporting.V2I_GuestFutureDaily gfd

LEFT JOIN FMT_Reporting.V2D_Property_Attributes pas
	ON gfd.GFD_mpehotel = pas.PAS_Protel_ID

LEFT JOIN FMT_Reporting.V2I_FXRates fx
	ON YEAR(gfd.GFD_datum) = fx.FX_year
	AND MONTH(gfd.GFD_datum) = fx.FX_month
	AND pas.PAS_cur3 = FX_cur3

SET
	gfd.GFD_n_logis_EUR = gfd.GFD_n_logis / fx.FX_avg,
	gfd.GFD_n_fb_EUR = gfd.GFD_n_fb / fx.FX_avg,
	gfd.GFD_n_other_EUR = gfd.GFD_n_other / fx.FX_avg

WHERE gfd.GFD_n_logis_EUR IS NULL AND gfd.GFD_sysimport >= "guestfuturedailyapaleo_2025-01-01";



UPDATE
	FMT_Reporting.V2I_GuestFutureDaily gfd

LEFT JOIN FMT_Reporting.V2D_Property_Attributes pas
	ON gfd.GFD_mpehotel = pas.PAS_Protel_ID

LEFT JOIN (
	SELECT *
	FROM FMT_Reporting.V2I_FXRates
	WHERE FX_year = (SELECT MAX(FX_year) FROM FMT_Reporting.V2I_FXRates)
	AND FX_month = (SELECT MAX(FX_month) FROM FMT_Reporting.V2I_FXRates)
) lfx

	ON pas.PAS_cur3 = lfx.FX_cur3

SET
	gfd.GFD_n_logis_EUR = gfd.GFD_n_logis / lfx.FX_avg,
	gfd.GFD_n_fb_EUR = gfd.GFD_n_fb / lfx.FX_avg,
	gfd.GFD_n_other_EUR = gfd.GFD_n_other / lfx.FX_avg

WHERE gfd.GFD_n_logis_EUR IS NULL AND  gfd.GFD_sysimport >= "guestfuturedailyapaleo_2025-01-01";

'''