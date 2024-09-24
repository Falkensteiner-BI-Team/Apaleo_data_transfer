INSERT INTO V2I_GFD_Apaleo_test(GFD_reservationid, GFD_leistacc,GFD_mpehotel,GFD_datumimp, GFD_datum,GFD_zimmernr,GFD_roomnights,GFD_resstatus,GFD_typ,GFD_anzerw,GFD_anzkin1, GFD_anzkin2,GFD_anzkin3,GFD_anzkin4,GFD_kbett,GFD_n_logis, GFD_n_fb,  GFD_n_other,GFD_sysimport)

WITH DetailedGFR AS (
    SELECT 
		main.GFR_reservationid,
        main.GFR_leistacc,
        main.GFR_mpehotel,
        main.GFR_Adults,
        main.GFR_datumimp,
        DATE_ADD(main.GFR_datumvon, INTERVAL Numbers.number DAY) AS GFD_datum,
        main.GFR_zimmernr,
       CASE
            WHEN DATE_ADD(main.GFR_datumvon, INTERVAL Numbers.number DAY) = main.GFR_datumbis THEN 0
            ELSE 1 END AS GFD_roomnights,
        CASE
            WHEN DATE_ADD(main.GFR_datumvon, INTERVAL Numbers.number DAY) = main.GFR_datumvon THEN 1
            WHEN DATE_ADD(main.GFR_datumvon, INTERVAL Numbers.number DAY) = main.GFR_datumbis THEN 3
            ELSE 2
        END AS GFD_resstatus,
        
        main.GFR_datumvon,
        main.GFR_datumbis
    FROM 
        V2I_GFR_Apaleo AS main
    JOIN 
        Numbers ON Numbers.number <= DATEDIFF(main.GFR_datumbis, main.GFR_datumvon)
    WHERE 
         main.GFR_datumimp >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
),

AggregatedChildGroups AS (
    SELECT
        -- g.GHR_leistacc,
        g.GFR_reservationid,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin1' THEN 1 ELSE 0 END) AS GFD_anzkin1,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin2' THEN 1 ELSE 0 END) AS GFD_anzkin2,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin3' THEN 1 ELSE 0 END) AS GFD_anzkin3,
        MAX(CASE WHEN c.CG_Group = 'GHD_anzkin4' THEN 1 ELSE 0 END) AS GFD_anzkin4,
        MAX(CASE WHEN c.CG_Group = 'GHD_kbett' THEN 1 ELSE 0 END) AS GFD_kbett
    FROM
        V2I_GFR_Apaleo g
    LEFT JOIN
        V2I_ChildGroups c ON g.GFR_reservationid = c.CG_reservationid
    GROUP BY
        g.GFR_reservationid
), 

Folios AS(
SELECT 
    revenue.id,
    revenue.FA_date,
    CAST(SUM(revenue.total_n_amount) AS DECIMAL(18,8)) AS total_n_amount,
    CAST(SUM(revenue.total_g_amount) AS DECIMAL(18,8))AS total_g_amount,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'Logis' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS logis,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 = 'F&B' THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS fb,
    CAST(SUM(CASE WHEN t.TAA_GroupOrd1 NOT IN ('Logis', 'F&B') OR t.TAA_GroupOrd1 IS NULL THEN revenue.total_n_amount ELSE 0 END) AS DECIMAL(18,8)) AS Other
FROM (
    SELECT
        CONCAT(r.FA_property, '-', r.FA_reservationid) AS id,
        r.FA_date,
        r.FA_taacode, 
        CAST(SUM(r.FA_n_amount) AS DECIMAL(18,8)) AS total_n_amount,
        CAST(SUM(r.FA_g_amount) AS DECIMAL(18,8)) AS total_g_amount
    FROM 
        V2I_Folios_Apaleo r
    WHERE 
        r.FA_taacode IS NOT NULL
    GROUP BY 
        r.FA_property, r.FA_reservationid, r.FA_taacode, r.FA_date
) revenue
LEFT OUTER JOIN V2D_TAA t ON revenue.FA_taacode = t.TAA_TAA
GROUP BY
    revenue.id, revenue.FA_date
)



SELECT
    d.GFR_reservationid,
    d.GFR_leistacc,
    d.GFR_mpehotel,
    d.GFR_datumimp,
    d.GFD_datum,
    d.GFR_zimmernr,
    d.GFD_roomnights,
    d.GFD_resstatus,
    0,
   -- d.GFR_datumvon,
   -- d.GFR_datumbis,
    d.GFR_Adults,
    a.GFD_anzkin1,
    a.GFD_anzkin2,
    a.GFD_anzkin3,
    a.GFD_anzkin4,
    a.GFD_kbett,
    COALESCE(f.logis, 0) AS logis,
    COALESCE(f.fb, 0) AS fb,
    COALESCE(f.other, 0) AS other,
    CONCAT('guestfuturedailyapaleo_', d.GFR_datumimp)
FROM
    DetailedGFR d
LEFT JOIN
    AggregatedChildGroups a ON d.GFR_reservationid = a.GFR_reservationid
LEFT JOIN
	Folios f ON d.GFR_reservationid = f.id and d.GFD_datum = f.FA_date
WHERE d.GFD_datum >= d.GFR_datumimp
