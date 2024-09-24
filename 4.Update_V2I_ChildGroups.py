import userdata
import mysql.connector
from APIClient import *
from datetime import datetime, date, timedelta
import datetime as dt

# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())
cursor_target = connection_target.cursor()


insert_select_query = """
INSERT INTO V2I_ChildGroups (CG_reservationid,  CG_Age, CG_mpehotel, CG_Group)
SELECT 
     
    V.GHR_reservationid, 
    J.age AS GHR_ChildAge,
    V.GHR_mpehotel,
    G.GHD_columns AS CG_Group
FROM 
    V2I_GHR_Apaleo V
    JOIN JSON_TABLE(V.GHR_ChildAges, '$[*]' COLUMNS (age INT PATH '$')) J
    
    LEFT OUTER JOIN V2D_Childgrp G 
    ON J.age = G.Age AND V.GHR_mpehotel = G.mpehotel
    
    LEFT OUTER JOIN V2D_Childgrp GDefault 
    ON J.age = GDefault.Age AND GDefault.mpehotel IS NULL
 
"""


insert_select_query_future = """
INSERT INTO V2I_ChildGroups (CG_reservationid,  CG_Age, CG_mpehotel, CG_Group)
SELECT 
     
    V.GFR_reservationid, 
    J.age AS GFR_ChildAge,
    V.GFR_mpehotel,
    G.GFD_columns AS CG_Group
FROM 
    V2I_GFR_Apaleo V
    JOIN JSON_TABLE(V.GFR_ChildAges, '$[*]' COLUMNS (age INT PATH '$')) J
    
    LEFT OUTER JOIN V2D_Childgrp G 
    ON J.age = G.Age AND V.GFR_mpehotel = G.mpehotel
    
    LEFT OUTER JOIN V2D_Childgrp GDefault 
    ON J.age = GDefault.Age AND GDefault.mpehotel IS NULL"""

try:
    # Execute the query
    cursor_target.execute("delete from V2I_ChildGroups")

    cursor_target.execute(insert_select_query)
    connection_target.commit()
   # print(f"{cursor_target.rowcount} rows were inserted.")

    cursor_target.execute(insert_select_query_future)

    connection_target.commit()
    #print(f"{cursor_target.rowcount} rows were inserted.")

except mysql.connector.Error as err:
    # Rollback in case of an error
    connection_target.rollback()
    #print(f"Error: {err}")






