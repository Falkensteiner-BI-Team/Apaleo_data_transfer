import mysql.connector
import pandas as pd

import userdata


# FMT_Reporting MySQL Server
connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                            password=userdata.mysql_password())

cursor_target = connection_target.cursor()


select_qry = """SELECT * 
FROM V2I_GFD_Apaleo
WHERE GFD_sysimport = (SELECT MAX(GFD_sysimport) FROM V2I_GFD_Apaleo)"""


data = pd.read_sql(select_qry, connection_target)

# Step 5: Export DataFrame to Excel
output_file = '../output.xlsx'
data.to_excel(output_file, index=False)

# Close the database connection
connection_target.close()

print(f"Data successfully exported to {output_file}")