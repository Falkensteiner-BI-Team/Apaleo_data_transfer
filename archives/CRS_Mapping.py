import userdata
import mysql.connector
from APIClient import *
import MySQLdb

# FMT_Reporting MySQL Server

def get_max_reservationid_local():
    connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                                password=userdata.mysql_password())
    cursor_target = connection_target.cursor()
    select_query = "SELECT MAX(Seq_ID) from V2D_crs_mapping_Jotform"
    cursor_target.execute(select_query, )

    return cursor_target.fetchall()

# test print(get_max_reservationid_local()[0][0])  240522: max num is 51002317

def get_recent_reservationid_cloud(maxid):
  connection_target_azure = MySQLdb.connect(
      host=userdata.azure_host(),
      database=userdata.azure_database(),
      user=userdata.azure_user(),
      password=userdata.azure_password(),
      charset='utf8',  # Optional: Specify character encoding
      ssl={'ca': 'DigiCertGlobalRootCA.crt.pem'}  # Path to CA certificate
  )

  cursor_target_azure = connection_target_azure.cursor()
  select_query = "select * from `fmtg-api-middleware`.resid_numid_mapping where Seq_ID > %s and CRS_ID is not null "

  cursor_target_azure.execute(select_query, (maxid,))

  # Fetch all rows from the executed query
  rows = cursor_target_azure.fetchall()

  # Close the connection
  connection_target_azure.close()

  return rows

#test print(get_recent_reservationid_cloud(51002312))

def insert_reservationid_local(max_res_num):
    # FMT_Reporting MySQL Server
    connection_target = mysql.connector.connect(host='DB-FMT06', database='FMT_Reporting', user=userdata.mysql_user(),
                                                password=userdata.mysql_password())
    cursor_target = connection_target.cursor()

    insert_query = "INSERT INTO V2D_res_num_mapping(Seq_ID, Apaleo_res_ID,CRS_ID) Values (%s,%s,%s)"


    delete_query = "DELETE FROM V2D_res_num_mapping WHERE Seq_ID=%s AND Apaleo_res_ID =%s "


    print(max_res_num)
    for row in get_recent_reservationid_cloud(max_res_num):
        cursor_target.execute(delete_query, (row[0], row[1],))  # handle duplicates
        cursor_target.execute(insert_query, (row[0], row[1],row[3],))

        connection_target.commit()



#insert_reservationid_local(1) # when uploading in an empty table
#insert_reservationid_local(get_max_reservationid_local()[0][0])

insert_reservationid_local(0)
















