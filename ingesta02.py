import boto3
import mysql.connector
import pandas as pd

ficheroUpload = ""
try:
    connection = mysql.connector.connect(
        host='98.82.74.138',
        port='8080',
        database='tienda',
        user='root',
        password='utec',
    )

    print("Pasa")
    sql_select_Query = "select * from fabricantes"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    df = pd.DataFrame(records)
    ficheroUpload = 'ingesta_02.csv'
    df.to_csv(ficheroUpload, index=False)
    cursor.close()
    connection.close()

except Exception as error:
    print(f"Error de conexión a la base de datos: {error}")
    exit()

s3 = boto3.client('s3')
response = s3.upload_file(ficheroUpload, 's5-output', ficheroUpload)

print("Ingesta completada")
