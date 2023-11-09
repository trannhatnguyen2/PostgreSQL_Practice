import pyodbc

# conn = pyodbc.connect('SERVER=LAPTOP-80UMU1UQ\\NHATNGUYEN;DATABASE=BI_DW_BoKho_K20406C;TRUSTED_CONNECTION=True;PORT=1433;DRIVER={SQL Server}')

conn = pyodbc.connect('SERVER=LAPTOP-80UMU1UQ\\NHATNGUYEN;DATABASE=BI_DW_BoKho_K20406C;TRUSTED_CONNECTION=True;PORT=1433;DRIVER={SQL Server};USER ID=trannhatnguyen2;PASSWORD=nhatnguyen18')
cursor = conn.cursor()
cursor.execute('SELECT * FROM DW.DimChannel')
rows = cursor.fetchall()
print(rows)
cursor.close()
conn.close()