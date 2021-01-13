import xlwings as xl
import time

import BMBscraping_2_0 as BM
from credenciales import credenciales as c


# Creamos los objetos necesacios para utilizar las funciones de scraping y configuracion de columnas
bmb = BM.scraping()
dc = BM.data_columns

# Login en nuestra cuenta
bmb.login(c.email, c.password)


# Asignamos los tickers que queremos traer con su respectivo termino
ggal = [
    ("GGAL", 1),
    ("GGAL", 2),
    ("GGAL", 3)
]
# Asignamos los tickers que queremos traer del panel de opciones
tickers = ["GFGC87.0FE", "GFGC96.0FE", "GFGC105.FE", "GFGC111.FE",
           "GFGC114.FE", "GFGC11881F", "GFGC123.FE", "GFGC12481F",
           "GFGC129.FE", "GFGC132.FE", "GFGC135.FE", "GFGC140.FE",
           "GFGC145.FE", "GFGC150.FE", "GFGC155.FE", "GFGC15881F",
           "GFGC16381F", "GFGC16881F", "GFGC175.FE", "GFGC180.FE",
           "GFGC190.FE", "GFGC195.FE", "GFGC19881F", "GFGV82808F",
           "GFGV87.0FE", "GFGV97808F", "GFGV105.FE", "GFGV111.FE",
           "GFGV114.FE", "GFGV11881F", "GFGV123.FE", "GFGV12481F",
           "GFGV129.FE", "GFGV132.FE", "GFGV135.FE"]
# Configuramos las columnas que queremos pasar al excel
columnas = [dc.Cbid, dc.bid, dc.ask, dc.Cask,
            dc.price, dc.variation, dc.apertura,
            dc.maximo, dc.minimo, dc.close,
            dc.totalAmount, dc.totalNominalValue]


# Configuracion de la planilla y la hoja
wb = xl.Book("TEST.xlsb")
hoja = wb.sheets('Bolsuite')
while True:
    try:
        # Actualizamos datos
        opciones = bmb.cotizaciones("opciones")      # Funcion para traer el panel de opciones
        ggal_data = bmb.ticker(ggal)                 # Funcion para traer los datos de galicia
        
        # Formateamos los datos
        opciones = bmb.formatear_datos(opciones, tickers=tickers, columnas=columnas).sort_index()
        ggal_data = bmb.formatear_datos(ggal_data, columnas=columnas)
        
        # Actualizamos el excel
        hoja.range("R2").options(index=True, header=False).value = ggal_data
        hoja.range("R5").options(index=True, header=False).value = opciones
        
        # Esperamos X cantidad de segundos para volver a actualizar
        time.sleep(5)
    except:
        # En caso de error imprime un mensaje
        print("Hubo un error y no se actualizaron los datos")



