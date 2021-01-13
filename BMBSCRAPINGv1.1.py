#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import json
import xlwings as xl
import time
import pandas as pd
from datetime import datetime
from credenciales import credenciales

def ip_():
    
    ip = requests.get("https://api.ipify.org/?format=json")
    
    return ip.json()["ip"]

def token_cookie(email, password, ip): 
    
    data= {
        "idNumber": email,
        "password": password,
        "loginData": {"ip": ip}
    }
    r = requests.post("https://www.bullmarketbrokers.com/Home/Login", data = data)
    token = json.loads(r.text)
    for c in r.cookies:
        cookie = c.name + "=" + c.value
        if "UserId" in cookie:
            break
        
    return token, cookie

def actualizar(url, headers):
    
    r = requests.get(url=url, headers=headers)
    datos = json.loads(r.text)
    
    return datos


def formatear(datos, ticker, mes="", mes_alt=""):
    
    try:
        datos = pd.DataFrame(datos["result"])
    except:
        datos = pd.DataFrame([datos])
    
    datos = datos[datos["ticker"].str.contains(ticker)]
    datos = datos[datos["ticker"].str.endswith(str(mes)) | datos["ticker"].str.endswith(str(mes_alt))]
    datos = datos.set_index("ticker")
                  
    return datos


def precios(datos):
    
    try:
        ask = datos["stockOffer"].apply(pd.Series)["askTop"].apply(pd.Series)[0].apply(pd.Series).sort_values("ticker")
        bid = datos["stockOffer"].apply(pd.Series)["bidTop"].apply(pd.Series)[0].apply(pd.Series).sort_values("ticker")
    except:
        ask = pd.DataFrame({'price': [0], 'quantity': [0], 'position': [0]},
                 index={"GGAL"})
        bid = pd.DataFrame({'price': [0], 'quantity': [0], 'position': [0]},
                 index={"GGAL"})
    return bid.drop("position", axis=1).rename(columns={"quantity":"bidQuant"}).sort_index(), ask.drop("position", axis=1).rename(columns={"quantity":"askQuant"}).sort_index()


def historial(datos):

    historial = {}
    for i in ["open", "min", "max", "price", "totalNominalValue", "totalAmount", "operations", "close", "lastPrice", "variation"]:
        historial[i] = datos["stockState"].apply(pd.Series)[i]
        
    return pd.DataFrame(historial).sort_index()


def base(datos, cauciones=False):
    
    bases = {}

    if cauciones:
        datos["Base"] = datos.index
    else:
        datos["Base"] = datos["indexes"].apply(pd.Series)[0].apply(pd.Series).index
        
    datos["Base"] = datos["Base"].str.extract('(\d+)')
    datos["Base"] = pd.to_numeric(datos["Base"])
    base = datos["Base"].unique().astype(float)
    
    for i in range(len(base)):
        if base[i] > 40000:
            base[i] = base[i] / 1000
        elif (base[i] > 400) & (base[i] <= 40000):
            base[i] = base[i] / 100
    
    base.sort()
    bases["Base"] = base
    
    return bases["Base"]


# In[2]:


url_opc = "https://www.bullmarketbrokers.com/Information/StockPrice/GetStockPrices?_ts=1603837694602&term=3&index=opciones&sortColumn=ticker&isAscending=true"
url_spot = "https://www.bullmarketbrokers.com/Information/StockPrice/GetStockPrices?_ts=1604339328079&term=3&index=merval&sortColumn=ticker&isAscending=true"

url_ggal = ["https://www.bullmarketbrokers.com/Operations/Orders/InitializeStockPrice?symbol=GGAL&term=1",
           "https://www.bullmarketbrokers.com/Operations/Orders/InitializeStockPrice?symbol=GGAL&term=2",
           "https://www.bullmarketbrokers.com/Operations/Orders/InitializeStockPrice?symbol=GGAL&term=3"]

url_cauciones = "https://www.bullmarketbrokers.com/Information/StockPrice/GetStockPrices?_ts=1604951877560&term=&index=cauciones&sortColumn=ticker&isAscending=true"

ip = ip_()
token, cookie = token_cookie(credenciales.email, credenciales.password, ip)

cookie = {
    "cookie": cookie
}


# In[ ]:


wb = xl.Book("EPGBTuneadaFEB.xlsb")
hoja = wb.sheets('Bolsuite')
bases = wb.sheets("GGAL")

while True:
    try:
        datos_opc = actualizar(url_opc, cookie)
        datos_opc = formatear(datos_opc, "GFG", "F", "FE")
        bid_opc, ask_opc = precios(datos_opc)
        historico_opc = historial(datos_opc)
        hoja.range("R5").options(index=True, header=False).value = bid_opc[["bidQuant", "price"]]
        hoja.range("U5").options(index=False, header=False).value = ask_opc[["price", "askQuant"]]
        hoja.range("W5").options(index=False, header=False).value = historico_opc[["price", "variation", "open", "max", "min", "close", "totalAmount", "totalNominalValue"]]
        bases.range("AZ3").options(index=False, header=False).value = base(datos_opc).reshape(len(datos_opc["Base"].unique()),1)
        for i, spot in enumerate(url_ggal):
            datos_spot = actualizar(spot, cookie)
            datos_spot = formatear(datos_spot, "GGAL")
            bid_spot, ask_spot = precios(datos_spot)
            historico_spot = historial(datos_spot)
            hoja.range("R"+ str(2 + i)).options(index=True, header=False).value = bid_spot[["bidQuant", "price"]]
            hoja.range("U"+ str(2 + i)).options(index=False, header=False).value = ask_spot[["price", "askQuant"]]
            hoja.range("W"+ str(2 + i)).options(index=False, header=False).value = historico_spot[["price", "variation", "open", "max", "min", "close", "totalAmount", "totalNominalValue"]]
        time.sleep(5)
    except:
        break


# In[ ]:




