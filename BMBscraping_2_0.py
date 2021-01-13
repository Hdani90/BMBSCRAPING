import pandas as pd
import requests
import json
import time

class data_columns:
    emisionDate = "emisionDate"
    emisionTime = "emisionTime"
    change = "change"
    date = "date"
    executionMonthOrder = "executionMonthOrder"
    strikePrice = "strikePrice"
    term = "term"
    hasInformation = "hasInformation"
    isOption = "isOption"
    ratio = "ratio"
    Cbid = "Cbid"
    bid = "bid"
    ask = "ask"
    Cask = "Cask"
    apertura = "open"
    minimo = "min"
    maximo = "max"
    price = "price"
    totalNominalValue = "totalNominalValue"
    totalAmount = "totalAmount"
    operations = "operations"
    close = "close"
    lastPrice = "lastPrice"
    variation = "variation"
    trend = "trend"
    setlementPrice = "setlementPrice"
    adjacentPrice = "adjacentPrice"
    openInterest = "openInterest"
    bondData = "bondData"
    rofexExpirationDate = "rofexExpirationDate"
    mayorista = "mayorista"
    month = "month"
    monthNumber = "monthNumber"
    year = "year"
    baseTicker = "baseTicker"
    categoryOrder = "categoryOrder"
    category = "category"
    cfiCode = "cfiCode"
    stockBaseTicker = "stockBaseTicker"
    entityType = "entityType"
    quoteTypes = "quoteTypes"
    last = "last"
    mark = "mark"
    key = "key"
    high = "high"
    low = "low"
    volume = "volume"
    changePc = "changePc"
    week52Low = "week52Low"
    week52High = "week52High"
    totalDailyVolume = "totalDailyVolume"
    askSize = "askSize"
    bidSize = "bidSize"
    previousClose = "previousClose"
    isHalted = "isHalted"

class scraping:
    
    def __init__(self):
        self.__url = "https://www.bullmarketbrokers.com/Information/StockPrice/GetStockPrices"
        self.__url_internacional = "https://www.bullmarketbrokers.com/Information/StockPrice/GetStockPricesUS"
        self.__url_login = "https://www.bullmarketbrokers.com/Home/Login"
        self.__url_precios = "https://www.bullmarketbrokers.com/Operations/Orders/InitializeStockPrice"
    
    def login(self, email, password):
        # La función login requiere el email y password para configurar la cookie y el token de acceso
        
        # email: tipo de dato string
        # password: tipo de dato string
        
        # Configuración ip
        ip = requests.get("https://api.ipify.org/?format=json")
        ip = ip.json()["ip"]
        
        # Configuración de los parametros para realizar el login
        data_login = {
        "idNumber": str(email),
        "password": str(password),
        "loginData": {"ip": str(ip)}
        }
        
        # Conección para obtener el token y la cookie 
        r = requests.post(self.__url_login, data = data_login)
#         token = json.loads(r.text)
        
        # Buscamos la cookie que se requiere para obtener los datos
        for c in r.cookies:
            self.__cookie = c.name + "=" + c.value
            if "UserId" in self.__cookie:
                self.__headers = {"cookie": self.__cookie}
                print("Logueado correctamente")
                break
        else:
            print("No se pudo loguear correctamente, compruebe que los datos ingresados son correctos")
    

    def cotizaciones(self, mercado, term=3):
        # Obtenemos las cotizaciones de los diferentes paneles
        
        # mercado: tipo de dato string, valores posibles:
                # "merval" para el panel Merval
                # "general" para el panel Panel General
                # "opciones" para el panel Opciones
                # "bonos" para el panel de Bonos
                # "letras" para el panel de letras del tesoro
                # "cedears" para el panel de Cedears
                # "futuros" para el panel de futuros de rofex
                # "internacional" para el panel de NYSE
                
        # term: tipo de dato strin, los valores van de:
                # "1" para Contado inmediato
                # "2" para 24 hs
                # "3" para 48 hs
        
        # Configuramos los datos para realizar el request
        data = {
                "_ts": "1609169689025",
                "term": str(term),
                "index": "",
                "sortColumn": "ticker",
                "isAscending": "true"
            }
        
        # Modifica los datos dependiendo el mercado
        url = self.__url
        market = {"merval": "merval",
                 "general": "panel general",
                 "letras": "lebacsP",
                 "opciones": "opciones",
                 "bonos": "bonos",
                 "cedears": "cedears",
                 "futuros": "rofex",
                 "cauciones": "cauciones",
                 "internacional": "internacional"}
        data["index"] = market[mercado]

        if mercado == "cauciones": data["term"] = ""
        if mercado == "opciones": data["term"] = "2"
        if mercado == "internacional":
            data = {
                "_ts": "1609169689025",
                "sortColumn": "ticker",
                "isAscending": "true"
            }
            url = self.__url_internacional
        
        # Se realiza el request para obtener los datos
        datos = self.__get_data(url, self.__headers, data)
        datos = datos["result"]
        
        return datos
        
    def ticker(self, tickers):
        # Obtenemos la cotizacion por una lista de ticker
        
        # tickers: tipo de dato: lista de tuplas, valores posibles cualquier ticker de cualquier panel de cotizaciones
        # panel: merval, general, bonos, cedears, opciones, futuros y cauciones
        # el formato del dato es el siguien [("GGAL", 3)]
        # para los futuros y las cauciones el term va vacio es decir con comillas vacias ""
        # por ejemplo [("PESOS-0302-U-CT-ARS", "")]
        
        datos = []
        url = self.__url_precios
        for ticker, term in tickers:
            data = {
                "symbol": str(ticker).upper(),
                "term": str(term)
            }
            
            data = self.__get_data(url, self.__headers, data)
            
            if "indexes" in data.keys():
                datos.append(data)
        
        return datos
    
    def formatear_datos(self, datos, columnas = None, tickers = None, level_2 = False):
        # Formateamos los datos
        # El tipo de dato de entrada es un json, el cual se formatea para retornar un dataframe
        # Tambien se puede obtener la profundidad del mercado
        # Esta funcion sirve tanto para los datos traidos atravez de la funcion cotizaciones 
        # como de la funcion ticker
        
        # datos: tipo de dato: json
        # level_2: tipo de dato booleano, por defecto esta en False, pero si queremos traer los datos
        # de la profundidad del mercado podemos ponerlo en True, retorna 3 dataframes: datos, bid, ask
        # tickers: tipo de dato: lista, los valores posibles son los tickers que tenga el panel de cotizaciones
        # columnas: tipo de dato: lista, los valores posibles son todos los valores de la clase data_columns

        self.__datos = pd.DataFrame(datos)

        # Preguntamos si existe la columna stockOffer para obtener sus datos y asignarlos al dataframe
        if "stockOffer" in self.__datos:
            if not self.__datos["stockOffer"].apply(pd.Series)["bidTop"].apply(pd.Series).empty:
                bidtop = self.__datos["stockOffer"].apply(pd.Series)["bidTop"].apply(pd.Series)[0].apply(pd.Series)
                bidtop.rename(columns={"quantity":"Cbid", "price":"bid"}, inplace=True)
                self.__datos[["Cbid", "bid"]] = bidtop[["Cbid", "bid"]]
                
            if not self.__datos["stockOffer"].apply(pd.Series)["askTop"].apply(pd.Series).empty:
                asktop = self.__datos["stockOffer"].apply(pd.Series)["askTop"].apply(pd.Series)[0].apply(pd.Series)
                asktop.rename(columns={"quantity":"Cask", "price":"ask"}, inplace=True)
                self.__datos[["ask", "Cask"]] = asktop[["ask", "Cask"]]

        # Preguntamos si existe la columna stockState para obtener sus datos y asignarlos al dataframe
        if "stockState" in self.__datos:
            estado = self.__datos["stockState"].apply(pd.Series)
            self.__datos = pd.concat([self.__datos, estado], axis=1)
        
        # Preguntamos si existe la columna rofexData para obtener sus datos y asignarlos al dataframe
        if "rofexData" in self.__datos:
            rofex = self.__datos["rofexData"].apply(pd.Series)
            self.__datos = pd.concat([self.__datos, rofex], axis=1)
            
        # Preguntamos si level_2 es true para obtener la profundidad del mercado y retornar los dataframes
        if level_2:
            bidtop = self.__datos["stockOffer"].apply(pd.Series)["bidTop"].apply(pd.Series)
            asktop = self.__datos["stockOffer"].apply(pd.Series)["askTop"].apply(pd.Series)
            
            self.__config_columns(self.__datos, columnas)
            self.__filter(self.__datos, tickers)
            
            return self.__datos, bidtop, asktop
        
        # Eliminamos las columnas repetidas y asignamos el ticker como index
        self.__config_columns(self.__datos, columnas)
        self.__filter(self.__datos, tickers)

        return self.__datos
    
    def __get_data(self, url, headers, params):
        r = requests.get(url = url, headers = headers, params = params)
        data = json.loads(r.text)
        return data
    def __config_columns(self, datos, columnas):
        if "stockOffer" in datos: del(datos["stockOffer"])
        if "stockState" in datos: del(datos["stockState"])
        if "rofexData" in datos: del(datos["rofexData"])
        if "indexes" in datos: del(datos["indexes"])
        if "ticker" in datos: datos.set_index("ticker", inplace=True)
        if columnas is not None: self.__datos = datos[columnas]
    def __filter(self, datos, tickers):
        if tickers is not None: self.__datos = datos[datos.index.isin(tickers)]