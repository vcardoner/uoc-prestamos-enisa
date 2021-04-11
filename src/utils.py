import datetime as dt
import requests
from random import randint
from time import sleep
import pandas as pd
import snscrape.modules.twitter as sntwitter
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent


def input_chromedriver():
    print("Para realizar scraping con Selenium es necesario introducir la ubicación de chromedriver.")
    print('Por defecto: D:\Program Files\chromedriver.exe')
    path = input('Introduzca la ruta de chromedriver: ')
    if path:
        print("Chromedriver parametrizado. \n")
    else:
        path = 'D:\Program Files\chromedriver.exe'
    return path


def input_linkedin():
    print("Proceso de scraping iniciado: " + str(dt.datetime.now()))
    print("Para realizar scraping en Linkedin es necesario introducir las credenciales.")
    print("(Esto es OPCIONAL, se realizará scraping en el resto de fuentes igualmente)")
    user = ''
    pwd = ''
    user = input('Introduzca usuario (email o teléfono) de Linkedin: ')
    if user:
        pwd = input('Introduzca pwd de Linkedin: ')
        if pwd: print("Credenciales almacenadas correctamente \n")
        else: print("No se realizará scraping en Linkedin \n")
    else: print("No se realizará scraping en Linkedin \n")
    return user, pwd


def user_agent_mod():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
        "dnt": "1",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/56.0.2924.87 Safari/537.36"
    }
    return headers


def enisa_scraper():
    # Definimos URL Base
    urlbase = 'https://www.enisa.es/es/comunidad-enisa/prestamos/searchresult?pag='

    i = 1
    # Iteramos para las primeras 10 páginas de resultado de búsqueda
    # por tanto los 120 préstamos más recientes

    # Creamos cabecera con user agent modificado para prevenir bloqueos
    h_mod = user_agent_mod()

    while i < 11:
        # Construimos la URL
        url = urlbase + str(i)
        # print(url)
        try:
            page = requests.get(url, headers=h_mod)
        except:
            print('Error al abrir fuente ENISA')
            return 0

        # Creamos el objeto bs para la URL
        soup = BeautifulSoup(page.content, from_encoding='utf-8', features="lxml")

        # Ponemos foco en el area que nos interesa
        t_row_header = soup.find_all('th')
        t_row_data = soup.find_all('td')

        # Condicion de salida
        if len(t_row_data) == 0: break

        header = []
        data = {}
        # En la primera iteración leemos cabecera y creamos dataframe
        if i == 1:
            for row in t_row_header:
                h = str.strip(row.p.text)
                header.append(h)
            df_enisa = pd.DataFrame(columns=header)

        # Empezamos a iterar para todas las filas con contenido de la página
        # Guardamos la info en el dataframe
        for row in t_row_data:
            col = str.strip(row.b.text)
            value = str.strip(row.div.find(text=True, recursive=False))
            data[col] = value
            if col == 'CC.AA.':
                df_enisa = df_enisa.append(data, ignore_index=True)
                data = {}
        i = i + 1

    # Retornamos DF y creamos el CSV con los datos
    df_enisa.to_csv('df_enisa.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')
    return df_enisa


def twitter_scraper(df_source):
    # Definimos entidades a buscar a partir de scraping de ENISA
    entity_list = df_source["Marca"].to_list()

    # Buscamos la fecha de hace 3 meses
    today = dt.date.today()
    fecha = today - dt.timedelta(days=90)

    # Creamos la lista para recopilar tweets
    tweets_list = []
    tweets_enisa_list = []

    for entity in entity_list:
        search = entity + " since:" + str(fecha)
        search_enisa = entity + " #clienteEnisa since:" + str(fecha)

        # Usamos TwitterSearchScraper para scrapear datos y guardar tweets en lista - Marca
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search).get_items()):
            if i > 500:
                break
            tweets_list.append([entity, tweet.date, tweet.id, tweet.content])

        # Usamos TwitterSearchScraper para scrapear datos y guardar tweets en lista - menciones enisa
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_enisa).get_items()):
            if i > 500:
                break
            tweets_enisa_list.append([entity, tweet.date, tweet.id, tweet.content])

    # Creamos dataframes con toda la inforamción - por si potencialmente se quieren trabajar los datos
    df_tweets = pd.DataFrame(tweets_list, columns=['Marca', 'Datetime', 'Tweet Id', 'Text'])
    df_tweets_enisa = pd.DataFrame(tweets_enisa_list, columns=['Marca', 'Datetime', 'Tweet Id', 'Text'])
    df_tweets.to_csv('df_twitter_detalle.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')
    df_tweets_enisa.to_csv('df_twitter_enisa_detalle.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')

    # Creamos dataframes y CSV con indicador de tweets en últimos 90d
    df_90d = df_tweets.Marca.value_counts().to_frame(name='Twitter_90d')
    df_90d['Marca'] = df_90d.index
    df_90d.reset_index(inplace=True, drop=True)
    df_90d.to_csv('df_90d.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')

    df_90d_enisa = df_tweets_enisa.Marca.value_counts().to_frame(name='Twitter_90d_enisa')
    df_90d_enisa['Marca'] = df_90d_enisa.index
    df_90d_enisa.reset_index(inplace=True, drop=True)
    df_90d_enisa.to_csv('df_90d_enisa.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')

    # Devolvemos los dataframe resultantes
    return df_90d, df_90d_enisa


def linkedin_scraper(userid, password, df_source, path):
    if userid and password:
        # Definimos path de chromedriver
        try:
            driver = webdriver.Chrome(path)
        except:
            print("No se encuentra webdriver, no se realizará scraping con Selenium")
            return 0

        # Definimos entidades a buscar a partir de scraping de ENISA
        entity_list = df_source["Marca"].to_list()

        # Definimos variables
        data = {}
        header = ["Marca", "Sitio web", "Sector", "Tamaño de la empresa", "Sede", "Tipo"]
        df_linkedin = pd.DataFrame(columns=header)

        # Abrimos el primer driver con la página de entrada de Linkedin
        try:
            driver.get('https://www.linkedin.com/login?fromSignIn=true&trk=guest_homepage-basic_nav-header-signin')
        except:
            print('Error al abrir fuente LINKEDIN')
            return df_linkedin

        # Hacemos login
        user = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "username")))
        user.send_keys(userid)
        pwd = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "password")))
        pwd.send_keys(password)
        pwd.submit()

        # Empezamos a iterar sobre el listado de empresas
        for entity in entity_list:
            data["Marca"] = entity
            try:
                company = "https://www.linkedin.com/company/"
                about = company + entity.replace(" ", "") + "/about/"
                # Accedemos a la página de "Acerca de" de la empresa
                driver.get(about)
                page_loaded = WebDriverWait(driver, 5).until(
                    EC.title_contains("Acerca"))

                # Pasamos a scrapear la página con bs4 - Recuperar datos útiles
                bs_about = driver.page_source
                soup = BeautifulSoup(bs_about, 'lxml')
                item_list = soup.find('dl')
                for item in item_list.find_all('dt'):
                    info = str.strip(item.text)
                    # print(info)
                    value = str.strip(item.find_next_sibling("dd").text)
                    if info in header: data[info] = value
                    # print(value)
                df_linkedin = df_linkedin.append(data, ignore_index=True)
            except:
                # Empresa NA - Pasamos a la siguiente
                pass

        driver.quit()

        # Creamos CSV y retornamos el dataframe con los datos
        df_linkedin.to_csv('df_linkedin.csv', sep=";", na_rep="", index=False, encoding='utf-8-sig')
        return df_linkedin
    else:
        header = ["Marca", "Sitio web", "Sector", "Tamaño de la empresa", "Sede", "Tipo"]
        df_linkedin = pd.DataFrame(columns=header)
        return df_linkedin


def infocif_scraper(df_source):
    # Definimos entidades a buscar a partir de scraping de ENISA
    lista_empresas = df_source["Razón Social"].to_list()

    # Inicializamos variables
    lista_cif = []
    df_infocif = pd.DataFrame(columns=['Razón Social', 'CIF'])

    # Iteramos para cada empresa
    for empresa in lista_empresas:
        nombre = empresa.replace(".", "")
        nombre = nombre.replace(",", "")
        nombre = nombre.replace(" ", "-")
        nombre = nombre.replace("'", "")

        # Pasamos a abrir la web a scrapear
        try:
            r = requests.get("https://www.infocif.es/ficha-empresa/" + nombre)
        except:
            print("Error al abrir fuente INFOCIF")
            return df_infocif

        # Hacemos scraping para recuperar CIF
        soup = BeautifulSoup(r.content, from_encoding='utf-8', features="lxml")
        cif = soup.find("h2", {"class": "editable col-md-10 col-sm-9 col-xs-12 mb10 text-right"})
        if cif:
            lista_cif.append([empresa, cif.text])

    # Creamos dataframes y CSV con indicador de tweets en últimos 90d
    df_infocif = pd.DataFrame(lista_cif, columns=['Razón Social', 'CIF'])
    df_infocif.to_csv("df_infocif.csv", sep=";", index=False, encoding='utf-8-sig')
    return df_infocif


def axesor_scraper(df_source, path):
    # Definimos entidades a buscar a partir de scraping de ENISA
    lista_empresas = df_source["CIF"].to_list()

    # Creamos la lista para recopilar CIFs
    lista_url = []
    lista_axesor = []

    # Camuflamos el user agent usando la librería fake_useragent
    options = Options()
    options.add_argument("window-size=1400,1000")
    ua = UserAgent()
    a = ua.random
    user_agent = ua.random
    options.add_argument(f'user-agent={user_agent}')

    # Usando Selenium, generamos el webdriver de chrome y cargamos la página de axesor
    try:
        driver = webdriver.Chrome(path, chrome_options=options)
    except:
        print("No se encuentra webdriver, no se realizará scraping con Selenium")
        return 0

    try:
        driver.get('https://www.axesor.es')
    except:
        print('Error al abrir fuente AXESOR')
        return 0

    try:
        i = 0
        # Iteramos para cada empresa
        for cif in lista_empresas:
            driver.get('https://www.axesor.es')
            # Cada 8 iteraciones reseteamos chromedriver y añadimos tiempo de espera para evitar bloqueo
            if i==8:
                driver.quit()
                sleep(randint(2, 4))
                driver.get('https://www.axesor.es')
                i = 0

            # En el buscador de Axesor, cargo el CIF de la empresa y lanzo la búscqueda
            search = driver.find_element_by_id('buscador-campo-nombre')
            search.clear()
            search.send_keys(cif)
            searchbutton = driver.find_element_by_xpath('//*[@id="buscador-submit"]')
            searchbutton.click()

            # Recuperamos la URL estática de la empresa desde la que lanzamos BeautifulSoup
            axesorUrl = driver.current_url
            lista_url.append([cif, axesorUrl])

            r = requests.get(axesorUrl)
            soup = BeautifulSoup(r.content.decode('utf-8', 'ignore'), 'lxml')
            table = soup.find("table", id="tablaInformacionGeneral")
            table_rows = table.findAll("tr")
            enlaces = table.find_all("a")
            for a in enlaces:
                a.decompose()

            # Identificamos las celdas de la tabla donde están los datos que queremos descargar
            try:
                tddireccion = table_rows[1].findAll("td")
                direccion = tddireccion[1].text
            except IndexError:
                direccion = ""
            try:
                tdconstitucion = table_rows[4].findAll("td")
                constitucion = tdconstitucion[1].text
            except IndexError:
                constitucion = ""
            try:
                tdCNAE = table_rows[6].findAll("td")
                CNAE = tdCNAE[1].text
            except IndexError:
                CNAE = ""
            try:
                tdSIC = table_rows[7].findAll("td")
                SIC = tdSIC[1].text
            except IndexError:
                SIC = ""

            # Añadimos los campos al dataframe
            lista_axesor.append([cif, direccion, constitucion, CNAE, SIC])
            # Incrementamos contador de bloqueo
            i = i+1
    except:
        driver.quit()
        df_axesor = pd.DataFrame(lista_axesor, columns=['CIF', 'Dirección', 'Constitución', 'CNAE', 'SIC'])
        df_axesor.to_csv("df_axesor.csv", sep=";", index=False, encoding='utf-8-sig')
        return df_axesor

    driver.quit()

    # Creamos dataframes y CSV con indicador de tweets en últimos 90d
    df_axesor = pd.DataFrame(lista_axesor, columns=['CIF', 'Dirección', 'Constitución', 'CNAE', 'SIC'])
    df_axesor.to_csv("df_axesor.csv", sep=";", index=False, encoding='utf-8-sig')
    return df_axesor


def genera_csv(df_enisa, df_infocif, df_axesor, df_twitter, df_twitter_enisa, df_linkedin):
    # Mergeamos los distintos dataframe obtenidos
    # Aseguramos que no exista error con la base de ENISA
    if not type(df_enisa) == int:
        df_enisa_completo = pd.merge(df_enisa, df_infocif, on="Razón Social", how="left")
        df_enisa_completo = pd.merge(df_enisa_completo, df_linkedin, on="Marca", how="left")
        df_enisa_completo = pd.merge(df_enisa_completo, df_axesor, on="CIF", how="left")
        df_enisa_completo = pd.merge(df_enisa_completo, df_twitter, on="Marca", how="left")
        df_enisa_completo = pd.merge(df_enisa_completo, df_twitter_enisa, on="Marca", how="left")

        # Generamos CSV final
        df_enisa_completo.to_csv("prestamos_enisa.csv", sep=";", index=False, encoding='utf-8-sig')
        print("Proceso de scraping finalizado: " + str(dt.datetime.now()))
