# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import csv
import sys
import os
from google.cloud import storage

sys.stdout.reconfigure(encoding='utf-8')

# URL de la página web
url_base = 'https://opennav.com/airportcodes'

# Lista de letras para la paginación
letras = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

# Encabezados para el archivo CSV
encabezados = ['IATA', 'ICAO', 'NAME', 'COUNTRY', 'ELEVATION', 'LATITUDE', 'LONGITUDE', 'WIKIPEDIA']

# Archivo CSV para guardar los datos
nombre_archivo_csv = 'airports_detail.csv'

with open(nombre_archivo_csv, 'w', newline='', encoding='utf-8') as archivo:
    escritor_csv = csv.writer(archivo)
    escritor_csv.writerow(encabezados)

    for letra in letras:
        print(letra)
        # Construir la URL de la página para cada letra
        url_pagina = f'{url_base}?{letra}'
        
        # Obtener el contenido de la página
        respuesta = requests.get(url_pagina,timeout=600)
        contenido = respuesta.content
        
        # Parsear el contenido con BeautifulSoup
        soup = BeautifulSoup(contenido, 'html.parser')
        
        # Encontrar la tabla con los datos de los aeropuertos
        tabla = soup.find('table')
        if tabla is not None:
            filas = tabla.find_all('tr')
            for fila in filas:
                # Extraer los datos de cada fila
                columnas = fila.find_all('td')
                if len(columnas) == 3:
                    iata = columnas[0].text.strip()
                    icao = columnas[1].text.strip()
                    name = columnas[2].text.strip()
                    
                    # Obtener el enlace a la página de detalles del aeropuerto
                    enlace = columnas[2].find('a')
                    if enlace is not None:
                        url_detalle = 'https://opennav.com'+ enlace['href']
                        #print(url_detalle)
                        # Obtener los detalles del aeropuerto
                        respuesta_detalle = requests.get(url_detalle,timeout=600)
                        contenido_detalle = respuesta_detalle.content
                        soup_detalle = BeautifulSoup(contenido_detalle, 'html.parser')
                        tabla_detalle = soup_detalle.find('table')
                        if tabla_detalle is not None:
                            fila_detalle = tabla_detalle.find_all('tr')
                            if len(fila_detalle) > 0:
                                # Extraer los campos de la tabla
                                try:
                                    country = fila_detalle[1].find_all('td')[1].text.strip()
                                except IndexError:
                                    country = ''
                                try:
                                    if "feet" in country:
                                        country=''
                                        elevation = fila_detalle[1].find_all('td')[1].text.strip()
                                        latitude = fila_detalle[2].find_all('td')[1].text.strip()
                                        longitude = fila_detalle[3].find_all('td')[1].text.strip()
                                        try:
                                            wikipedia = fila_detalle[4].find_all('td')[1].text.strip()
                                        except IndexError:
                                            wikipedia=''
                                    else:
                                        elevation = fila_detalle[2].find_all('td')[1].text.strip()
                                        latitude = fila_detalle[3].find_all('td')[1].text.strip()
                                        longitude = fila_detalle[4].find_all('td')[1].text.strip()
                                        try:
                                            wikipedia = fila_detalle[5].find_all('td')[1].text.strip()
                                        except IndexError:
                                            wikipedia=''
                                except IndexError:
                                    country=''
                                    elevation=''
                                    latitude=''
                                    longitude=''
                                    wikipedia=''
                                    print('Error')

                                print(wikipedia)
                                escritor_csv.writerow([iata, icao, name, country, elevation,latitude,longitude,wikipedia])
                                respuesta_detalle.close()
                        else:
                            country=''
                            elevation=''
                            latitude=''
                            longitude=''
                            wikipedia=''
                    # Escribir los datos en el archivo CSV
                    
                            escritor_csv.writerow([iata, icao, name, country, elevation,latitude,longitude,wikipedia])
        
        else:
            print(f'No se encontró la tabla en la página {url_pagina}')
            
        respuesta.close()

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#++++++++++++++++++LOAD TO BUCKET++++++++++++++++++++++++++++++++++
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Obtiene la ruta absoluta del archivo de credenciales en la carpeta actual
credenciales_path = os.path.join(os.getcwd(), "credentials.json")

# Establece la variable de entorno "GOOGLE_APPLICATION_CREDENTIALS" con la ruta absoluta del archivo de credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credenciales_path

client = storage.Client()
bucket = client.bucket('it-analytics-inventory-387221-dev')
bucket_path = 'datalake/workload/external_sources'

archivo_csv_bucket=os.path.join(os.getcwd(), nombre_archivo_csv)
nombre_blob = f'{bucket_path}/{nombre_archivo_csv}'
blob = bucket.blob(nombre_blob)
blob.upload_from_filename(archivo_csv_bucket)
print('archivo subido al bucket')
