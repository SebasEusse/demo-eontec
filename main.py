import functions_framework
import requests
import json
import pandas as pd
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from flask import jsonify


@functions_framework.http
def balance(request):
  headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  }

  if request.method == 'OPTIONS':
    return '', 204, headers

  if request.method == 'POST':
    try:  
      # Configurar el logging básico
      logging.basicConfig(level=logging.INFO,  # Nivel de logging (INFO, ERROR)
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
      logger = logging.getLogger(__name__)

      conn = sqlite3.connect(':memory:')

      data = request.get_json()

      
      # Definicion de variables

      id_archivo_balance_drive= data.get('id_archivo_balance_drive', 'drive id is missing')
      fecha_inicio_str = data.get('fecha_inicio', 'fecha inicio is missing')
      fecha_fin_str = data.get('fecha_fin', 'fecha fin  is missing')
      
      fast_upload_api_key_1="SOMZhzP2yrnfCyKY9sf5NxmXG3XTvFZ5bcV3JHCe2daHKypCTs965vfgZ7XlExaO"
      fast_upload_api_key_2="HOHVqtuAfy5aZGCFvwp9IZfqQTeA3g3QeBWoqOGX8uVipgH68yrKHyxkLGjq58NM"
      
      formato_fecha = "%Y-%m-%d"

      # Convertir las cadenas a objetos de fecha
      try:
        fecha_inicio = datetime.strptime(fecha_inicio_str, formato_fecha)
        fecha_fin = datetime.strptime(fecha_fin_str, formato_fecha)
      except ValueError as e:
          return jsonify({"error":"Las fechas no tienen el formato correcto. AAAA-mm-dd "}), 500

      # Validar que la fecha de inicio no sea mayor que la fecha de fin
      if fecha_inicio > fecha_fin:
        return jsonify({"error":"La fecha de inicio no puede ser mayor que la fecha de fin."}), 500



      ########### EXTRACCION ##############

      # Consumir Apis XM
        
      url = f"https://www.simem.co/backend-files/api/PublicData?startDate={fecha_inicio_str}&endDate={fecha_fin_str}&datasetId=96D56E"
      print(url)
      response_precios = requests.get(url)

      if response_precios.status_code == 200:
          logger.info("Se obtuvo la lista de precios")
          json_data_precios = response_precios.json()
          preciosBolsa = json_data_precios['result']
      else:
          logger.error("Error consumiendo la API de precios")
          return jsonify({"error":"Error consumiendo precios en la api SIMEM "}), 500



      url = f"https://www.simem.co/backend-files/api/PublicData?startdate={fecha_inicio_str}&endDate={fecha_fin_str}&datasetId=ff027b"
      response = requests.get(url)
      if response.status_code == 200:
          logger.info("Se obtuvo el plan de despachos")
          json_data = response.json()
          despachosUnidades = json_data['result']
      else:
          logger.error("Error consumiendo la API de despachos")
          return jsonify({"error":"Error consumiendo despachos en la api SIMEM "}), 500


      # Descargar Drive File

      download_url = f'https://drive.google.com/uc?export=download&id={id_archivo_balance_drive}'

      try:
        # Descargar el archivo
        response = requests.get(download_url)
        local_file_path = '/tmp/balances1.xlsx'
        response.raise_for_status()
        
        # Guardar el archivo en la ruta temporal
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
            logger.info("Descarga exitosa")

      except requests.exceptions.RequestException as e:
        # Manejar errores en la solicitud
        logger.error("Error descargando el archivo de Drive")
        return jsonify({"error":"Error descargando el archivo de Drive"}), 500

      try:
        dfArchivoCapacidad = pd.read_excel(local_file_path,engine='openpyxl')
      except:
        return jsonify({"error":"Error leyendo el archivo de Drive"}), 500

      if(dfArchivoCapacidad.empty):
        return jsonify({"error":"El archivo de capacidad esta vacio"}), 500

      print(dfArchivoCapacidad.head())



      ########### TRANSFORMACION ##############

      preciosBolsa_records = preciosBolsa['records']
      df_precios_records = pd.DataFrame(preciosBolsa_records)
      logger.info("SIMEM Precios")
      if df_precios_records.empty:
        return jsonify({"error":"No hay informacion de precios en SIMEM."}), 500
      print(df_precios_records.head())

      despachosUnidades_records = despachosUnidades['records']
      df_despachos_records = pd.DataFrame(despachosUnidades_records)
      if df_despachos_records.empty:
        return jsonify({"error":"No hay informacion de despachos en SIMEM."}), 500
      logger.info("SIMEM Plan Despachos")
      print(df_despachos_records.head())


      df_despachos_records.to_sql('df_despachos_records', conn, index=False, if_exists='replace')

      query = "SELECT * FROM df_despachos_records WHERE CodigoPlanta IN ('ZPA2','ZPA3','ZPA4','ZPA5','GVIO','QUI1','CHVR') order by Valor,FechaHora DESC"
      dfDespachosAcme = pd.read_sql_query(query, conn)
      dfDespachosAcme.to_sql('dfCapacidadDespachosAcme', conn, index=False, if_exists='replace')

      
      # Definir el esquema deseado 
      column_mapping = {
          "FECHA": "fecha",
          "PLANTA": "planta",
          "GENERADOR": "generador",
          "CAPACIDAD (Kwh)": "capacidad",
          "CODIGO": "codigo"
      }

      
      # Renombrar las columnas según el esquema
      dfArchivoCapacidad = dfArchivoCapacidad.rename(columns=column_mapping)
      print(dfArchivoCapacidad)

      dfArchivoCapacidad['fecha'] = pd.to_datetime(dfArchivoCapacidad['fecha'])
      dfArchivoCapacidad['anio'] = dfArchivoCapacidad['fecha'].dt.year
      dfArchivoCapacidad['mes'] = dfArchivoCapacidad['fecha'].dt.month
      dfArchivoCapacidad['dia'] = dfArchivoCapacidad['fecha'].dt.day
      dfArchivoCapacidad['hora'] = dfArchivoCapacidad['fecha'].dt.hour
      
      # Seleccionar las columnas deseadas y eliminar las innecesarias
      dfArchivoCapacidad = dfArchivoCapacidad[['anio', 'mes', 'dia', 'hora', 'codigo', 'capacidad']]

      # Limpiar los valores nulos en el DataFrame
      dfArchivoCapacidad = dfArchivoCapacidad.dropna(how='all')  # Elimina filas completamente nulas 

      print(dfArchivoCapacidad)

      dfDespachosAcme['FechaHora'] = pd.to_datetime(dfDespachosAcme['FechaHora'])
      dfDespachosAcme['dia'] = dfDespachosAcme['FechaHora'].dt.day
      dfDespachosAcme['mes'] = dfDespachosAcme['FechaHora'].dt.month
      dfDespachosAcme['anio'] = dfDespachosAcme['FechaHora'].dt.year
      dfDespachosAcme['hora'] = dfDespachosAcme['FechaHora'].dt.hour

      # Seleccionar y renombrar las columnas
      dfDespachosAcme = dfDespachosAcme.rename(columns={
        'Valor': 'capacidad',
        'CodigoPlanta': 'codigo'
      })
      dfDespachosAcme = dfDespachosAcme[['anio', 'mes', 'dia', 'hora', 'codigo','capacidad']]

      
      logger.info("------------------dfDespachosAcmeTransformado--------------")
      print(dfDespachosAcme)
    
      dfArchivoCapacidad['codigo'] = dfArchivoCapacidad['codigo'].str.strip()
      dfDespachosAcme['codigo'] = dfDespachosAcme['codigo'].str.strip()
      
      dfArchivoCapacidad[['anio', 'mes', 'dia', 'hora', 'codigo']].drop_duplicates().head()
      dfDespachosAcme[['anio', 'mes', 'dia', 'hora', 'codigo']].drop_duplicates().head()

      logger.info("------------------dfArchivoCapacidad-----------------")
      print(dfArchivoCapacidad)


      df_merged = pd.merge(
        dfArchivoCapacidad,
        dfDespachosAcme,
        right_on=['anio', 'mes', 'dia', 'hora', 'codigo'],
        left_on=['anio', 'mes', 'dia', 'hora', 'codigo'],
      )
    
     # Paso 2: Calcular el balance disponible por hora
      df_merged['balance_disponible_horario'] = pd.to_numeric(df_merged['capacidad_x'], errors='coerce') - pd.to_numeric(df_merged['capacidad_y'], errors='coerce')
      df_balance_consolidado = df_merged.groupby(['anio', 'mes', 'dia', 'codigo']).agg({'balance_disponible_horario': 'sum'}).reset_index()


      df_balance_consolidado = df_balance_consolidado.rename(columns={'balance_disponible_horario': 'consolidado_planta'})


      precios_bolsa_filtrados = df_precios_records[
        (df_precios_records['CodigoVariable'] == "PPBOGReal") & 
        (df_precios_records['Version'] == "TXR")
      ].copy()

     # Crear columnas para día, mes y año
      precios_bolsa_filtrados['dia'] = pd.to_datetime(precios_bolsa_filtrados['Fecha']).dt.day
      precios_bolsa_filtrados['mes'] = pd.to_datetime(precios_bolsa_filtrados['Fecha']).dt.month
      precios_bolsa_filtrados['anio'] = pd.to_datetime(precios_bolsa_filtrados['Fecha']).dt.year

     # Paso 2: Unir las tablas 
      resultado_final = pd.merge(
        df_balance_consolidado, 
        precios_bolsa_filtrados[['dia', 'mes', 'anio', 'Valor']], 
        how='inner', 
        on=['anio', 'mes', 'dia']
      )

     # Paso 3: Calcular el compromiso en miles de millones de pesos
      resultado_final['Compromisos_MCOP'] = (resultado_final['consolidado_planta'] * resultado_final['Valor']) / 1000
      resultado_final['Operacion'] = resultado_final['consolidado_planta'].apply(lambda x: 'Comprar' if x < 0 else 'Vender')
      if resultado_final.empty:
        return jsonify({"error":"No hay inforacion para cruzar."}), 500

     # Resultado final
      print(resultado_final[['anio', 'mes', 'dia', 'codigo', 'consolidado_planta', 'Compromisos_MCOP','Operacion']])
      

     ########### CARGA ##############
      local_csv_path = "/tmp/dfReporteCompraVentaEnergiaAcme.csv"
      resultado_final.to_csv(local_csv_path, index=False, header=True)
      file_upload_access_token = ""
      file_upload_account_id = ""
      params = {'key1': fast_upload_api_key_1, 'key2': fast_upload_api_key_2}
      response = requests.get("https://fastupload.io/api/v2/authorize", params)
      json_response = json.loads(response.text)

      try:
        file_upload_access_token = json_response["data"]["access_token"]
        file_upload_account_id = json_response["data"]["account_id"]
      except:
        print("Error autenticando y autorizando en el servicio remoto de carga de archivos.")
        return jsonify({"error":"Error autenticando y autorizando en el servicio remoto de carga de archivos."}), 500


      upload_folder_id = ""
      json_response=""
      try:
        with open("/tmp/dfReporteCompraVentaEnergiaAcme.csv", "rb") as archivo:
            files = {"upload_file": (archivo.name, archivo)}  
            data = {
            "access_token": file_upload_access_token,
            "account_id": file_upload_account_id,
            "folder_id": upload_folder_id 
            }
            response = requests.post("https://fastupload.io/api/v2/file/upload", files=files, data=data)
            logger.info(f"Carga Exitosa")     
            return jsonify({"message": "Balance cargado exitosamente en XM "}), 200

      except requests.exceptions.RequestException as e:
        logger.error(f"Error durante la carga del balance: {e}")  
        return jsonify({"error":"Error durante la carga del balance"}), 500
    except Exception as e:
      # Manejo de errores
     return jsonify({"error": str(e)}), 500, headers    
   