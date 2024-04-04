import pandas as pd

df_TablaSoluING = pd.DataFrame()
flypasID_df = pd.DataFrame()

def procesar_archivos(flypass_df, descargue_df, fecha_inicio_df, fecha_fin_df):
#def procesar_archivos(flypass_df, general_df, descargue_df, trayectos_df, acumulado_df, fecha_inicio_df, fecha_fin_df):
    #flypass_df = modificacion_flypass(flypass_df, fecha_inicio_df, fecha_fin_df)
    descargue_df = modificacion_descargue(descargue_df)
    df_TablaSoluING = descargue_df
    # Devolver el DataFrame modificado
    return df_TablaSoluING

def modificacion_flypass(flypass_df, fecha_inicio_df, fecha_fin_df):

    # Insertar una nueva columna "ID" en el DataFrame flypass_df
    flypass_df.insert(0, "ID", range(1, len(flypass_df) + 1))

    # ---------------------------------------------------------------------------------------
    # -------------------------------IMPORTANTE ---------------------------------------------

    # Aca se genera el DataFrame auxiliar para trabajar despues con los pendientes

    global flypasID_df  # Para modificar la variable global
    flypasID_df = flypass_df  # Asignar el DataFrame modificado a la variable global

    # ---------------------------------------------------------------------------------------

    #Continuación del proceso de modificacion de Flypass
    flypass_df['FECHA_APLICACION'] = flypass_df['FECHA_APLICACION'].str.split().str[0]

    # Convertir la columna 'FECHA_APLICACION' a tipo datetime
    flypass_df['FECHA_APLICACION'] = pd.to_datetime(flypass_df['FECHA_APLICACION'])

    # Filtrar las filas que están dentro del rango entre fecha_inicio y fecha_fin
    flypass_df = flypass_df[(flypass_df['FECHA_APLICACION'] >= fecha_inicio_df) & (flypass_df['FECHA_APLICACION'] <= fecha_fin_df)]

     # Eliminar filas donde el valor de la columna 'TIPO' sea diferente a "CONSUMO PEAJE"
    flypass_df = flypass_df[flypass_df['TIPO'] == "CONSUMO PEAJE"]

     # Extraer placa de la columna 'DESCRIPCION' usando expresiones regulares
    flypass_df['PLACA'] = flypass_df['DESCRIPCION'].str.extract(r'(\b[A-Z]{3}\d{3,4}\b)')


    return flypass_df

def modificacion_descargue(descargue_df):

    # Eliminar las columnas
    columnas_a_eliminar = ['VIAJE','TRAILER','CLIENTE','ORIGEN','DESTINO','INICIO VIAJE','TIEMPO EN RUTA']
    descargue_df = descargue_df.drop(columns=columnas_a_eliminar)
    descargue_df = descargue_df.sort_values(by=['PLACA', 'MFTO'], ascending=[True, True])

    #Se ajusta el nombre del encabezado de la columna para poder aplicar la logica de hallar la fecha y hora de cargue
    descargue_df = descargue_df.rename(columns={'ENTRADA DESCARGUE': 'DESCARGUE'})


    # ---------------- LOGICA PARA HALLAR FECHA Y HORA DE CARGUE A PARTIR DE LA FECHA DE DESCARGUE DEL ARCHIVO ---------


    # (NO OPTIMO segun GPT) Calcular la diferencia en minutos entre DESCARGUE actual y DESCARGUE anterior
    # descargue_df['Diferencia'] = descargue_df.groupby('PLACA')['DESCARGUE'].diff().fillna(pd.Timedelta(seconds=0))

    #Segun GPT es para optimización de código a diferencia de la anterior linea de codigo
    descargue_df['Diferencia'] = descargue_df.groupby('PLACA')['DESCARGUE'].diff().fillna(pd.Timedelta(seconds=0)).infer_objects(copy=False)

    # Calcular la fecha y hora de cargue usando la lógica deseada
    descargue_df['CARGUE'] = descargue_df['DESCARGUE'] - descargue_df['Diferencia']
    
    # Llenar la primera fila de la columna CARGUE con la fecha y hora de DESCARGUE
    descargue_df.loc[descargue_df.groupby('PLACA').head(1).index, 'CARGUE'] = descargue_df['DESCARGUE']

    # Eliminar la columna
    descargue_df = descargue_df.drop(columns=['Diferencia'])

    # Reorganizar las columnas cambiando el orden de los nombres de las columnas
    descargue_df = descargue_df[['PLACA', 'MFTO', 'CARGUE', 'DESCARGUE']]


    return descargue_df