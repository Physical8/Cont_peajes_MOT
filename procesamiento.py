import pandas as pd
import warnings


df_TablaSoluING = pd.DataFrame()
flypasID_df = pd.DataFrame()
flypass_plantilla = pd.DataFrame()
df_acumulado = pd.DataFrame()

def procesar_archivos(flypass_df, descargue_df, fecha_inicio_df, fecha_fin_df):
    print(fecha_inicio_df)
    print(fecha_fin_df)
#def procesar_archivos(flypass_df, general_df, descargue_df, trayectos_df, acumulado_df, fecha_inicio_df, fecha_fin_df):
    
    # TRANSFORMACION DE ARCHIVO FLYPASS PARA EXTRAER SOLO LOS REGISTROS QUE HACEN PARTE DEL CORTE SOLICITADO
    flypass_df = modificacion_flypass(flypass_df, fecha_inicio_df, fecha_fin_df)
    flypass_plantilla = flypass_df.copy()
    
    # TRANSFORMACIÓN DE ARCHIVO MF DESCARGUE PARA DEJAR SOLO LA PLACA - MANIFIESTO - CARGUE - DESCARGUE
    descargue_df = modificacion_descargue(descargue_df)
    
    # CRUCE DE INFORMACIÓN PARA ASOCIAR EL MF QUE CORRESPONDE AL PEAJE E IDENTIFICAR LOS PENDIENTES
    df_acumulado

    # # Convertimos la columna 'FECHA_MVTO' a tipo datetime en df1
    # flypass_df['FECHA_MVTO'] = pd.to_datetime(flypass_df['FECHA_MVTO'])

    # # Realizamos la fusión de los DataFrames en función de la columna 'PLACA'
    # merged_df = pd.merge(flypass_df, descargue_df, on='PLACA', how='left')

    # # Filtramos las filas donde la 'FECHA_MVTO' está dentro del rango de 'CARGUE' y 'DESCARGUE'
    # filtered_df = merged_df[(merged_df['FECHA_MVTO'] >= merged_df['CARGUE']) & (merged_df['FECHA_MVTO'] <= merged_df['DESCARGUE'])]

    # # Creamos la columna 'MFTO ENCONTRADO' con los valores de 'MFTO' cuando la 'PLACA' coincide y la 'FECHA_MVTO' está dentro del rango
    # filtered_df.loc[:, 'MFTO ENCONTRADO'] = filtered_df['MFTO']

    # # Cuando la 'PLACA' coincide pero la 'FECHA_MVTO' no está dentro del rango, asignamos 'No encontrado'
    # filtered_df.loc[~filtered_df['MFTO ENCONTRADO'].notnull(), 'MFTO ENCONTRADO'] = 'No encontrado porque la FECHA_MVTO no está dentro del rango CARGUE y DESCARGUE de la placa correspondiente'

    # # Eliminamos registros duplicados basados en la columna 'PLACA'
    # df_acumulado = filtered_df.drop_duplicates(subset=['ID'])

    # # Seleccionamos las columnas deseadas
    # df_acumulado = df_acumulado[['ID', 'FECHA_MVTO', 'PLACA', 'MFTO ENCONTRADO']]


    # merged_df2 = pd.merge(flypasID_df2,df_acumulado, on='ID', how='left')

    df_TablaSoluING = descargue_df

    # Devolver el DataFrame modificado
    return df_TablaSoluING

def modificacion_flypass(flypass_df, fecha_inicio_df, fecha_fin_df):

    # Insertar una nueva columna "ID" en el DataFrame flypass_df
    flypass_df.insert(0, "ID", range(1, len(flypass_df) + 1))

    # ---------------------------------------------------------------------------------------
    # -------------------------------IMPORTANTE ---------------------------------------------

    # Aca se genera el DataFrame auxiliar para trabajar despues con los pendientes

    global flypasID_df
     # Para modificar la variable global
    flypasID_df = flypass_df  # Asignar el DataFrame modificado a la variable global

    # ---------------------------------------------------------------------------------------

    #Continuación del proceso de modificacion de Flypass
    flypass_df['FECHA_APLICACION'] = flypass_df['FECHA_APLICACION'].str.split().str[0]

    # Convertir la columna 'FECHA_APLICACION' a tipo datetime
    flypass_df['FECHA_APLICACION'] = pd.to_datetime(flypass_df['FECHA_APLICACION'])
    print(flypass_df['FECHA_APLICACION'])
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


    # Ignorar la advertencia FutureWarning específica
    warnings.filterwarnings("ignore", message="Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated.*", category=FutureWarning)

    # Calcular la diferencia de tiempo entre DESCARGUE de cada PLACA
    descargue_df['Diferencia'] = descargue_df.groupby('PLACA')['DESCARGUE'].diff().fillna(pd.Timedelta(seconds=0))

    # Convertir la columna 'Diferencia' a tipo timedelta
    descargue_df['Diferencia'] = descargue_df['Diferencia'].astype('timedelta64[ns]')

    # Calcular la fecha y hora de cargue usando la lógica deseada
    descargue_df['CARGUE'] = descargue_df['DESCARGUE'] - descargue_df['Diferencia']

    # Llenar la primera fila de la columna CARGUE con la fecha y hora de DESCARGUE
    descargue_df.loc[descargue_df.groupby('PLACA').head(1).index, 'CARGUE'] = descargue_df['DESCARGUE']

    # Reorganizar las columnas cambiando el orden de los nombres de las columnas
    descargue_df = descargue_df[['PLACA', 'MFTO', 'CARGUE', 'DESCARGUE']]




    return descargue_df

