import pandas as pd
import warnings
from datetime import date


df_TablaSoluING = pd.DataFrame()
flypasID_df = pd.DataFrame()
flypass_plantilla = pd.DataFrame()
df_etapa1 = pd.DataFrame()
df_etapa2 = pd.DataFrame()
df_etapa3 = pd.DataFrame()
flypass_prev_pdte = pd.DataFrame()
general_df_original = pd.DataFrame()



def procesar_archivos(flypass_df, descargue_df, fecha_inicio_df, fecha_fin_df,general_df,Pendientes_df,Trayectos_df,acumulado_maestro_df):
    print(fecha_inicio_df)
    print(fecha_fin_df)
#def procesar_archivos(flypass_df, general_df, descargue_df, trayectos_df, acumulado_df, fecha_inicio_df, fecha_fin_df):
    
    # TRANSFORMACION DE ARCHIVO FLYPASS PARA EXTRAER SOLO LOS REGISTROS QUE HACEN PARTE DEL CORTE SOLICITADO
    flypass_df = modificacion_flypass(flypass_df, fecha_inicio_df, fecha_fin_df)

    if not Pendientes_df.empty:
        flypass_df = pegar_pendientes(flypass_df, Pendientes_df)
        print("añadio los pendientes")

    print("continuo melo")
    flypass_plantilla = flypass_df.copy()
    
    # TRANSFORMACIÓN DE ARCHIVO MF DESCARGUE PARA DEJAR SOLO LA PLACA - MANIFIESTO - CARGUE - DESCARGUE
    descargue_df = modificacion_descargue(descargue_df)
    
    # CRUCE DE INFORMACIÓN PARA HALLAR LOS MF A PARTIR DE LOS VIAJES QUE YA ESTAN DESCARGADOS, FALTARIA IDENTIFICAR CUALES ESTAN EN CURSO
    # Y DEFINIR LOS QUE DEFINITIVAMENTE LOS QUE NO TIENEN UN MF (PENDIENTES)
    df_etapa1 = cruce_fly_descargue(flypass_df,descargue_df)

    # COPIA DE SEGURIDAD DE MF GENERAL PARA PODER EXTRAER LA INFORMACIÓN Y ARMAR LA TABLA SOLUING
    general_df_original = general_df.copy()

    general_df = asoc_mf_encurso(general_df)
    
    #CRUCE DE ARCHIVOS FLY PASS , EL FLYPASS ORIGINAL CON CORTE DEFINIDO Y EL FLYPASS CON MF HALLADOS CON DESCARGUE , CON EL FIN DE EXTRAER
    #LAS FILAS VACIAS PARA BUSCAR EN GENERAL
    df_etapa2 = cruce_acu_general(flypass_plantilla,df_etapa1)

    #DIVISION DEL ARCHIVO df_etapa2 PARTE 1 = REGISTROS VACIOS  PARTE 2 = REGISTROS CON INFORMACION
    # Dividir el DataFrame en dos partes
    etapa2_con_datos = df_etapa2[df_etapa2['MFTO ENCONTRADO'].notnull()]  # Filas donde la columna MFTO tiene datos
    etapa2_sin_datos = df_etapa2[df_etapa2['MFTO ENCONTRADO'].isnull()]    # Filas donde la columna MFTO está vacía

    #TRANSFORMACION DE ARCHIVO sin datos para unir posteriormente

    etapa2_sin_datos = modificar_sin_datos(etapa2_sin_datos)

    df_etapa3 = cruce_fly_general(etapa2_sin_datos,general_df)

    flypass_prev_pdte = hallar_pendientes(etapa2_sin_datos,df_etapa3)

    pendientes_v1 = flypass_prev_pdte[flypass_prev_pdte['MFTO ENCONTRADO'].isnull()]    # Filas donde la columna MFTO está vacía

    # Unión de los DataFrames donde se hallaron manifiestos, uno con descargue y otro con general - luego organizado por el ID
    df_concatenado = pd.concat([df_etapa1, df_etapa3]).sort_values(by=['ID'], ascending=[True])

    # Ahora relacionar el archivo concatenado con el archivo flypass original para tener un archivo FLypass con todos los posibles MF 
    df_flypass_MFenc = relacionar_mf(flypass_plantilla,df_concatenado)

    # Extracción de MF ENCONTRADOS
    df_materia_prima = df_flypass_MFenc[df_flypass_MFenc['MFTO ENCONTRADO'].notnull()] 

    # Extraccion de pendientes
    df_pendientes = df_flypass_MFenc[df_flypass_MFenc['MFTO ENCONTRADO'].isnull()] 

    # Consolidacion de informacion
    df_mp_consolidada = consolidar_transa(df_materia_prima)

    #Extracción de Informacion de general para armado 1 de TablaSoluING
    Tabla1 = cruce_con_general(general_df_original,df_mp_consolidada)

    Trayectos_df = transformar_trayectos(Trayectos_df)

    #Cruce de informacion para traer codigo de trayectos
    Tabla2 = pd.merge(Tabla1, Trayectos_df, on='RUTA', how='left')

    # Logica para hallar Ruta 1
    Tabla2['Orig'] = Tabla2['Origen'].str.split('(').str[0]
    Tabla2['Dest'] = Tabla2['Destino'].str.split('(').str[0]
    Tabla2['Ruta 1'] = Tabla2['Orig'].str.cat(Tabla2['Dest'], sep=' - ')
    columnas_a_eliminar10 = ['Orig','Dest']
    Tabla3 = Tabla2.drop(columns=columnas_a_eliminar10)
    # _____________________________________________________________________

    # Asignar tipo de Documento
    Tabla3['Tipo Doc'] = Tabla3['Total'].apply(determinar_tipo_doc)

    # Agregar la columna "Nit Tercero" con el valor constante "900219834"
    Tabla3['Nit Tercero'] = 900219834
    Tabla3['Nombre Tercero'] = 'F2X'
    Tabla3['MARCA'] = 'CORTE ACTUAL'

    # Reorganizar las columnas cambiando el orden de los nombres de las columnas
    Tabla3 = Tabla3[['Año Apl', 'Mes Apl', 'Dia Apl', 'Placa', 'Referencia1', 'Manifiesto', 'RUTA', 'CONDUCTOR', 'CLIENTE', 'Peaje', 'Total', 'Nit Tercero', 'Nombre Tercero', 'Tipo Doc', 'Origen', 'Destino', 'Ruta 1', 'Cod.rut.Prp', 'MARCA']]

    df_masteracu_y_actual = pd.concat([acumulado_maestro_df, Tabla3]).sort_values(by=['Placa','Manifiesto','Mes Apl','Dia Apl'], ascending=[True, True, True, True])

    df_masteracu_y_actual['Numerox'] = ''

    # Agrupar por 'Manifiesto' y contar el número de ocurrencias dentro de cada grupo
    df_masteracu_y_actual['Numerox'] = df_masteracu_y_actual.groupby('Manifiesto').cumcount()

    # Ajustar la numeración para que inicie en 0 o vacío
    df_masteracu_y_actual['Numerox'] = df_masteracu_y_actual['Numerox'].apply(lambda x: '' if x == 0 else x + 0)

    # Añadir el valor del 'Manifiesto' a la secuencia generada
    df_masteracu_y_actual['Numerox'] = df_masteracu_y_actual['Manifiesto'].astype(str) + df_masteracu_y_actual['Numerox'].astype(str)

    # Copiar df_masteracu_y_actual a df_acu_master
    df_acu_master = df_masteracu_y_actual.copy()

    # Copiar df_masteracu_y_actual a df_TablaSoluING
    df_TablaSoluING = df_masteracu_y_actual.copy()

    df_TablaSoluING = df_TablaSoluING[df_TablaSoluING['MARCA'] == 'CORTE ACTUAL']
    df_TablaSoluING = df_TablaSoluING.sort_values(by=['Mes Apl', 'Dia Apl'], ascending=[True, True])
    df_TablaSoluING.drop(columns=['Documento'], inplace=True)
    df_TablaSoluING.drop(columns=['MARCA'], inplace=True)
    df_TablaSoluING = df_TablaSoluING.rename(columns={'Numerox': 'Documento'})
    
    # Iterar sobre cada valor de la columna "Documento" y aplicar la función try_convert_to_numeric
    df_TablaSoluING['Documento'] = df_TablaSoluING['Documento'].apply(try_convert_to_numeric)


    df_acu_master.drop(columns=['Documento'], inplace=True)
    df_acu_master.drop(columns=['MARCA'], inplace=True)
    df_acu_master = df_acu_master.rename(columns={'Numerox': 'Documento'})

    df_acu_master['Documento'] = df_acu_master['Documento'].apply(try_convert_to_numeric)

    df_acumulado_total = df_acu_master.copy()

    return df_TablaSoluING, df_pendientes, df_acumulado_total

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
    #print(flypass_df['FECHA_APLICACION'])
    # Filtrar las filas que están dentro del rango entre fecha_inicio y fecha_fin
    flypass_df = flypass_df[(flypass_df['FECHA_APLICACION'] >= fecha_inicio_df) & (flypass_df['FECHA_APLICACION'] <= fecha_fin_df)]

     # Eliminar filas donde el valor de la columna 'TIPO' sea diferente a "CONSUMO PEAJE"
    flypass_df = flypass_df[flypass_df['TIPO'] == "CONSUMO PEAJE"]

     # Extraer placa de la columna 'DESCRIPCION' usando expresiones regulares
    flypass_df['PLACA'] = flypass_df['DESCRIPCION'].str.extract(r'(\b[A-Z]{3}\d{3,4}\b)')



    return flypass_df


def pegar_pendientes(flypass_df,Pendientes_df):

    # Descarte de registros relacionados a las PLACAS administrativas
    Pendientes_df.drop(Pendientes_df[Pendientes_df['PLACA'].isin(['GTN725', 'GTS333'])].index, inplace=True)

    columnas_a_eliminar_pend = ['ID','MFTO ENCONTRADO']
    Pendientes_df = Pendientes_df.drop(columns=columnas_a_eliminar_pend)

    # Insertar una nueva columna "ID" en el DataFrame Pendientes_df
    # Calcular la longitud del DataFrame flypass_df
    longitud_pend = len(Pendientes_df)

    # Generar una lista de valores para la columna "ID" comenzando desde 0.0001
    valores_id = [0.0001 + i * 0.0001 for i in range(longitud_pend)]

    # Insertar la columna "ID" en el DataFrame Pendientes_df
    Pendientes_df.insert(0, "ID", valores_id)

    # Unión de los DataFrames, pendientes mes pasado y mf corte actual - luego organizado por el ID
    df_concat_fly_pend = pd.concat([flypass_df, Pendientes_df]).sort_values(by=['ID'], ascending=[True])


    return df_concat_fly_pend

def modificacion_descargue(descargue_df):

    # Eliminar las columnas
    columnas_a_eliminar = ['VIAJE','TRAILER','CLIENTE','ORIGEN','DESTINO','INICIO VIAJE','TIEMPO EN RUTA']
    descargue_df = descargue_df.drop(columns=columnas_a_eliminar)
    

    #Se ajusta el nombre del encabezado de la columna para poder aplicar la logica de hallar la fecha y hora de cargue
    descargue_df = descargue_df.rename(columns={'ENTRADA DESCARGUE': 'DESCARGUE'})

    descargue_df = descargue_df.sort_values(by=['PLACA', 'DESCARGUE'], ascending=[True, True])

    # ---------------- LOGICA PARA HALLAR FECHA Y HORA DE CARGUE A PARTIR DE LA FECHA DE DESCARGUE DEL ARCHIVO ---------

    # Calcular la fecha de descargue
    descargue_df['CARGUE'] = descargue_df.groupby('PLACA')['DESCARGUE'].shift(1)

    # Para los primeros registros de cada placa, colocar la fecha de cargue como 1 día antes de la fecha de descargue
    primeros_indices = descargue_df.groupby('PLACA').head(1).index
    descargue_df.loc[primeros_indices, 'CARGUE'] = descargue_df.loc[primeros_indices, 'DESCARGUE'] - pd.Timedelta(days=1)

    # Reorganizar las columnas cambiando el orden de los nombres de las columnas
    descargue_df = descargue_df[['PLACA', 'MFTO', 'CARGUE', 'DESCARGUE']]

    return descargue_df

def cruce_fly_descargue(flypass_df,descargue_df):

    # Convertimos la columna 'FECHA_MVTO' a tipo datetime en df1
    flypass_df['FECHA_MVTO'] = pd.to_datetime(flypass_df['FECHA_MVTO'])

    # # Realizamos la fusión de los DataFrames en función de la columna 'PLACA'
    merged_df = pd.merge(flypass_df, descargue_df, on='PLACA', how='left')

    # # Filtramos las filas donde la 'FECHA_MVTO' está dentro del rango de 'CARGUE' y 'DESCARGUE'
    filtered_df = merged_df[(merged_df['FECHA_MVTO'] >= merged_df['CARGUE']) & (merged_df['FECHA_MVTO'] <= merged_df['DESCARGUE'])]

    # # Creamos la columna 'MFTO ENCONTRADO' con los valores de 'MFTO' cuando la 'PLACA' coincide y la 'FECHA_MVTO' está dentro del rango
    #filtered_df.loc[:, 'MFTO ENCONTRADO'] = filtered_df['MFTO']
    
    
    filtered_df = filtered_df.copy()
    filtered_df['MFTO ENCONTRADO'] = filtered_df['MFTO'].copy()




    # # Cuando la 'PLACA' coincide pero la 'FECHA_MVTO' no está dentro del rango, asignamos 'No encontrado'
    filtered_df.loc[~filtered_df['MFTO ENCONTRADO'].notnull(), 'MFTO ENCONTRADO'] = 'No encontrado porque la FECHA_MVTO no está dentro del rango CARGUE y DESCARGUE de la placa correspondiente'

    # # Eliminamos registros duplicados basados en la columna 'PLACA'
    df_acumulado = filtered_df.drop_duplicates(subset=['ID'])

    # # Seleccionamos las columnas deseadas
    df_acumulado = df_acumulado[['ID', 'FECHA_MVTO', 'PLACA', 'MFTO ENCONTRADO']]

    return df_acumulado

def asoc_mf_encurso(general_df):

    # Eliminar las columnas
    columnas_a_eliminar2 = ['Viaje','Origen','Destino','Ruta','Vinculo','Nombre conductor','Apellidos conductor','Cliente','Tarifa','Flete','FleteTotal','FleteNeto','Primer Anticipo','Otros Anticipos','Utilidad','%','TipoVehi','TipoCarga','DescProducto', 'FacturarA', 'LineaNegocio', 'Remesa', 'Nombre Tenedor', 'Apellidos Tenedor', 'Documento Tenedor', 'Carroceria', 'Oficina', 'CentroCosto', 'UsuLiquida', 'UsuDespacha', 'UsuCumple', 'UsuColoca']
    general_df = general_df.drop(columns=columnas_a_eliminar2)
    
    #Se ajusta el nombre del encabezado de la columna para poder aplicar la logica de hallar la fecha y hora de cargue
    general_df = general_df.rename(columns={'Fecha': 'CARGUE', 'Manifiesto': 'MFTO', 'Placa': 'PLACA'})

    general_df = general_df.sort_values(by=['PLACA', 'CARGUE'], ascending=[True, True])

    # Calcular la fecha de descargue
    general_df['DESCARGUE'] = general_df.groupby('PLACA')['CARGUE'].shift(-1)

    # Para los últimos registros de cada placa, colocar la fecha de hoy como DESCARGUE
    ultimos_indices = general_df.groupby('PLACA').tail(1).index
    general_df.loc[ultimos_indices, 'DESCARGUE'] = pd.Timestamp.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Reorganizar las columnas cambiando el orden de los nombres de las columnas
    general_df = general_df[['PLACA', 'MFTO', 'CARGUE', 'DESCARGUE']]

    

    return general_df

def cruce_acu_general(flypass_plantilla,df_etapa1):
    
    # Realizamos la fusión de los DataFrames en función de la columna 'PLACA'
    merged2_df = pd.merge(flypass_plantilla, df_etapa1, on='ID', how='left')

    df_etapa2 = merged2_df.copy()

    return df_etapa2

def modificar_sin_datos(etapa2_sin_datos):

    # Eliminar las columnas
    columnas_a_eliminar3 = ['FECHA_INGRESO','FECHA_APLICACION','TIPO','DESCRIPCION','TRANSACCION','MONTO','COMISION','VALOR_REAL','SALDO','FECHA_MVTO_y','PLACA_y','MFTO ENCONTRADO']
    etapa2_sin_datos = etapa2_sin_datos.drop(columns=columnas_a_eliminar3)

    return etapa2_sin_datos

import pandas as pd

def cruce_fly_general(etapa2_sin_datos, general_df):
    
    # Convertimos la columna 'FECHA_MVTO' a tipo datetime en df1
    etapa2_sin_datos['FECHA_MVTO_x'] = pd.to_datetime(etapa2_sin_datos['FECHA_MVTO_x'])

    etapa2_sin_datos = etapa2_sin_datos.rename(columns={'PLACA_x': 'PLACA'})

    ETP2GEN_merged_df = pd.merge(etapa2_sin_datos, general_df, on='PLACA', how='left')

    # Filtramos las filas donde la 'FECHA_MVTO' está dentro del rango de 'CARGUE' y 'DESCARGUE'
    ETP2GEN_filtered_df = ETP2GEN_merged_df[(ETP2GEN_merged_df['FECHA_MVTO_x'] >= ETP2GEN_merged_df['CARGUE']) & (ETP2GEN_merged_df['FECHA_MVTO_x'] <= ETP2GEN_merged_df['DESCARGUE'])].copy()

    # Creamos la columna 'MFTO ENCONTRADO' con los valores de 'MFTO' cuando la 'PLACA' coincide y la 'FECHA_MVTO' está dentro del rango
    ETP2GEN_filtered_df['MFTO ENCONTRADO'] = ETP2GEN_filtered_df['MFTO']

    # Cuando la 'PLACA' coincide pero la 'FECHA_MVTO' no está dentro del rango, asignamos 'No encontrado'
    ETP2GEN_filtered_df.loc[~ETP2GEN_filtered_df['MFTO ENCONTRADO'].notnull(), 'MFTO ENCONTRADO'] = 'No encontrado porque la FECHA_MVTO no está dentro del rango CARGUE y DESCARGUE de la placa correspondiente'

    # Eliminamos registros duplicados basados en la columna 'PLACA'
    ETP2GEN_df_acumulado = ETP2GEN_filtered_df.drop_duplicates(subset=['ID'])

    ETP2GEN_df_acumulado = ETP2GEN_df_acumulado[['ID', 'FECHA_MVTO_x', 'PLACA', 'MFTO ENCONTRADO']]

    ETP2GEN_df_acumulado = ETP2GEN_df_acumulado.rename(columns={'FECHA_MVTO_x': 'FECHA_MVTO'})

    return ETP2GEN_df_acumulado


def hallar_pendientes(etapa2_sin_datos,df_etapa3):
    
    etapa2_sin_datos = etapa2_sin_datos.rename(columns={'PLACA_x': 'PLACA'})

    # Realizamos la fusión de los DataFrames en función de la columna 'ID'
    merged3_df = pd.merge(etapa2_sin_datos, df_etapa3, on='ID', how='left')

    df_etapaPEN = merged3_df.copy()

    return df_etapaPEN

def relacionar_mf(flypass_plantilla,df_concatenado):
    
    # Realizamos la fusión de los DataFrames en función de la columna 'ID'
    mergedfly_x_mf_df = pd.merge(flypass_plantilla, df_concatenado, on='ID', how='left')
    # Eliminar las columnas
    columnas_a_eliminar4 = ['FECHA_MVTO_y','PLACA_y']
    mergedfly_x_mf_df = mergedfly_x_mf_df.drop(columns=columnas_a_eliminar4)

    mergedfly_x_mf_df = mergedfly_x_mf_df.rename(columns={'PLACA_x': 'PLACA'})
    mergedfly_x_mf_df = mergedfly_x_mf_df.rename(columns={'FECHA_MVTO_x': 'FECHA_MVTO'})

    df_asoc_x_id = mergedfly_x_mf_df.copy()

    return df_asoc_x_id


def consolidar_transa(df_materia_prima):

    # Eliminar las columnas
    columnas_a_eliminar5 = ['ID','FECHA_MVTO','FECHA_INGRESO','TIPO','MONTO','COMISION','SALDO']
    df_matery = df_materia_prima.drop(columns=columnas_a_eliminar5)

    # Extracción del nombre del peaje
    df_matery['Peaje'] = df_matery['DESCRIPCION'].str.split('-').str[0]
    df_matery.drop(columns=['DESCRIPCION'], inplace=True)

    df_matery_consol = df_matery.groupby(['FECHA_APLICACION', 'TRANSACCION']).agg({
    'Peaje': 'first',
    'VALOR_REAL': 'sum',
    'PLACA': 'first',
    'MFTO ENCONTRADO': 'first'
    }).reset_index()

    df_matery_consol = df_matery_consol.rename(columns={'MFTO ENCONTRADO': 'Manifiesto'})
    
    return df_matery_consol

def cruce_con_general(general_df_original,df_mp_consolidada):

    # Eliminar las columnas
    columnas_a_eliminar6 = ['Viaje', 'Fecha', 'Placa', 'Vinculo', 'Tarifa', 'Flete', 'FleteTotal', 'FleteNeto', 'Primer Anticipo', 'Otros Anticipos', 'Utilidad', '%', 'TipoVehi', 'TipoCarga', 'DescProducto', 'FacturarA', 'LineaNegocio', 'Remesa', 'Nombre Tenedor', 'Apellidos Tenedor', 'Documento Tenedor', 'Carroceria', 'Oficina', 'CentroCosto', 'UsuLiquida', 'UsuDespacha', 'UsuCumple', 'UsuColoca']
    general_df_original = general_df_original.drop(columns=columnas_a_eliminar6)

    cruce_general = pd.merge(df_mp_consolidada, general_df_original, on='Manifiesto', how='left')

    cruce_general = cruce_general.rename(columns={'PLACA': 'Placa', 'TRANSACCION': 'Referencia1', 'Ruta': 'RUTA', 'Cliente': 'CLIENTE', 'VALOR_REAL': 'Total'})

    # Crear columnas para el año, mes y día
    cruce_general['Año Apl'] = cruce_general['FECHA_APLICACION'].dt.year
    cruce_general['Mes Apl'] = cruce_general['FECHA_APLICACION'].dt.month
    cruce_general['Dia Apl'] = cruce_general['FECHA_APLICACION'].dt.day

    # Concatenar las columnas "Nombre conductor" y "Apellidos conductor" en una nueva columna "CONDUCTOR"
    cruce_general['CONDUCTOR'] = cruce_general['Nombre conductor'].str.cat(cruce_general['Apellidos conductor'], sep=' ')

    # Eliminar las columnas
    columnas_a_eliminar7 = ['FECHA_APLICACION','Nombre conductor','Apellidos conductor']
    cruce_general = cruce_general.drop(columns=columnas_a_eliminar7)

    return cruce_general


def transformar_trayectos(Trayectos_df):

    # Eliminar las columnas
    columnas_a_eliminar8 = ['Codigo', 'Descripcion', 'RutaAlterna', 'Kilometros', 'RutaAlterna1', 'RutaTercero', 'CodRutaGPS', 'IndMciaPeligrosa', 'IndUnigis', 'IndGPS']
    trans_trayectos = Trayectos_df.drop(columns=columnas_a_eliminar8)

    trans_trayectos['RUTA'] = trans_trayectos['Origen'].str.cat(trans_trayectos['Destino'], sep='-')

    # Eliminar las columnas
    columnas_a_eliminar9 = ['Origen','Destino']
    trans_trayectos = trans_trayectos.drop(columns=columnas_a_eliminar9)

    trans_trayectos = trans_trayectos.rename(columns={'RutaPropio': 'Cod.rut.Prp'})

    # Extracción de registros con codigo
    trans_trayectos = trans_trayectos[trans_trayectos['Cod.rut.Prp'].notnull()]

    # Eliminamos registros duplicados basados en la columna 'RUTA'
    trans_trayectos = trans_trayectos.drop_duplicates(subset=['RUTA'])

    return trans_trayectos

# Definir la función para determinar el valor de 'Tipo Doc'
def determinar_tipo_doc(total):
    if total < 0:
        return 'PEAJES CGV04'
    elif total > 0:
        return 'DFLY'
    else:
        return ''
    
def try_convert_to_numeric(value):
    try:
        return pd.to_numeric(value)
    except ValueError:
        return value