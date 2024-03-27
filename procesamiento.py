import pandas as pd

df_TablaSoluING = pd.DataFrame()
flypasID_df = pd.DataFrame()

def procesar_archivos(flypass_df, general_df, descargue_df, trayectos_df, acumulado_df):
    flypass_df = modificacion_flypass(flypass_df)
    df_TablaSoluING = flypass_df
    # Devolver el DataFrame modificado
    return df_TablaSoluING

def modificacion_flypass(flypass_df):
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










    return flypass_df