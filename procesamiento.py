import pandas as pd

def procesar_archivos(flypass_df, general_df, descargue_df, trayectos_df, acumulado_df):
    # Unir los DataFrames flypass_df y general_df
    resultado = pd.concat([flypass_df, general_df], ignore_index=True)
    
    return resultado