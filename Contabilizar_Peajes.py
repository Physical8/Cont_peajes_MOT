import tkinter as tk
from tkinter import filedialog
from procesamiento import procesar_archivos
from datetime import datetime
import pandas as pd
import ctypes
import os
import threading

# Variables globales para las fechas
fecha_inicio = None
fecha_fin = None

# Diccionarios para los nombres de los meses y los cortes
meses = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

cortes = {
    "Corte 1": (1, 7), "Corte 2": (8, 14), "Corte 3": (15, 21), "Corte 4": (22, 31)
}

# Función para actualizar las fechas de inicio y fin según la selección de los menús desplegables
def actualizar_fechas():
    global fecha_inicio, fecha_fin
    mes_index = list(meses.keys())[list(meses.values()).index(mes_select.get())]  # Obtiene el índice del mes seleccionado
    corte_seleccionado = corte_select.get()
    if mes_index in meses.keys() and corte_seleccionado in cortes.keys():
        dia_inicio, dia_fin = cortes[corte_seleccionado]
        fecha_inicio = f"{dia_inicio:02d}/{mes_index:02d}/2024"
        fecha_fin = f"{dia_fin:02d}/{mes_index:02d}/2024"
        fecha_inicio = datetime.strptime(fecha_inicio, "%d/%m/%Y").strftime("%Y-%m-%d")
        fecha_fin = datetime.strptime(fecha_fin, "%d/%m/%Y").strftime("%Y-%m-%d")
        # Deshabilitar los menús desplegables después de confirmar la selección
        mes_dropdown["state"] = "disabled"
        corte_dropdown["state"] = "disabled"
        confirmar_button.config(state="disabled")
        # Verificar en consola
        print("Fechas")
        print(fecha_inicio)
        print(fecha_fin)
        # Verificar si todos los archivos están cargados y habilitar el botón de procesamiento
        check_archivos_cargados()

# Función para cargar un archivo de Excel
def cargar_archivo():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
    if file_path:
        return pd.read_excel(file_path, engine='openpyxl'), os.path.basename(file_path)
    else:
        return None
    
def cargar_archivo2():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
    if file_path:
        # Leer el archivo Excel sin encabezados
        df = pd.read_excel(file_path, engine='openpyxl', header=None)
        
        # Asignar encabezados manualmente
        encabezados = df.iloc[2]  # Obtener la tercera fila como encabezados
        df = df[3:]  # Eliminar las primeras tres filas (encabezados y filas vacías)
        df.columns = encabezados  # Asignar los encabezados al DataFrame
        
        return df, os.path.basename(file_path)
    else:
        return None

# Función para mostrar el mensaje de "Cargado exitosamente"
def mostrar_mensaje(label, texto):
    label.config(text=texto)

# Función para cargar el archivo "Flypass"
def cargar_flypass():
    global Flypass
    Flypass, nombre_archivo = cargar_archivo()
    if Flypass is not None:
        mensaje = f"Cargado exitosamente: {nombre_archivo}"
        mostrar_mensaje(flypass_label, mensaje)
        check_archivos_cargados()

# Función para cargar el archivo "MF General"
def cargar_general():
    global General
    General, nombre_archivo = cargar_archivo2()
    if General is not None:
        mensaje = f"Cargado exitosamente: {nombre_archivo}"
        mostrar_mensaje(general_label, mensaje)
        check_archivos_cargados()

# Función para cargar el archivo "MF Descargue"
def cargar_descargue():
    global Descargue
    Descargue, nombre_archivo = cargar_archivo2()
    if Descargue is not None:
        mensaje = f"Cargado exitosamente: {nombre_archivo}"
        mostrar_mensaje(descargue_label, mensaje)
        check_archivos_cargados()

# Función para cargar el archivo "Trayectos"
def cargar_trayectos():
    global Trayectos
    Trayectos, nombre_archivo = cargar_archivo()
    if Trayectos is not None:
        mensaje = f"Cargado exitosamente: {nombre_archivo}"
        mostrar_mensaje(trayectos_label, mensaje)
        check_archivos_cargados()

# Función para cargar el archivo "Trayectos"
def cargar_acumulado():
    global Acumulado
    Acumulado, nombre_archivo = cargar_archivo()
    if Acumulado is not None:
        mensaje = f"Cargado exitosamente: {nombre_archivo}"
        mostrar_mensaje(acumulado_label, mensaje)
        check_archivos_cargados()

# Función para comprobar si todos los archivos están cargados y habilitar el botón de procesamiento
def check_archivos_cargados():
    if 'Flypass' in globals() and 'Descargue' in globals() and 'General' in globals():
    #if 'Flypass' in globals() and 'General' in globals() and 'Descargue' in globals() and 'Trayectos' in globals() and 'Acumulado' in globals():
        if fecha_inicio is not None and fecha_fin is not None:  # Verifica si ambas fechas no son None
            mostrar_mensaje(msg5_label, "¡Todo listo\npara procesar!")
            procesar_button.config(state=tk.NORMAL)
        else:
            mostrar_mensaje(msg5_label, "Archivos Cargados.\nFalta definir las fechas")

# Función para procesar la información entre los archivos cargados
def procesar_informacion():
    mostrar_mensaje(resultado_label, "Procesando... Por favor, espere.")
    
    # Llama a la función de procesamiento y pasa los DataFrames de los archivos cargados como argumentos
    resultado_procesado = procesar_archivos(Flypass, Descargue,fecha_inicio,fecha_fin,General)
    #resultado_procesado = procesar_archivos(Flypass, General, Descargue, Trayectos, Acumulado,fecha_inicio,fecha_fin)
    # Muestra un mensaje de éxito y habilita el botón para descargar el resultado
    mostrar_mensaje(resultado_label, "Información procesada correctamente.")
    descargar_resultado_button.config(state=tk.NORMAL)
    # Asigna el resultado procesado a una variable global o utiliza según tu necesidad
    global Resultado
    Resultado = resultado_procesado


# Función para descargar el resultado del procesamiento
def descargar_resultado():
    global Resultado
    file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    if file_path:
        Resultado.to_excel(file_path, index=False)
        mostrar_mensaje(ruta_label, f"Proceso finalizado.\nResultado guardado en: {file_path}")
    else:
        mostrar_mensaje(ruta_label, "Guardado cancelado")

# Obtener el alto de la barra de tareas
def get_taskbar_height():
    hwnd = ctypes.windll.user32.FindWindowW(u'Shell_TrayWnd', None)
    rect = ctypes.wintypes.RECT()
    ctypes.windll.user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect.bottom - rect.top
    
# Crear la ventana principal de Tkinter
root = tk.Tk()
root.title("Sistema de contabilización de Peajes")

# Obtener las dimensiones de la pantalla
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Obtener el alto de la barra de tareas
taskbar_height = get_taskbar_height()

# Definir las dimensiones de la ventana
window_width = 750
window_height = 735

# Calcular la posición para centrar la ventana
x = (screen_width // 2) - (window_width // 2)
y = (screen_height // 2) - (window_height // 2) - (taskbar_height // 2)

# Establecer la geometría de la ventana
root.geometry(f"{window_width}x{window_height}+{x}+{y}")


# Etiquetas para mostrar mensajes
msg1_label = tk.Label(root, text="1. Defina los parámetros inciales:", justify="left")
msg1_label.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky='w')



fecha_guardada_label = tk.Label(root, text="")
fecha_guardada_label.grid(row=3, column=0, columnspan=4, padx=10, pady=10)

msg2_label = tk.Label(root, text="2. Cargue los siguientes archivos:")
msg2_label.grid(row=4, column=0, columnspan=2, padx=10, pady=10, sticky='w') 

flypass_label = tk.Label(root, text="")
flypass_label.grid(row=5, column=2, padx=10, pady=10)  

general_label = tk.Label(root, text="")
general_label.grid(row=6, column=2, padx=10, pady=10)  

descargue_label = tk.Label(root, text="")
descargue_label.grid(row=7, column=2, padx=10, pady=10)  

trayectos_label = tk.Label(root, text="")
trayectos_label.grid(row=8, column=2, padx=10, pady=10)  

acumulado_label = tk.Label(root, text="")
acumulado_label.grid(row=9, column=2, padx=10, pady=10) 

msg5_label = tk.Label(root, text="\n")
msg5_label.grid(row=10, column=2, columnspan=2, padx=10, pady=10) 

msg3_label = tk.Label(root, text="3. Ejecute el proceso:", justify="left")
msg3_label.grid(row=11, column=0, columnspan=2, padx=10, pady=10, sticky='w') 

resultado_label = tk.Label(root, text="")
resultado_label.grid(row=13, column=2, columnspan=2, padx=10, pady=10)  

msg4_label = tk.Label(root, text="4. Descargue los archivos:", justify="left")
msg4_label.grid(row=14, column=0, columnspan=2, padx=10, pady=10, sticky='w')



 



ruta_label = tk.Label(root, text="")
ruta_label.grid(row=16, column=0, columnspan=4, padx=10, pady=10)  

  


# Botones para cargar archivos
cargar_flypass_button = tk.Button(root, text="Cargar Flypass", command=cargar_flypass, width=20)
cargar_flypass_button.grid(row=5, column=1, padx=10, pady=10)  

cargar_general_button = tk.Button(root, text="Cargar MF General", command=cargar_general, width=20)
cargar_general_button.grid(row=6, column=1, padx=10, pady=10)  

cargar_descargue_button = tk.Button(root, text="Cargar MF Descargue", command=cargar_descargue, width=20)
cargar_descargue_button.grid(row=7, column=1, padx=10, pady=10)  

cargar_trayectos_button = tk.Button(root, text="Cargar Trayectos", command=cargar_trayectos, width=20)
cargar_trayectos_button.grid(row=8, column=1, padx=10, pady=10)  

cargar_acumulado_button = tk.Button(root, text="Cargar Acumulado", command=cargar_acumulado, width=20)
cargar_acumulado_button.grid(row=9, column=1, padx=10, pady=10) 


# Menús desplegables para seleccionar el mes y el corte
mes_select = tk.StringVar(root)
corte_select = tk.StringVar(root)

mes_select.set("Seleccione Mes")
corte_select.set("Seleccione Corte")

# Define el tamaño estándar para los menús desplegables
menu_width = 20

mes_dropdown = tk.OptionMenu(root, mes_select, *meses.values())
mes_dropdown.config(width=menu_width)
mes_dropdown.grid(row=2, column=1, padx=10, pady=10)

corte_dropdown = tk.OptionMenu(root, corte_select, *cortes.keys())
corte_dropdown.config(width=menu_width)
corte_dropdown.grid(row=2, column=2, padx=10, pady=10)

# Botón para confirmar selección de fechas
confirmar_button = tk.Button(root, text="Confirmar Selección", command=actualizar_fechas)
confirmar_button.grid(row=3, column=1, columnspan=2, padx=10, pady=10)






# Botón para procesar la información (inicialmente deshabilitado)
procesar_button = tk.Button(root, text="Procesar Información", command=procesar_informacion, width=20, state=tk.DISABLED)
procesar_button.grid(row=12, column=1, padx=10, pady=10)

# Botón para descargar el resultado (inicialmente deshabilitado)
descargar_resultado_button = tk.Button(root, text="Tabla SoluING", command=descargar_resultado, width=20, state=tk.DISABLED)
descargar_resultado_button.grid(row=15, column=1, padx=10, pady=10)

# Botón para descargar el resultado (inicialmente deshabilitado)
descargar_resultado2_button = tk.Button(root, text="Pendientes", command=descargar_resultado, width=20, state=tk.DISABLED)
descargar_resultado2_button.grid(row=15, column=2, padx=10, pady=10)

# Ejecutar el bucle principal de Tkinter
root.mainloop()
