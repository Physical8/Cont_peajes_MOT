import tkinter as tk
from tkinter import filedialog
from procesamiento import procesar_archivos
import pandas as pd
import threading

# Variables globales para las fechas
fecha_inicio = None
fecha_fin = None


# Botón para confirmar y guardar las fechas
def guardar_fechas():
    global fecha_inicio, fecha_fin
    fecha_inicio = fecha_inicio_entry.get()
    fecha_fin = fecha_fin_entry.get()
    print(f"Fecha inicio es {fecha_inicio} y la Fecha fin es {fecha_fin}")
    mostrar_mensaje(fecha_guardada_label, "Fechas guardadas correctamente.")
    # Deshabilitar los campos de entrada
    fecha_inicio_entry.config(state="disabled")
    fecha_fin_entry.config(state="disabled")
    check_archivos_cargados()


# Función para cargar un archivo de Excel
def cargar_archivo():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
    if file_path:
        return pd.read_excel(file_path, engine='openpyxl')
    else:
        return None

# Función para mostrar el mensaje de "Cargado exitosamente"
def mostrar_mensaje(label, texto):
    label.config(text=texto)

# Función para cargar el archivo "Flypass"
def cargar_flypass():
    global Flypass
    Flypass = cargar_archivo()
    if Flypass is not None:
        mostrar_mensaje(flypass_label, "Cargado exitosamente")
        check_archivos_cargados()

# Función para cargar el archivo "MF General"
def cargar_general():
    global General
    General = cargar_archivo()
    if General is not None:
        mostrar_mensaje(general_label, "Cargado exitosamente")
        check_archivos_cargados()

# Función para cargar el archivo "MF Descargue"
def cargar_descargue():
    global Descargue
    Descargue = cargar_archivo()
    if Descargue is not None:
        mostrar_mensaje(descargue_label, "Cargado exitosamente")
        check_archivos_cargados()

# Función para cargar el archivo "Trayectos"
def cargar_trayectos():
    global Trayectos
    Trayectos = cargar_archivo()
    if Trayectos is not None:
        mostrar_mensaje(trayectos_label, "Cargado exitosamente")
        check_archivos_cargados()

# Función para cargar el archivo "Trayectos"
def cargar_acumulado():
    global Acumulado
    Acumulado = cargar_archivo()
    if Acumulado is not None:
        mostrar_mensaje(acumulado_label, "Cargado exitosamente")
        check_archivos_cargados()

# Función para comprobar si todos los archivos están cargados y habilitar el botón de procesamiento
def check_archivos_cargados():
    if 'Flypass' in globals() and 'General' in globals() and 'Descargue' in globals() and 'Trayectos' in globals() and 'Acumulado' in globals():
        if fecha_inicio is not None and fecha_fin is not None:  # Verifica si ambas fechas no son None
            mostrar_mensaje(msg5_label, "Archivos Cargados. ¡Listos para procesar!")
            procesar_button.config(state=tk.NORMAL)
        else:
            mostrar_mensaje(msg5_label, "Archivos Cargados. Falta definir las fechas")

# Función para procesar la información entre los archivos cargados
def procesar_informacion():
    mostrar_mensaje(resultado_label, "Procesando... Por favor, espere.")
    # Llama a la función de procesamiento y pasa los DataFrames de los archivos cargados como argumentos
    resultado_procesado = procesar_archivos(Flypass, General, Descargue, Trayectos, Acumulado)
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
        mostrar_mensaje(ruta_label, f"Proceso finalizado. Resultado guardado en: {file_path}")
    else:
        mostrar_mensaje(ruta_label, "Guardado cancelado")

# Crear la ventana principal de Tkinter
root = tk.Tk()
root.title("Sistema de contabilización de Peajes")
root.geometry("750x700")

# Etiquetas para mostrar mensajes
msg1_label = tk.Label(root, text="1. Digite el periodo de contabilización con este formato DD/MM/AAAA")
msg1_label.grid(row=1, column=1, columnspan=2, padx=10, pady=10) 

msg2_label = tk.Label(root, text="          2. Cargue los siguientes archivos:")
msg2_label.grid(row=4, column=0, columnspan=2, padx=10, pady=10) 

msg3_label = tk.Label(root, text="          3. Ejecute el proceso:")
msg3_label.grid(row=11, column=0, columnspan=2, padx=10, pady=10) 

msg4_label = tk.Label(root, text="          4. Descargue los archivos:")
msg4_label.grid(row=14, column=0, columnspan=2, padx=10, pady=10) 

msg5_label = tk.Label(root, text="")
msg5_label.grid(row=10, column=2, columnspan=2, padx=10, pady=10) 

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

resultado_label = tk.Label(root, text="")
resultado_label.grid(row=13, column=2, columnspan=2, padx=10, pady=10)  

ruta_label = tk.Label(root, text="")
ruta_label.grid(row=16, column=0, columnspan=4, padx=10, pady=10)  

fecha_guardada_label = tk.Label(root, text="")
fecha_guardada_label.grid(row=3, column=0, columnspan=4, padx=10, pady=10)  


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


# Etiquetas y entradas para seleccionar las fechas
fecha_inicio_label = tk.Label(root, text="Fecha de inicio:")
fecha_inicio_label.grid(row=2, column=1, padx=10, pady=10)

fecha_inicio_entry = tk.Entry(root)
fecha_inicio_entry.grid(row=2, column=2, padx=10, pady=10)

fecha_fin_label = tk.Label(root, text="Fecha de fin:")
fecha_fin_label.grid(row=2, column=3, padx=10, pady=10)

fecha_fin_entry = tk.Entry(root)
fecha_fin_entry.grid(row=2, column=4, padx=10, pady=10)



guardar_fechas_button = tk.Button(root, text="Guardar Fechas", command=guardar_fechas)
guardar_fechas_button.grid(row=2, column=5, columnspan=2, padx=10, pady=10)


# Botón para procesar la información (inicialmente deshabilitado)
procesar_button = tk.Button(root, text="Procesar Información", command=procesar_informacion, state=tk.DISABLED)
procesar_button.grid(row=12, column=1, padx=10, pady=10)

# Botón para descargar el resultado (inicialmente deshabilitado)
descargar_resultado_button = tk.Button(root, text="Descargar Archivo", command=descargar_resultado, state=tk.DISABLED)
descargar_resultado_button.grid(row=15, column=1, padx=10, pady=10)

# Ejecutar el bucle principal de Tkinter
root.mainloop()
