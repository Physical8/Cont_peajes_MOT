import tkinter as tk
from tkinter import filedialog
import pandas as pd
import threading

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

# Crear la ventana principal de Tkinter
root = tk.Tk()
root.title("Contabilizar peajes")
root.geometry("800x500")

# Etiquetas para mostrar mensajes de éxito
flypass_label = tk.Label(root, text="")
flypass_label.grid(row=1, column=1, padx=10, pady=10)  

general_label = tk.Label(root, text="")
general_label.grid(row=2, column=1, padx=10, pady=10)  

resultado_label = tk.Label(root, text="")
resultado_label.grid(row=3, column=1, padx=10, pady=10)  

ruta_label = tk.Label(root, text="")
ruta_label.grid(row=4, column=1, padx=10, pady=10)  

# Función para cargar el archivo "Flypass"
def cargar_flypass():
    global Flypass
    Flypass = cargar_archivo()
    if Flypass is not None:
        mostrar_mensaje(flypass_label, "Cargado exitosamente")
        if 'General' in globals():
            unir_archivos()

# Función para cargar el archivo "MF General"
def cargar_general():
    global General
    General = cargar_archivo()
    if General is not None:
        mostrar_mensaje(general_label, "Cargado exitosamente")
        if 'Flypass' in globals():
            unir_archivos()

# Botones para cargar archivos
cargar_flypass_button = tk.Button(root, text="Cargar Flypass", command=cargar_flypass)
cargar_flypass_button.grid(row=1, column=0, padx=10, pady=10)  

cargar_general_button = tk.Button(root, text="Cargar MF General", command=cargar_general)
cargar_general_button.grid(row=2, column=0, padx=10, pady=10)  

# Función para unir los archivos y guardar el resultado en un DataFrame llamado "Resultado"
def unir_archivos():
    global Resultado
    Resultado = pd.concat([Flypass, General], axis=0)  
    mostrar_mensaje(resultado_label, "Archivos Procesados exitosamente")
    descargar_resultado_button = tk.Button(root, text="Descargar Resultado", command=descargar_resultado)
    descargar_resultado_button.grid(row=3, column=0, padx=10, pady=10)  

# Función para descargar el resultado obtenido
def descargar_resultado():
    global Resultado
    if Resultado is not None:
        mostrar_mensaje(ruta_label, "Procesando... Por favor, espere.")
        # Ejecutar la descarga en un hilo separado
        t = threading.Thread(target=descargar_en_hilo, args=(Resultado,))
        t.start()
    else:
        mostrar_mensaje(ruta_label, "Primero debes unir los archivos")

# Función para descargar en un hilo separado
def descargar_en_hilo(data):
    file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    if file_path:
        data.to_excel(file_path, index=False)
        mostrar_mensaje(ruta_label, f"Resultado guardado en {file_path}")
    else:
        mostrar_mensaje(ruta_label, "Guardado cancelado")

# Ejecutar el bucle principal de Tkinter
root.mainloop()
