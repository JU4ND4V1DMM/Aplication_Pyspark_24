import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import pandas as pd

def limpiar_y_guardar_archivo(ruta_entrada, ruta_salida):
    try:
        # Lee el archivo Excel
        df = pd.read_excel(ruta_entrada)

        # Limpiar columna de hora de salida
        df['usuhorasalida'] = df['usuhorasalida'].apply(lambda x: str(x).split(',')[-1])

        # Limpiar columna de fecha
        df['fecha'] = df['fecha'].apply(lambda x: str(x).split(' ')[0])

        # Limpiar columna de hora de entrada
        df['usuhoraentrada'] = df['usuhoraentrada'].apply(lambda x: str(x).split(',')[0])

        # Verificar si las columnas 'Ruta_Entrada' y 'Ruta_Salida' existen antes de intentar eliminarlas
        columnas_a_eliminar = ['Ruta_Entrada', 'Ruta_Salida']
        columnas_existentes = df.columns.tolist()
        columnas_a_eliminar = [col for col in columnas_a_eliminar if col in columnas_existentes]

        # Eliminar las columnas si existen
        if columnas_a_eliminar:
            df.drop(columns=columnas_a_eliminar, inplace=True)

        # Guardar el DataFrame modificado en un nuevo archivo Excel
        nombre_archivo_salida = 'archivo_limpiado.xlsx'
        ruta_archivo_salida = ruta_salida + '\\' + nombre_archivo_salida
        df.to_excel(ruta_archivo_salida, index=False)

        print("¡Proceso completado! Se ha creado el archivo '{}' con las columnas 'usuhorasalida', 'fecha' y 'usuhoraentrada' limpiadas.".format(nombre_archivo_salida))
        messagebox.showinfo("Proceso Completado", f"Se ha limpiado y guardado el archivo en {ruta_archivo_salida}.")
    except FileNotFoundError:
        messagebox.showerror("Error", f"El archivo {ruta_entrada} especificado no fue encontrado.")
    except Exception as e:
        messagebox.showerror("Error", f"Ocurrió un error al procesar el archivo: {str(e)}")


def seleccionar_archivo():
    ruta_archivo = filedialog.askopenfilename(title="Seleccionar archivo Excel", filetypes=[("Archivos Excel", "*.xlsx")])
    if ruta_archivo:
        ruta_entrada_entry.delete(0, tk.END)
        ruta_entrada_entry.insert(0, ruta_archivo)
        ruta_entrada_entry.config(width=500, height=350 , large= 20 ,)  # Ajustar el ancho y el alto de la caja de entrada

def seleccionar_carpeta_salida():
    ruta_carpeta = filedialog.askdirectory()
    if ruta_carpeta:
        ruta_salida_entry.delete(0, tk.END)
        ruta_salida_entry.insert(0, ruta_carpeta)
        ruta_salida_entry.config(width=50, height=5)  # Ajustar el ancho y el alto de la caja de entrada


# Crear la ventana principal
root = tk.Tk()  
root.title("Función de Logueos")
root.geometry("695x450")  # Cambiado el tamaño de la ventana
root.configure(bg="#f0f0f0")

style = ttk.Style()
style.configure("TButton", padding=(6, 3), font=("Helvetica", 12))  # Ajustado el tamaño de los botones

# Crear el frame principal
frame = ttk.Frame(root, padding="20", relief="raised", borderwidth=2)
frame.pack(expand=True, fill=tk.BOTH)

# Crear los widgets
label_titulo = ttk.Label(frame, text="FUNCION DE LOGUEOS", font=("Helvetica", 16, "bold"))  # Añadido el título y negrita
label_titulo.grid(row=0, column=0, columnspan=3, pady=(20, 10), padx=(250, 100), sticky="ewns")  # Centrado el título y aumentado el espacio vertical

label_entrada = ttk.Label(frame, text="Ruta de Entrada:", font=("Helvetica", 14))  # Aumento de tamaño de la letra
label_entrada.grid(row=1, column=0, sticky="w", padx=10, pady=(20, 5))  # Alineado a la izquierda y aumento del espacio vertical

ruta_entrada_entry = ttk.Entry(frame, width=40, font=("Helvetica", 12))  # Aumento de tamaño de la letra
ruta_entrada_entry.grid(row=1, column=1, padx=10, pady=(20, 5))

boton_seleccionar_archivo = ttk.Button(frame, text="Seleccionar archivo", command=seleccionar_archivo)
boton_seleccionar_archivo.grid(row=2, column=1, padx=(10, 5), pady=5)  # Ajuste de padding y espacio vertical

separator1 = ttk.Separator(frame, orient="horizontal")
separator1.grid(row=3, column=0, columnspan=6 , pady= 40, sticky="ewns")  # Línea separadora y espacio vertical

label_salida = ttk.Label(frame, text="Carpeta de Salida:", font=("Helvetica", 14))  # Aumento de tamaño de la letra
label_salida.grid(row=4, column=0, sticky="w", padx=10, pady=5)

ruta_salida_entry = ttk.Entry(frame, width=40, font=("Helvetica", 12))  # Aumento de tamaño de la letra
ruta_salida_entry.grid(row=4, column=1, padx=10, pady=5)

boton_seleccionar_carpeta = ttk.Button(frame, text="Seleccionar carpeta", command=seleccionar_carpeta_salida)
boton_seleccionar_carpeta.grid(row=5, column=1, padx=(10, 5), pady=5)  # Ajuste de padding y espacio vertical

boton_limpiar_guardar = ttk.Button(frame, text="Limpiar y Guardar", command=lambda: limpiar_y_guardar_archivo(ruta_entrada_entry.get(), ruta_salida_entry.get()))
boton_limpiar_guardar.grid(row=6, column=1, pady=(50, 40))  # Se aumenta el espacio vertical entre el botón y el final del frame

root.mainloop()
