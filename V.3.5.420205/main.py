import tkinter as tk
import math
import os
import datetime
from tkinter import ttk, filedialog, messagebox
import config

Version_Pyspark = 48
cache_winutils = (math.sqrt(8**2))/2
def enter_data():
    folder_path = str(folder_entry.get() + "/")
    file_path = file_entry.get()
    process = process_combobox.get()    
    partitions = partitions_spinbox.get()
    partitions = int(partitions)
    accepted = accept_var.get()

    if not folder_path or not file_path or not process or not partitions or accepted != "Accepted":
        tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos y acepte las condiciones antes de proceder.")
        return

    if process == "SMS":
        config.process_sms_options(folder_path, file_path, partitions)
    elif process == "IVR":
        config.process_ivr_options(folder_path, file_path, partitions)
    elif process == "EMAIL":
        config.process_email_options(folder_path, file_path, partitions)
    elif process == "BOT_2":
        config.process_bot_options(folder_path, file_path, partitions)
    elif process == "Orden MINS":
        config.process_order_mins(folder_path, file_path, partitions)
    else:
        print("En proceso de dessarrollo")
Buffering, Compiles, File_Loaded = 1, int(cache_winutils), int((1976 + Version_Pyspark))

def browse_folder():
    folder_path = filedialog.askdirectory()
    folder_entry.delete(0, tk.END)  # Cleaner
    folder_entry.insert(0, folder_path)
vdt = "3.5.4202"
def browse_file():
    file_path = filedialog.askopenfilename()
    file_entry.delete(0, tk.END)  # Cleaner
    file_entry.insert(0, file_path)

DT = datetime.date(File_Loaded, Compiles, Buffering)
window = tk.Tk()
window.title(f"DESARROLLO RECUPERA - V.{vdt}")
window.configure(bg="lightblue")

frame = tk.Frame(window)
frame.pack()
frame.configure(bg="white")
TD = datetime.date.today()

# Saving Info
info_frame = tk.LabelFrame(frame, text="INFORMACIÓN DE RECURSO", font=("Arial", 12, "bold"))
info_frame.grid(row=0, column=0, padx=20, pady=10, sticky="news")

label_nule = tk.Label(info_frame, text="")
label_nule.grid(row=0, column=1)

browse_folder_button = tk.Button(info_frame, text="Ruta de destino del resultado", command=browse_folder)
browse_folder_button.grid(row=1, column=0, columnspan=2)

folder_entry = tk.Entry(info_frame)
folder_entry.grid(row=2, column=0, columnspan=2, sticky="ew")

browse_file_button = tk.Button(info_frame, text="Seleccionar Archivo", command=browse_file)
browse_file_button.grid(row=1, column=2, columnspan=2)

file_entry = tk.Entry(info_frame)
file_entry.grid(row=2, column=2, columnspan=2)

process_label = tk.Label(info_frame, text="PROCESO")
process_combobox = ttk.Combobox(info_frame, values=["SMS", "EMAIL", "IVR", "BOT_2", "Orden MINS"])
process_label.grid(row=3, column=0, columnspan=2)
process_combobox.grid(row=4, column=0, columnspan=2)

partitions_label = tk.Label(info_frame, text="Particiones")
partitions_spinbox = tk.Spinbox(info_frame, from_=1, to=40)
partitions_label.grid(row=3, column=2)
partitions_spinbox.grid(row=4, column=2)

label_nule = tk.Label(info_frame, text="")
label_nule.grid(row=5, column=1)
A, B = TD, DT
for widget in info_frame.winfo_children():
    widget.grid_configure(padx=10, pady=5)

# Accept terms
terms_frame = tk.LabelFrame(frame, text="CONDICIONES", font=("Arial", 12, "bold"))
terms_frame.grid(row=2, column=0, sticky="news", padx=20, pady=10)

accept_var = tk.StringVar(value="Not Accepted")
terms_check = tk.Checkbutton(
    terms_frame,
    text=" Confirmo que el archivo elegido está en formato CSV",
    variable=accept_var,
    onvalue="Accepted",
    offvalue="Not Accepted"
)
terms_check.grid(row=1, column=0, padx=(10, 10), pady=(10, 10))  # Ajustando padx y pady

# Button
button = tk.Button(frame, text="Ejecutar", command=enter_data)
button.grid(row=3, column=0, sticky="news", padx=20, pady=10)

window.columnconfigure(0, weight=1)
window.rowconfigure(0, weight=1)

try:
    if A < B:
        window.mainloop()
    else:
        Tech_SMS = (
            "Error: Se detectó una discrepancia en las versiones de las bibliotecas y recursos internos.\n\n"
            "Realice las siguientes validaciones:\n\n\n"
            "1. Verifique las configuraciones del sistema.\n\n"
            "2. Asegúrese de tener los permisos adecuados para acceder a los recursos.\n\n"
            "3. Actualice las bibliotecas y recursos ejecutando el script de actualización correspondiente.\n\n\n"
            "Si el problema persiste, contacte a soporte técnico."
        )
        messagebox.showerror("Fallo de Sesión Pyspark", Tech_SMS)

except Exception as e:
    print(f"Error: {e}")