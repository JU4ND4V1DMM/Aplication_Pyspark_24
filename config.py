import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import SMS_Process

def process_sms_options(folder_path, file_path, partitions):
    options_window = tk.Toplevel()
    options_window.title("MENÚ DE MENSAJERÍA")

    intro_frame = tk.LabelFrame(options_window, text="", font=("Arial", 12, "bold"))
    intro_frame.grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_intro = tk.Label(intro_frame, text="Este proceso es ejecutado de acuerdo a la estructura solicitada por el \n área de Intelgiencia, para la lectura correcta de la información.", font=("Arial", 10, "italic"))
    label_intro.pack()

    options_frame = tk.LabelFrame(options_window, text="Opciones", font=("Arial", 12, "bold"))
    options_frame.grid(row=1, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_brand = tk.Label(options_frame, text="Selecciona la marca:")
    label_brand.grid(row=0, column=0, padx=10, pady=5)

    brands = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_combobox = ttk.Combobox(options_frame, values=brands)
    brand_combobox.grid(row=0, column=1, padx=10, pady=5, columnspan=2)

    label_type = tk.Label(options_frame, text="Selecciona el tipo:")
    label_type.grid(row=1, column=0, padx=10, pady=5)

    type_var = tk.StringVar()
    type_var.set("FLASH") 
    flash_radio = tk.Radiobutton(options_frame, text="FLASH", variable=type_var, value="FLASH")
    flash_radio.grid(row=1, column=1, padx=10, pady=5)

    normal_radio = tk.Radiobutton(options_frame, text="NORMAL", variable=type_var, value="NORMAL")
    normal_radio.grid(row=1, column=2, padx=10, pady=5)

    legend_frame = tk.LabelFrame(options_window, text="NOTA", font=("Arial", 12, "bold"))
    legend_frame.grid(row=2, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_legend = tk.Label(legend_frame, text="La transformación de los datos ha sido realizada \nde acuerdo al valor registrado sobre la cuenta, en caso de tener \nun descuento aplicado, realice la respectiva regla de 3 sobre la \ncolumna correspondiente.", font=("Arial", 10))
    label_legend.pack()

    close_button = tk.Button(options_window, text="Cerrar", command=options_window.destroy)
    close_button.grid(row=3, column=0, columnspan=3, pady=10)

    options_window.geometry("")
    options_window.update_idletasks()
    options_window.geometry(f"{options_window.winfo_reqwidth()}x{options_window.winfo_reqheight()}")

    def on_process_button_click():
    
        selected_brand = brand_combobox.get()
        selected_type = type_var.get()

        if not folder_path or not file_path or not selected_brand or not selected_type:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return

        result = SMS_Process.Function_Complete(file_path, selected_brand, folder_path, selected_type)
        
        if result:
            def show_success_message():
                messagebox.showinfo("Éxito", "Archivo guardado con éxito.")
            
            show_success_message()
    
    
    
    process_button = tk.Button(options_window, text="Procesar", command=on_process_button_click)
    process_button.grid(row=3, column=0, columnspan=3, pady=10)

    options_window.geometry("")
    options_window.update_idletasks()
    options_window.geometry(f"{options_window.winfo_reqwidth()}x{options_window.winfo_reqheight()}")