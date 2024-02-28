import tkinter as tk
from tkinter import messagebox, ttk
from utils import SMS_Process, IVR_Process


### Proceso de mensajería
def process_sms_options(folder_path, file_path, partitions):
    options_window = tk.Toplevel()
    options_window.title("MENÚ DE MENSAJERÍA")

    intro_frame = tk.LabelFrame(options_window, text="", font=("Arial", 12, "bold"))
    intro_frame.grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_intro = tk.Label(intro_frame, text="Este proceso es ejecutado de acuerdo a la estructura solicitada por el \n área de Inteligencia, para la lectura correcta de la información.", font=("Arial", 10, "italic"))
    label_intro.pack()

    options_frame = tk.LabelFrame(options_window, text="Opciones - SMS", font=("Arial", 12, "bold"))
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

    options_window.geometry("")
    options_window.update_idletasks()
    options_window.geometry(f"{options_window.winfo_reqwidth()}x{options_window.winfo_reqheight()}")

    def on_process_button_click_SMS():
    
        selected_brand = brand_combobox.get()
        selected_brand = str(selected_brand)
        selected_type = type_var.get()

        if not folder_path or not file_path or not selected_brand or not selected_type:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return

        SMS_Process.Function_Complete(file_path, selected_brand, folder_path, selected_type, partitions)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")
            
    
    process_1_button = tk.Button(options_window, text="Procesar", command=on_process_button_click_SMS)
    process_1_button.grid(row=3, column=0, columnspan=3, pady=10)

    options_window.geometry("")
    options_window.update_idletasks()
    options_window.geometry(f"{options_window.winfo_reqwidth()}x{options_window.winfo_reqheight()}")


        
### Proceso de IVR
def process_ivr_options(folder_path, file_path, partitions):

    options_window = tk.Toplevel()
    options_window.title("MENÚ DEL IVR")

    main_frame = tk.Frame(options_window)
    main_frame.grid(row=0, column=0, padx=10, pady=10)

    options_frame = tk.LabelFrame(main_frame, text="Opciones - IVR", font=("Arial", 12, "bold"))
    options_frame.grid(row=0, column=0, columnspan=2, padx=10, pady=(10, 15))

    label_brands = tk.Label(options_frame, text="Selecciona las marcas necesarias:")
    label_brands.grid(row=0, column=0, padx=10, pady=5)

    brands = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_vars = [tk.IntVar() for _ in brands]
    brand_checkbuttons = [tk.Checkbutton(options_frame, text=brand, variable=var) for brand, var in zip(brands, brand_vars)]
    for i, checkbutton in enumerate(brand_checkbuttons):
        checkbutton.grid(row=i + 1, column=0, padx=10, pady=5, sticky="w")

    label_origins = tk.Label(options_frame, text="Selecciona los orígenes:")
    label_origins.grid(row=0, column=1, padx=10, pady=5)

    origins = ["BSCS", "RR", "SGA", "ASCARD"]
    origin_vars = [tk.IntVar() for _ in origins]
    origin_checkbuttons = [tk.Checkbutton(options_frame, text=origin, variable=var) for origin, var in zip(origins, origin_vars)]
    for i, checkbutton in enumerate(origin_checkbuttons):
        checkbutton.grid(row=i + 1, column=1, padx=10, pady=5, sticky="w")

    legend_frame = tk.LabelFrame(main_frame, text="")
    legend_frame.grid(row=1, column=0, columnspan=2, padx=10, pady=(10, 15))

    label_legend = tk.Label(legend_frame, text="La transformación de los datos ha sido realizada \nde acuerdo a la estructura solicitada en VICIDIAL.", font=("Arial", 10))
    label_legend.pack()

    def on_process_button_click_IVR():
        
        selected_brands = [brand.cget("text") for i, brand in enumerate(brand_checkbuttons) if brand_vars[i].get() == 1]
        selected_origins = [origin.cget("text") for i, origin in enumerate(origin_checkbuttons) if origin_vars[i].get() == 1]

        if not folder_path or not file_path or not selected_brands or not selected_origins:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return

        IVR_Process.Function_Complete(file_path, selected_brands, folder_path)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")

        # Muestra las opciones seleccionadas
        for brand in selected_brands:
            etiqueta_brand = tk.Label(options_window, text=f"Marca seleccionada: {brand}")
            etiqueta_brand.pack()

        for origin in selected_origins:
            etiqueta_origin = tk.Label(options_window, text=f"Origen seleccionado: {origin}")
            etiqueta_origin.pack()

    process_2_button = tk.Button(options_window, text="Procesar", command=on_process_button_click_IVR)
    process_2_button.grid(row=1, column=0, pady=10)
    
    options_window.geometry("")
    options_window.update_idletasks()
    options_window.geometry(f"{options_window.winfo_reqwidth()}x{options_window.winfo_reqheight()}")