import tkinter as tk
from tkinter import messagebox, ttk
from utils import SMS_Process, IVR_Process, EMAIL_Process, BOT_Process

### Proceso de mensajería
def process_sms_options(folder_path, file_path, partitions):
    
    options_window_sms = tk.Toplevel()
    options_window_sms.title("MENÚ DE MENSAJERÍA")

    intro_frame_sms = tk.LabelFrame(options_window_sms, text="", font=("Arial", 12, "bold"))
    intro_frame_sms.grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_intro_sms = tk.Label(intro_frame_sms, text="Este proceso es ejecutado de acuerdo a la estructura solicitada por el \n área de Inteligencia, para la lectura correcta de la información.", font=("Arial", 10, "italic"))
    label_intro_sms.pack()

    options_frame_sms = tk.LabelFrame(options_window_sms, text="Opciones - SMS", font=("Arial", 12, "bold"))
    options_frame_sms.grid(row=1, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_brand_sms = tk.Label(options_frame_sms, text="Selecciona la marca:")
    label_brand_sms.grid(row=0, column=0, padx=10, pady=5)

    brands_sms = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_combobox_sms = ttk.Combobox(options_frame_sms, values=brands_sms)
    brand_combobox_sms.grid(row=0, column=1, padx=10, pady=5, columnspan=2)

    label_type_sms = tk.Label(options_frame_sms, text="Selecciona el tipo:")
    label_type_sms.grid(row=1, column=0, padx=10, pady=5)

    type_var_sms = tk.StringVar()
    type_var_sms.set("FLASH") 
    flash_radio_sms = tk.Radiobutton(options_frame_sms, text="FLASH", variable=type_var_sms, value="FLASH")
    flash_radio_sms.grid(row=1, column=1, padx=10, pady=5)

    normal_radio_sms = tk.Radiobutton(options_frame_sms, text="NORMAL", variable=type_var_sms, value="NORMAL")
    normal_radio_sms.grid(row=1, column=2, padx=10, pady=5)

    legend_frame_sms = tk.LabelFrame(options_window_sms, text="NOTA", font=("Arial", 12, "bold"))
    legend_frame_sms.grid(row=2, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_legend_sms = tk.Label(legend_frame_sms, text="La transformación de los datos ha sido realizada \nde acuerdo al valor registrado sobre la cuenta. Por favor \nvalidar descuentos aplicados en Control Next.", font=("Arial", 10))
    label_legend_sms.pack()

    options_window_sms.geometry("")
    options_window_sms.update_idletasks()
    options_window_sms.geometry(f"{options_window_sms.winfo_reqwidth()}x{options_window_sms.winfo_reqheight()}")

    def on_process_button_click_SMS():
    
        selected_brand_sms = brand_combobox_sms.get()
        selected_brand_sms = str(selected_brand_sms)
        selected_type_sms = type_var_sms.get()

        if not folder_path or not file_path or not selected_brand_sms or not selected_type_sms:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return
        
        print("Directorio antes de guardar SMS:", folder_path)
        SMS_Process.Function_Complete(file_path, selected_brand_sms, folder_path, selected_type_sms, partitions)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")
            
    
    process_1_button_sms = tk.Button(options_window_sms, text="Procesar", command=on_process_button_click_SMS)
    process_1_button_sms.grid(row=3, column=0, columnspan=3, pady=10)

    options_window_sms.geometry("")
    options_window_sms.update_idletasks()
    options_window_sms.geometry(f"{options_window_sms.winfo_reqwidth()}x{options_window_sms.winfo_reqheight()}")


### Proceso de EMAIL
def process_email_options(folder_path, file_path, partitions):
    
    options_window_email = tk.Toplevel()
    options_window_email.title("MENÚ DE CORREOS")

    intro_frame_email = tk.LabelFrame(options_window_email, text="", font=("Arial", 12, "bold"))
    intro_frame_email.grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_intro_email = tk.Label(intro_frame_email, text="Este proceso es ejecutado de acuerdo a la estructura solicitada por el \n área de Inteligencia, para la lectura correcta de la información.", font=("Arial", 10, "italic"))
    label_intro_email.pack()

    options_frame_email = tk.LabelFrame(options_window_email, text="Opciones - Correos", font=("Arial", 12, "bold"))
    options_frame_email.grid(row=1, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_brand_email = tk.Label(options_frame_email, text="Selecciona la marca:")
    label_brand_email.grid(row=0, column=0, padx=10, pady=5)

    brands_email = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_combobox_email = ttk.Combobox(options_frame_email, values=brands_email)
    brand_combobox_email.grid(row=0, column=1, padx=10, pady=5, columnspan=2)

    options_window_email.geometry("")
    options_window_email.update_idletasks()
    options_window_email.geometry(f"{options_window_email.winfo_reqwidth()}x{options_window_email.winfo_reqheight()}")

    def on_process_button_click_email():
    
        selected_brand_email = brand_combobox_email.get()
        selected_brand_email = str(selected_brand_email)

        if not folder_path or not file_path or not selected_brand_email:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return
        
        print("Directorio antes de guardar Correos:", folder_path)
        EMAIL_Process.Function_Complete(file_path, selected_brand_email, folder_path, partitions)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")
            
    
    process_email_button = tk.Button(options_window_email, text="Procesar", command=on_process_button_click_email)
    process_email_button.grid(row=3, column=0, columnspan=3, pady=10)

    options_window_email.geometry("")
    options_window_email.update_idletasks()
    options_window_email.geometry(f"{options_window_email.winfo_reqwidth()}x{options_window_email.winfo_reqheight()}")

### Proceso de IVR
def process_ivr_options(folder_path, file_path, partitions):

    options_window_ivr = tk.Toplevel()
    options_window_ivr.title("MENÚ DEL IVR")

    main_frame_ivr = tk.Frame(options_window_ivr)
    main_frame_ivr.grid(row=0, column=0, padx=10, pady=10)

    options_frame_ivr = tk.LabelFrame(main_frame_ivr, text="Opciones - IVR", font=("Arial", 12, "bold"))
    options_frame_ivr.grid(row=0, column=0, columnspan=2, padx=10, pady=(10, 15))

    label_brands_ivr = tk.Label(options_frame_ivr, text="Selecciona las marcas necesarias:")
    label_brands_ivr.grid(row=0, column=0, padx=10, pady=5)

    brands_ivr = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_vars_ivr = [tk.IntVar() for _ in brands_ivr]
    brand_checkbuttons_ivr = [tk.Checkbutton(options_frame_ivr, text=brand, variable=var) for brand, var in zip(brands_ivr, brand_vars_ivr)]
    for i, checkbutton in enumerate(brand_checkbuttons_ivr):
        checkbutton.grid(row=i + 1, column=0, padx=10, pady=5, sticky="w")

    label_origins_ivr = tk.Label(options_frame_ivr, text="Selecciona los orígenes:")
    label_origins_ivr.grid(row=0, column=1, padx=10, pady=5)

    origins_ivr = ["BSCS", "RR", "SGA", "ASCARD"]
    origin_vars_ivr = [tk.IntVar() for _ in origins_ivr]
    origin_checkbuttons_ivr = [tk.Checkbutton(options_frame_ivr, text=origin, variable=var) for origin, var in zip(origins_ivr, origin_vars_ivr)]
    for i, checkbutton in enumerate(origin_checkbuttons_ivr):
        checkbutton.grid(row=i + 1, column=1, padx=10, pady=5, sticky="w")

    legend_frame_ivr = tk.LabelFrame(main_frame_ivr, text="")
    legend_frame_ivr.grid(row=1, column=0, columnspan=2, padx=10, pady=(10, 15))

    label_legend_ivr = tk.Label(legend_frame_ivr, text="La transformación de los datos ha sido realizada \nde acuerdo a la estructura solicitada en VICIDIAL.", font=("Arial", 10))
    label_legend_ivr.pack()

    def on_process_button_click_IVR():
        selected_brands_ivr = [brands_ivr[i] for i, var in enumerate(brand_vars_ivr) if var.get() == 1]
        selected_origins_ivr = [origins_ivr[i] for i, var in enumerate(origin_vars_ivr) if var.get() == 1]

        if not folder_path or not file_path or not selected_brands_ivr or not selected_origins_ivr:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return

        print("Directorio antes de guardar IVR:", folder_path)
        IVR_Process.Function_Complete(file_path, folder_path, partitions, selected_brands_ivr, selected_origins_ivr)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")

        # Muestra las opciones seleccionadas
        etiqueta_seleccion_ivr = tk.Label(options_window_ivr, text="")
        etiqueta_seleccion_ivr.pack()

    process_2_button_ivr = tk.Button(options_window_ivr, text="Procesar", command=on_process_button_click_IVR)
    process_2_button_ivr.grid(row=1, column=0, pady=10)

    options_window_ivr.geometry("")
    options_window_ivr.update_idletasks()
    options_window_ivr.geometry(f"{options_window_ivr.winfo_reqwidth()}x{options_window_ivr.winfo_reqheight()}")


### Proceso de BOT
def process_bot_options(folder_path, file_path, partitions):
    
    options_window_bot = tk.Toplevel()
    options_window_bot.title("MENÚ DE BOT 2")

    intro_frame_bot = tk.LabelFrame(options_window_bot, text="", font=("Arial", 12, "bold"))
    intro_frame_bot.grid(row=0, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_intro_bot = tk.Label(intro_frame_bot, text="Este proceso es ejecutado de acuerdo a la estructura solicitada por el \n proveedor del servicio, para la lectura correcta de la información.", font=("Arial", 10, "italic"))
    label_intro_bot.pack()

    options_frame_bot = tk.LabelFrame(options_window_bot, text="Opciones - BOT", font=("Arial", 12, "bold"))
    options_frame_bot.grid(row=1, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_brand_bot = tk.Label(options_frame_bot, text="Selecciona la marca:")
    label_brand_bot.grid(row=0, column=0, padx=10, pady=5)

    brands_bot = ["0", "30", "60", "potencial", "prechurn", "castigo"]
    brand_combobox_bot = ttk.Combobox(options_frame_bot, values=brands_bot)
    brand_combobox_bot.grid(row=0, column=1, padx=10, pady=5, columnspan=2)

    legend_frame_bot = tk.LabelFrame(options_window_bot, text="NOTA", font=("Arial", 12, "bold"))
    legend_frame_bot.grid(row=2, column=0, columnspan=3, padx=10, pady=(10, 15))

    label_legend_bot = tk.Label(legend_frame_bot, text="La transformación de los datos ha sido realizada \nde acuerdo al valor registrado sobre la cuenta. Por favor \nvalidar descuentos aplicados en Control Next.", font=("Arial", 10))
    label_legend_bot.pack()

    options_window_bot.geometry("")
    options_window_bot.update_idletasks()
    options_window_bot.geometry(f"{options_window_bot.winfo_reqwidth()}x{options_window_bot.winfo_reqheight()}")

    def on_process_button_click_BOT():
    
        selected_brand_bot = brand_combobox_bot.get()
        selected_brand_bot = str(selected_brand_bot)

        if not folder_path or not file_path or not selected_brand_bot:
            tk.messagebox.showwarning(title="Error", message="Por favor, complete todos los campos antes de proceder.")
            return
        
        print("Directorio antes de guardar BOT:", folder_path)
        BOT_Process.Function_Complete(file_path, selected_brand_bot, folder_path, partitions)
        messagebox.showinfo("Éxito", "Archivo guardado con éxito.")
            
    
    process_1_button_bot = tk.Button(options_window_bot, text="Procesar", command=on_process_button_click_BOT)
    process_1_button_bot.grid(row=3, column=0, columnspan=3, pady=10)

    options_window_bot.geometry("")
    options_window_bot.update_idletasks()
    options_window_bot.geometry(f"{options_window_bot.winfo_reqwidth()}x{options_window_bot.winfo_reqheight()}")
