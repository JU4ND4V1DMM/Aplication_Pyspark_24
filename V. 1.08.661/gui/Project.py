from datetime import datetime
from PyQt6.QtCore import QDate
from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QMessageBox
import modules.BOT_Process
import modules.EMAIL_Process
import modules.IVR_Process
import modules.SMS_Process
from gui.Tables import Process_Data_Tables

class Process_Data(QtWidgets.QMainWindow):

    def __init__(self, row_count, file_path, folder_path, partitions):
        super().__init__()
        
        self.file_path = file_path
        self.folder_path = folder_path
        self.partitions = partitions
        self.process_data = uic.loadUi("C:/winutils/cpd/gui/Project.ui")
        self.process_data.label_Total_Registers_2.setText(row_count)
        self.process_data.show()
        self.exec_process()

    def exec_process(self):
        self.data_to_process = []
        self.process_data.pushButton_Process.clicked.connect(self.compilation_process)
        self.process_data.pushButton_Graphic.clicked.connect(self.data_tables)
    
    def data_tables(self):

        self.validation_data()
        lenght_list = len(self.data_to_process)

        if lenght_list >= 10:
            list_Tables = self.data_to_process
            self.Tables = Process_Data_Tables(list_Tables)
        else: 
            pass

    def validation_data(self):

        ### CheckBox
        list_CheckBox_Brands = []
        checkbox_names_brands = [
            "checkBox_Brand_0", "checkBox_Brand_30", "checkBox_Brand_60",
            "checkBox_Brand_90", "checkBox_Brand_120", "checkBox_Brand_150",
            "checkBox_Brand_180", "checkBox_Brand_210", "checkBox_Brand_Potencial",
            "checkBox_Brand_Prechurn", "checkBox_Brand_Castigo"
        ]
        for checkbox_name in checkbox_names_brands:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                if "Marca " in text:
                    text = text.replace("Marca ", "")
                list_CheckBox_Brands.append(text)
            
        list_CheckBox_Origins = []
        checkbox_names_origins = ["checkBox_Origin_ASCARD", "checkBox_Origin_BSCS", "checkBox_Origin_RR", "checkBox_Origin_SGA"]
        for checkbox_name in checkbox_names_origins:
            checkbox = getattr(self.process_data, checkbox_name, None)
            if checkbox is not None and checkbox.isChecked():
                text = checkbox.text()
                list_CheckBox_Origins.append(text)

        ### Calendar FLP
        Date_Selection = str(self.process_data.checkBox_ALL_DATES_FLP.isChecked())
        Calendar_Date = str(self.process_data.calendarWidget.selectedDate())
        Today = datetime.now().date()
        Today = str(QDate(Today.year, Today.month, Today.day))

        print(Date_Selection)
        print(type(Date_Selection))
        print(Calendar_Date)
        print(type(Calendar_Date))

        if Date_Selection == "True":
            Date_Selection_Filter = "All Dates"

        elif Calendar_Date == Today:
            Date_Selection_Filter = None

        else:
            Date_Selection_Filter = Calendar_Date
    
        ### Lists
        V_Benefits = self.process_data.comboBox_Benefits.currentText()
        V_Min_Contact = self.process_data.comboBox_Min_Contact.currentText()
        V_Selected_Process = self.process_data.comboBox_Selected_Process.currentText()

        if V_Selected_Process == "EMAIL" or V_Selected_Process == "SMS":
            V_Min_Contact_Filter = "Todos"

        list_empty = ["--- Seleccione opción"]
        validation_list_filters = [V_Benefits, V_Min_Contact, V_Selected_Process]

        ### Numeric Spaces
        V_Min_Price = self.process_data.lineEdit_Mod_Init_Min.text()
        V_Max_Price = self.process_data.lineEdit_Mod_Init_Max.text()

        ### Validation
        if len(list_CheckBox_Brands) == 0 or len(list_CheckBox_Origins) == 0:

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar al menos una opción de marca y origen.")
            Mbox_Incomplete.exec()

        elif Date_Selection_Filter is None:
            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Debe seleccionar al menos una fecha o elegir todas las FLP.")
            Mbox_Incomplete.exec()

        elif all(item in validation_list_filters for item in list_empty):
            
            if "--- Seleccione opción" == V_Selected_Process:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe elegir el tipo de proceso que desea realizar\npara generar el respectivo archivo.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Selected_Process.setFocus()

            elif "--- Seleccione opción" == V_Benefits:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de beneficios\npara realizar la transformación de los datos.")
                Mbox_Incomplete.exec()
                self.process_data.comboBox_Benefits.setFocus()
            
            else:
                Mbox_Incomplete = QMessageBox()
                Mbox_Incomplete.setWindowTitle("Error de procesamiento")
                Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
                Mbox_Incomplete.setText("Debe completar el campo de filtro de números\npara realizar la transformación de los datos.")
                Mbox_Incomplete.exec()    
                self.process_data.comboBox_Min_Contact.setFocus()
        
        elif V_Min_Price.strip() == "" or V_Max_Price.strip() == "":

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ingrese un valor numérico en el rango mínimo y máximo.")
            Mbox_Incomplete.exec()

            if V_Min_Price.strip() == "" and V_Max_Price.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min.setText("1")
                self.process_data.lineEdit_Mod_Init_Max.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Min.setFocus()
            elif V_Min_Price.strip() == "":
                self.process_data.lineEdit_Mod_Init_Min.setText("1")
                self.process_data.lineEdit_Mod_Init_Min.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Max.setText("999999999")
                self.process_data.lineEdit_Mod_Init_Max.setFocus()

        elif not (V_Min_Price.isdigit() and V_Max_Price.isdigit()):

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese solo NÚMEROS.")
            Mbox_Incomplete.exec()

            if not (V_Min_Price.isdigit() or V_Max_Price.isdigit()):
                self.process_data.lineEdit_Mod_Init_Max.setText("")
                self.process_data.lineEdit_Mod_Init_Min.setText("")
                self.process_data.lineEdit_Mod_Init_Max.setFocus()
            elif V_Min_Price.isdigit():
                self.process_data.lineEdit_Mod_Init_Max.setText("")
                self.process_data.lineEdit_Mod_Init_Max.setFocus()
            else:
                self.process_data.lineEdit_Mod_Init_Min.setText("")
                self.process_data.lineEdit_Mod_Init_Min.setFocus()

        elif V_Min_Price >= V_Max_Price:

            Mbox_Incomplete = QMessageBox()
            Mbox_Incomplete.setWindowTitle("Error de procesamiento")
            Mbox_Incomplete.setIcon(QMessageBox.Icon.Warning)
            Mbox_Incomplete.setText("Ha digitado valores inválidos, ingrese un rango coherente.")
            Mbox_Incomplete.exec()

            self.process_data.lineEdit_Mod_Init_Max.setText("")
            self.process_data.lineEdit_Mod_Init_Min.setText("")
            self.process_data.lineEdit_Mod_Init_Max.setFocus()

        else:
            
            V_Min_Contact_Filter = V_Min_Contact

            self.data_to_process = [list_CheckBox_Brands, list_CheckBox_Origins, Date_Selection_Filter, V_Benefits, V_Min_Contact_Filter, \
                                    V_Selected_Process, V_Min_Price, V_Max_Price]

            list_data = [self.file_path, self.folder_path, self.partitions]
            list_data.extend(self.data_to_process)
            self.data_to_process = list_data

    def compilation_process(self):
        
        self.validation_data()
        lenght_list = len(self.data_to_process)
        widget_filter = "Create_File"

        if lenght_list >= 8:        

            list_data = self.data_to_process

            Process = list_data[8]

            file = list_data[0]
            root = list_data[1]
            partitions = int(list_data[2])
            brands = list_data[3]
            brands = [brand_w.lower() for brand_w in brands]
            origins = list_data[4]
            date = list_data[5]
            benefits = list_data[6]
            contact = list_data[7]
            value_min = int(list_data[9])
            value_max = int(list_data[10])

            if Process == "BOT":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.BOT_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                      value_min, value_max, widget_filter)
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de BOTS ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process == "EMAIL":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.EMAIL_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, \
                                                        value_min, value_max, widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de EMAIL ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process == "IVR":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.IVR_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                      value_min, value_max, widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de IVR ejecutado exitosamente.")
                Mbox_In_Process.exec()

            elif Process == "SMS":

                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("Procesando")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Por favor espere la ventana de confirmación, mientras se procesa el archivo.")
                Mbox_In_Process.exec()

                modules.SMS_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                      value_min, value_max, widget_filter)
                
                Mbox_In_Process = QMessageBox()
                Mbox_In_Process.setWindowTitle("")
                Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
                Mbox_In_Process.setText("Proceso de SMS ejecutado exitosamente.")
                Mbox_In_Process.exec()

            else:
                pass

        else:
            pass