from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QTableWidgetItem
import locale
import modules.BOT_Process
import modules.EMAIL_Process
import modules.IVR_Process
import modules.SMS_Process

class Process_Data_Tables(QtWidgets.QMainWindow):

    def __init__(self, list_data):
        super().__init__()

        self.list_data = list_data
        
        self.process_tables = uic.loadUi("C:/Users/juan_/Downloads/App_Recupera/gui/Tables.ui")
        self.process_tables.show()
        self.r_data = self.process_to_exec()
        self.exec_process()

    def exec_process(self):
        
        if self.r_data is not None:

            origins = ["ASCARD", "BSCS", "RR", "SGA"]
            brands = ["0", "30", "60", "90", "120", "150", "180", "210", "Prechurn", "Potencial", "Castigo"]
            table1__data = self.r_data.dropDuplicates(["cuenta"])

            for row in range(11):  
                for col in range(4):  

                    brand_filter = brands[row]
                    origin_filter = origins[col]

                    value = table1__data.filter((col("marca") == brand_filter) & (col("origen") == origin_filter)).count()
                    formatted_value = locale.format('%d', value, grouping=True)

                    item = QTableWidgetItem(f"{formatted_value}")
                    self.process_tables.tableWidget.setItem(row, col, item)
        else:
            pass

    def process_to_exec(self):
        
        r_data = None

        list_data = self.list_data

        widget_filter = "Tables"
        lenght_list = len(list_data)

        if lenght_list >= 8:

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

                r_data = modules.BOT_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                        value_min, value_max, widget_filter)

            elif Process == "EMAIL":

                r_data = modules.EMAIL_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, \
                                                        value_min, value_max, widget_filter)

            elif Process == "IVR":

                r_data = modules.IVR_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                        value_min, value_max, widget_filter)

            elif Process == "SMS":

                r_data = modules.SMS_Process.Function_Complete(file, root, partitions, brands, origins, date, benefits, contact, \
                                                        value_min, value_max, widget_filter)

            else:
                pass

        else:
            pass
    
        return r_data