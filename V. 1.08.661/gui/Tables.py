from PyQt6 import QtWidgets, uic
from PyQt6.QtWidgets import QTableWidgetItem
import locale
import modules.BOT_Process
import modules.EMAIL_Process
import modules.IVR_Process
import modules.SMS_Process
import locale
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, coalesce

class Process_Data_Tables(QtWidgets.QMainWindow):

    def __init__(self, list_data):
        super().__init__()

        self.list_data = list_data
        
        self.process_tables = uic.loadUi("C:/winutils/cpd/gui/Tables.ui")
        self.process_tables.show()
        r_data = self.process_to_exec()
        self.exec_process(r_data)

    def exec_process(self, original_df):
        
        _1_data = original_df

        if original_df is not None:

            _1_data = _1_data.dropDuplicates(["Cuenta_Next"])
            origins = ["ASCARD", "BSCS", "RR", "SGA"]
            brands = ["0", "30", "60", "90", "120", "150", "180", "210", "prechurn", "potencial", "castigo"]

            count_dict = {(brand, origin): 0 for brand in brands for origin in origins}
            sum_dict = {(brand, origin): 0 for brand in brands for origin in origins}
            register_dict = {(origin, brand): 0 for origin in origins for brand in brands}

            total_count = 0
            total_debt = 0
            total_register = 0

            locale.setlocale(locale.LC_ALL, 'es_CO.UTF-8')

            for row in _1_data.collect():
                brand = row["Edad_Mora"]
                origin = row["CRM"]
                count_dict[(brand, origin)] += 1
                sum_dict[(brand, origin)] += row["DEUDA_REAL"]
                total_count += 1
                total_debt += row["DEUDA_REAL"]

            self.process_tables.Confirmation_LABEL_2.setText(locale.format_string('%d', total_count, grouping=True))

            formatted_total_debt = locale.currency(total_debt, grouping=True)
            formatted_total_debt = formatted_total_debt.rstrip('0').rstrip(',')
            self.process_tables.Confirmation_LABEL.setText(formatted_total_debt)

            for (brand, origin), count in count_dict.items():
                row_index = brands.index(brand)
                col_index = origins.index(origin)
                formatted_count = locale.format_string('%d', count, grouping=True) 
                item = QTableWidgetItem(formatted_count)
                self.process_tables.tableWidget.setItem(row_index, col_index, item)

            for (brand, origin), total_debt in sum_dict.items():
                row_index = brands.index(brand)
                col_index = origins.index(origin)
                formatted_debt = locale.currency(total_debt, grouping=True)
                formatted_debt = formatted_debt.rstrip('0').rstrip(',')
                item = QTableWidgetItem(formatted_debt)
                self.process_tables.tableWidget_2.setItem(row_index, col_index, item)

            for row in original_df.collect():
                brand = row["Edad_Mora"]
                origin = row["CRM"]
                register_dict[(origin, brand)] += 1
                total_register += 1

            for (origin, brand), register in register_dict.items():
                row_index = origins.index(origin)
                col_index = brands.index(brand)
                formatted_count = locale.format_string('%d', register, grouping=True) 
                item = QTableWidgetItem(formatted_count)
                self.process_tables.tableWidget_3.setItem(row_index, col_index, item)

            for brand_index, brand in enumerate(brands):
                total_register_brand = sum(register_dict[(origin, brand)] for origin in origins)
                formatted_total_register_brand = locale.format_string('%d', total_register_brand, grouping=True) 
                item = QTableWidgetItem(formatted_total_register_brand)
                self.process_tables.tableWidget_3.setItem(len(origins), brand_index, item)

            
            self.process_tables.Confirmation_LABEL_4.setText(locale.format_string('%d', total_register, grouping=True))

        else:
            self.process_tables.Confirmation_LABEL_2.setText("0")
            self.process_tables.Confirmation_LABEL.setText("$ 0")
            self.process_tables.Confirmation_LABEL_4.setText("0")

    def process_to_exec(self):

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