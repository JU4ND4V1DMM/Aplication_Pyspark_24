import os

def partition_DATA(ruta_archivo, root_file, partitions):
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as origin_file:
            rows = origin_file.readlines()

            rows_por_particion = len(rows) // partitions

            for i in range(partitions):
                begining = i * rows_por_particion
                end = (i + 1) * rows_por_particion if i < partitions - 1 else len(rows)

                nombre_particion = os.path.join(root_file, f"parte{i+1}.csv")

                with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                    file_output.writelines(rows[begining:end])

            if end < len(rows):
                nombre_particion = os.path.join(root_file, f"File_Part_{partitions+1}.csv")
                with open(nombre_particion, 'w', encoding='utf-8') as file_output:
                    file_output.writelines(rows[end:])

    except FileNotFoundError:
        print("El archivo especificado no fue encontrado.")
    except IOError:
        print("Error de entrada/salida al manipular el archivo.")

File_Name = "reporte_clientes2_21_03_2024_0830_Castigo"
path = f'C:/Users/c.operativo/Downloads/Dev_/Código/Phyton/Data/{File_Name}.csv'
output_directory = 'C:/Users/c.operativo/Downloads/Dev_/Código/Phyton/Results/'
partitions = 2

partition_DATA(path, output_directory, partitions)