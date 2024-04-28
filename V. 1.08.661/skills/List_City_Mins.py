from pyspark.sql.functions import col, concat, lit, when, split, length, regexp_replace

def lines_inactives_df(data_frame):
    
       data_frame = data_frame.withColumnRenamed("22_", "LUGAR")

       Data_1 = data_frame.filter(col("dato") <= 3000000000)
       Data_2 = data_frame.filter(col("dato") >= 3599999999)
       Data_2 = Data_2.filter(col("dato") <= 6010000000)
       Data_3 = data_frame.filter(col("dato") >= 6089999999)

       data_frame = Data_1.union(Data_2)
       data_frame = data_frame.union(Data_3)

       data_frame = data_frame.withColumn("LUGAR", when((col("LUGAR") == "") | (col("LUGAR").isNull()), "BOGOTA")
                                   .otherwise(col("LUGAR")))

       data_frame = data_frame.withColumn("LUGAR", split(col("LUGAR"), "/").getItem(0))
       data_frame = data_frame.withColumn("dato", regexp_replace(col("dato"), " ", ""))
       data_frame = data_frame.withColumn("dato", regexp_replace(col("dato"), "  ", ""))
       data_frame = data_frame.withColumn("LARGO", length(col("dato")))
       data_frame = data_frame.withColumn("TELEFONO", col("dato"))
       data_frame = data_frame.withColumn("dato", lit(""))

       # 601
       list1 = ["BOGOTA"]
       # 602
       list2 = ["CAUCA", "NARIÑO", "VALLE"]
       # 604
       list3 = ["ANTIOQUIA", "CORDOBA", "CHOCO", "MEDELLÍN", "MEDELLIN"]
       # 605
       list4 = ["ATLANTICO", "BOLIVAR", "CESAR", "LA GUAJIRA", "MAGDALENA", "SUCRE"]
       # 606
       list5 = ["CALDAS", "QUINDIO", "RISARALDA"]
       # 607
       list6 = ["ARAUCA", "NORTE DE SANTANDER", "SANTANDER"]
       # 608
       list7 = ["AMAZONAS", "BOYACA", "CASANARE", "CAQUETA", "GUAVIARE", "GUAINIA", "HUILA", "META", "TOLIMA", "PUTUMAYO", \
              "SAN ANDRES", "VAUPES", "VICHADA"]

       data_frame = data_frame.withColumn("INDICATIVO",
       when(col("LUGAR").isin(list1), lit("1"))
       .when(col("LUGAR").isin(list2), lit("2"))
       .when(col("LUGAR").isin(list3), lit("4"))
       .when(col("LUGAR").isin(list4), lit("5"))
       .when(col("LUGAR").isin(list5), lit("6"))
       .when(col("LUGAR").isin(list6), lit("7"))
       .when(col("LUGAR").isin(list7), lit("8"))
       .otherwise("000"))

       data_frame = data_frame.filter(col("INDICATIVO") != "000")

       ### Arreglo de MINS
       data_frame = data_frame.withColumn("dato",
       when(col("LARGO") == 7, concat(lit("60"), col("INDICATIVO"), col("TELEFONO")))
       .when(col("LARGO") == 8, concat(lit("60"), col("TELEFONO")))
       .otherwise(""))

       data_frame = data_frame.filter(col("LARGO") >= 7)
       data_frame = data_frame.filter(col("LARGO") <= 8)

       data_frame = data_frame.select("1_", "2_", "ciudad", "depto", "dato", "tipodato")
    

       return data_frame