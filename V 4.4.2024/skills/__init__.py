from pyspark.sql.functions import col, concat, lit, when

def generate_sms_message_column(data_frame):
    
    data_frame = data_frame.withColumn("SMS_0", lit(""))

    
    data_frame = data_frame.withColumn("SMS_1",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Tus servicios Claro "), col("PRODUCTO"),lit(" Ref "), col("Referencia"), lit( " por "), col("Form_Moneda"), \
               lit(" estan proximos a ser suspendidos. Evita suspension. Si pagaste, omite este msj AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Tu Equipo financiado Claro con Ref "), col("Referencia"), \
                          lit(" por "), col("Form_Moneda"), lit(" se encuentra proximo a ser inhabilitado. Si pagaste, omite este msj AplicaTyC(15)"))))
    

    ## SMS_2

    data_frame = data_frame.withColumn("SMS_2",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Debido a la mora en tu factura Claro "),col("PRODUCTO"), lit(" Ref "),col("Referencia"), lit(" por "), \
               col("Form_Moneda"), lit(" podras ser reportado negativamente. Paga de inmediato. Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit ("Debido a la mora en la factura de tu Equipo Claro Ref " ), col("Referencia"), lit(" por "), \
                          col("Form_Moneda"), lit(" podras ser reportado negativamente. Paga de inmediato. Si pagaste omite msj. AplicaTyC(15)"))))
    

     ## SMS_3

    data_frame = data_frame.withColumn("SMS_3",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Conserva tu buen habito de pago pagando hoy tu factura "), col ("PRODUCTO"), lit("  Claro Ref "), \
               col("Referencia"), lit (" "), col("Form_Moneda"), lit(" Si pagaste omite msj. Inf __WHATSAPP__ AplicanTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Conserva tu buen habito de pago pagando hoy la factura de tu Equipo Claro Ref "), col("Referencia"), lit (" "), \
                          col("Form_Moneda"), lit(" Si pagaste omite msj. Inf __WHATSAPP__ AplicanTyC(15)"))))
    



    ## SMS_4

    data_frame = data_frame.withColumn("SMS_4",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Paga hoy tu factura Claro "),col("PRODUCTO"), lit (" Ref "), col("Referencia"), lit (" "), col("Form_Moneda"), \
               lit("  y evita la suspension de tus servicios y cobro por reconexion. Si pagaste omite msj AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Paga hoy la factura de tu Equipo Claro  Financiado Ref "),col("Referencia"), lit (" "), col("Form_Moneda"), \
                          lit(", evita la inhabilitacion y cobros adicionales. Si pagaste omite msj AplicaTyC(15)"))))
    



     ##SMS_5

    data_frame = data_frame.withColumn("SMS_5",

        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("No arriesgues tu historial crediticio! Paga hoy la factura de tu Claro "),col("PRODUCTO"),lit(" con Ref "), \
               col("Referencia"),lit(" por "), col("Form_Moneda"),lit(" Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("No arriesgues tu historial crediticio! Paga hoy la factura de tu Equipo Claro con Ref "),col("Referencia"), \
                          lit(" por "),col("Form_Moneda"),lit(" Si pagaste omite msj. AplicaTyC(15)"))))

    

    ##SMS_6

    data_frame = data_frame.withColumn("SMS_6",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("El no pago oportuno de tu factura "), col("PRODUCTO"),lit(" Claro Ref "),col("Referencia"), lit (" "), col("Form_Moneda"), \
               lit(" genera la suspension de tus servicios. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("El no pago oportuno de tu factura del Equipo Claro Ref "),col("Referencia"), lit (" "), col("Form_Moneda"), \
                          lit(" genera la inhabilitación de tus dispositivo. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15)"))))




    ##SMS_7

    data_frame = data_frame.withColumn("SMS_7",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Aun no recibimos el pago de tu factura "),col("PRODUCTO"),lit(" Claro Ref "),col("Referencia"),lit(" por "), \
               col("Form_Moneda"),lit(" ingresa a App Mi Claro Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Aun no recibimos el pago de tu Equipo Claro Ref "),col("Referencia"),lit(" por "), \
                          col("Form_Moneda"),lit(" ingresa a App Mi Claro Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)"))))




        #SMS_8

    data_frame = data_frame.withColumn("SMS_8",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("La deuda de tu factura Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"),lit(" por "),col("Form_Moneda"), \
               lit(" se encuentra en etapa prejuridica. Paga hoy en Claro Pay. Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("La deuda de tu Equipo Claro Ref "),col("Referencia"),lit(" por "),col("Form_Moneda"), \
                          lit(" se encuentra en etapa prejuridica. Paga hoy en Claro Pay. Si pagaste omite msj. AplicaTyC(15)"))))



        #SMS_9

    data_frame = data_frame.withColumn("SMS_9",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("La factura Claro "),col("PRODUCTO"),lit(" presenta mora y podras ser reportado negativamente. Paga con Ref "), \
               col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("La factura de tu Equipo Claro presenta mora y podras ser reportado negativamente. Paga con Ref "), \
                          col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Si pagaste omite msj. AplicaTyC(15)"))))


        ##SMS_10

    data_frame = data_frame.withColumn("SMS_10",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("No dejes ningun saldo por pagar de la factura Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"),lit(" por "), \
               col("Form_Moneda"),lit(" Paga en App Mi Claro. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("No dejes ningun saldo por pagar de la factura de tu Equipo Claro Ref "),col("Referencia"),lit(" por "), \
                          col("Form_Moneda"),lit(" Paga en App Mi Claro. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15)"))))

        #SMS_11

    data_frame = data_frame.withColumn("SMS_11",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Dcto del "),col("DCTO"),lit("% en la factura Claro "),col("PRODUCTO"),lit(" con Ref "),col("Referencia"), lit(" Paga "), col("Form_Moneda"), lit(" Vigencia "), \
               col("Fecha_Envio"),lit(" Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Dcto del "),col("DCTO"),lit("% en la factura de tu Equipo Claro con Ref "),col("Referencia"), lit(" Paga "), col("Form_Moneda"), lit(" Vigencia "), \
               col("Fecha_Envio"),lit(" Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)"))))


        ##SMS_12

    data_frame = data_frame.withColumn("SMS_12",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Aun no vemos reflejado el pago de tu factura en mora Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"), \
               lit(" por "),col("Form_Moneda"),lit(" Recuerda que de no pagar hoy seras reportado negativamente en centrales de riesgo. Paga aquí bit.ly/3aULNAj. Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Aun no vemos reflejado el pago de tu Equipo Claro en mora Ref "),col("Referencia"), \
               lit(" por "),col("Form_Moneda"),lit(" Recuerda que de no pagar hoy seras reportado negativamente en centrales de riesgo. Paga aquí bit.ly/3aULNAj. Si pagaste omite msj. AplicaTyC(15)"))))


        ##SMS13
    
    data_frame = data_frame.withColumn("SMS_13",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Cuida tu historial Crediticio paga hoy la factura vencida Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"), \
               col("Form_Moneda"),lit(" evita reportes negativos en Centrales. Si pagaste omite msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Cuida tu historial Crediticio paga hoy la factura de tu Equipo Claro Ref "),col("Referencia"), \
               col("Form_Moneda"),lit(" evita reportes negativos en Centrales. Si pagaste omite msj. AplicaTyC(15)"))))


        ##SMS_14

    data_frame = data_frame.withColumn("SMS_14",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Paga de Inmediato tu factura en mora Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"),lit(" por "), \
               col("Form_Moneda"),lit(" Evita que tu cuenta pase a etapa de cobro prejuridico. Si pagaste omite msj. AplicaTyC(15")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Paga de Inmediato tu factura en mora del Equipo Claro Ref "),col("Referencia"),lit(" por "), \
                          col("Form_Moneda"),lit(" Evita que tu cuenta pase a etapa de cobro prejuridico. Si pagaste omite msj. AplicaTyC(15"))))



        ##SMS_15

    data_frame = data_frame.withColumn("SMS_15",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Aun no recibimos el pago de tu factura Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"),lit(" por "), \
               col("Form_Moneda"),lit(" evita posibles cobros juridicos. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Aun no recibimos el pago de de tu Equipo Claro Ref "),col("Referencia"),lit(" por "), \
                          col("Form_Moneda"),lit(" evita posibles cobros juridicos. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15"))))


        ##SMS_16

    data_frame = data_frame.withColumn("SMS_16",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Reactiva nuevamente los servicios Claro "),col("PRODUCTO"),lit(". Paga hoy con Ref "),lit("Referencia"), \
               lit(" por "),col("Form_Moneda"),lit(" evita cobros adicionales. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Reactiva nuevamente tu Equipo Claro. Paga hoy con Ref "),lit("Referencia"), \
                          lit(" por "),col("Form_Moneda"),lit(" evita cobros adicionales. Inf __WHATSAPP__ Si pagaste omite msj. AplicaTyC(15"))))


        ##SMS_17

    data_frame = data_frame.withColumn("SMS_17",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Conserva tu buen habito de pago pagando tu factura Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"), \
               lit(" por "),col("Form_Moneda"),lit(" en App Mi Claro. Inf __WHATSAPP__ Si pagaste omite msj. AplicanTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Conserva tu buen habito de pago pagando tu Equipo Claro Ref "),col("Referencia"), \
                          lit(" por "),col("Form_Moneda"),lit(" en App Mi Claro. Inf __WHATSAPP__ Si pagaste omite msj. AplicanTyC(15)"))))


        ##SMS_18

    data_frame = data_frame.withColumn("SMS_18",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Debido a la mora que presentas en tu factura Claro "),col("PRODUCTO"),lit(" Ref "),col("Referencia"), \
               lit(" por "),col("Form_Moneda"),lit(" podras ser reportado a centrales de riesgo. Si pagaste omite msj. AplicanTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("Debido a la mora que presentas con tu Equipo Claro Ref"),col("Referencia"), \
                          lit(" por "),col("Form_Moneda"),lit(" podras ser reportado a centrales de riesgo. Si pagaste omite msj. AplicanTyC(15)"))))


        ##SMS_19

    data_frame = data_frame.withColumn("SMS_19",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("La deuda de tu factura Claro "),col("PRODUCTO"),lit(" se encuentra en etapa prejuridica Paga con Ref "), \
               col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("La deuda de tu factura Claro Postpago se encuentra en etapa prejuridica Paga con Ref "), \
                          col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Inf __WHATSAPP__ Si pagaste omite msj AplicaTyC(15)"))))

       
        ##sms_20

    data_frame = data_frame.withColumn("SMS_20",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("Recuerda! La factura Claro "),col("PRODUCTO"),lit(" con Ref "),col("Referencia"),lit(" "), col("Form_Moneda"), \
               lit(" esta vencida, pagala de manera facil aqui bit.ly/3aULNAj. Si pagaste omite msj. AplicaTyC(15)")))


        ### ORIGEN ASCARD
        .otherwise(concat(lit("Recuerda! La factura de tu Equipo Claro con Ref "),col("Referencia"),lit(" "), col("Form_Moneda"), \
               lit(" esta vencida, pagala de manera facil aqui bit.ly/3aULNAj. Si pagaste omite msj. AplicaTyC(15)"))))

        
        ##SMS_21

    data_frame = data_frame.withColumn("SMS_21",
        when((col("CRM") == "BSCS") | (col("CRM") == "RR") | (col("CRM") == "SGA"), 
        
        ### ORIGEN RR - BSCS - SGA 
        concat(lit("No dejes ningun saldo pendiente por pagar de la factura Claro "),col("PRODUCTO"),lit(" Ref "), \
               col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Paga en App Mi Claro. Si pagaste, omite este msj. AplicaTyC(15)")))

        ### ORIGEN ASCARD
        .otherwise(concat(lit("No dejes ningun saldo pendiente por pagar de la factura Claro del Equipo Ref"), \
                          col("Referencia"),lit(" por "),col("Form_Moneda"),lit(" Paga en App Mi Claro. Si ya pagaste, omite este msj.  AplicaTyC(15)"))))
    

    return data_frame