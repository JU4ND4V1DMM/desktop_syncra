import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date, explode, array_join, when
from pyspark.sql.functions import trim, format_number, expr, coalesce, datediff, current_date
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

spark = None
sqlContext = None

def get_lazy_spark():
    global spark, sqlContext
    if spark is None:
        from pyspark.sql import SQLContext
        spark = get_spark_session()
        sqlContext = SQLContext(spark)
    return spark, sqlContext

def function_complete_telematics(path, output_directory, partitions, process_resource):
    get_lazy_spark()
    print(f"Processing Telematics Payjoy with resource: {process_resource}")
    
    Data_Frame = First_Changes_DataFrame(path)
    if process_resource == "EMAIL":
        Data_Frame = Email_Data(Data_Frame)
        Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="Correo")
    else:
        Data_Frame = Phone_Data(Data_Frame)
        if process_resource == "SMS":
            Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="Celular")
        elif process_resource == "BOT":
            Data_Frame = conversion_process_bot(Data_Frame, output_directory, partitions, Contacts_Min="NA")
        elif process_resource == "IVR":
            Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="NA")

    mejorperfil_filter = (col("mejorperfil") != "Fallecido") & (col("mejorperfil") != "Numero Errado") & (col("mejorperfil") != "Posible Fraude") & (col("mejorperfil") != "Reclamacion")
    ultimoperfil_filter = (col("ultimoperfil") != "Fallecido") & (col("ultimoperfil") != "Numero Errado") & (col("ultimoperfil") != "Posible Fraude") & (col("ultimoperfil") != "Reclamacion")
    #Data_Frame = Data_Frame.filter(col("valor_pago").isNull())

    Save_Data_Frame(Data_Frame, output_directory, partitions, process_resource)

    return Data_Frame

def First_Changes_DataFrame(Root_Path):
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    return DF

def change_character_account (Data_, Column):
    character_list = ["-"]
    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    return Data_

def Renamed_Column(Data_Frame):
    if "account" in Data_Frame.columns:
        Data_Frame = Data_Frame.withColumnRenamed("account", "ID_Payjoy")
    else:
        Data_Frame = Data_Frame.withColumnRenamed("cuenta", "ID_Payjoy")
    Data_Frame = Data_Frame.withColumnRenamed("identifitation", "Identificacion")
    if "monto_pago" in Data_Frame.columns:
        Data_Frame = Data_Frame.withColumnRenamed("monto_pago", "valor_cuota")
    return Data_Frame

def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, resource):
    Type_File = f"BD Payjoy {resource}"
    delimiter = ";"
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, partitions, delimiter)
    return Data_Frame

def Phone_Data(Data_):
    if "telefonos" not in Data_.columns and "celulares_agregados" in Data_.columns:
        column_array_phone = "celulares_agregados"
    else:
        column_array_phone = "telefonos"

    cleaned_df = Data_.withColumn(
        "telefonos_limpios",
        regexp_replace(column_array_phone, "\\[|\\]|\\{|\\}|\\s+|\\\"", "")
    )

    array_df = cleaned_df.withColumn(
        "telefonos_array",
        split("telefonos_limpios", "\\|")
    )

    exploded_df = array_df.withColumn("Dato_Contacto_1", explode("telefonos_array"))

    final_df = exploded_df.withColumn(
        "telefonos_array_string",
        array_join("telefonos_array", ", ")
    )

    final_df = final_df.drop(column_array_phone, "telefonos_limpios", "telefonos_array", "telefonos_array_string")

    column_new = ["Dato_Contacto_1"]
    columns_to_drop = column_new

    Stacked_Data_Frame = final_df.select("*", *columns_to_drop)

    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*",
        expr(f"stack({len(columns_to_drop)}, {', '.join(columns_to_drop)}) as Dato_Contacto")
    )

    final_df = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = final_df.select("*")

    return final_df

def Email_Data(Data_):
    cleaned_df = Data_.withColumn(
        "correos_limpios",
        regexp_replace("correos_agregados", r'\[|\]|\{|\}|\s+|\"', "")
    )

    exploded_df = cleaned_df.withColumn(
        "correos_array",
        split(col("correos_limpios"), r"\|")
    ).withColumn(
        "Dato_Contacto",
        explode(col("correos_array"))
    )
    
    final_df = exploded_df.withColumn(
        "Dato_Contacto",
        regexp_replace(
            col("Dato_Contacto"),
            r'\[|\]|\{|\}|\s+|\"|^\||\|$', 
            ""
        )
    ).filter(
        (col("Dato_Contacto") != "") &
        (col("Dato_Contacto").isNotNull()) 
    )

    final_df = final_df.drop("correos_array", "correos_limpios")
    
    return final_df

def conversion_process_bot(Data_Frame, output_directory, partitions, Contacts_Min):
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"
    
    Data_ = Data_Frame

    if "account" in Data_.columns:
        Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("account"), lit("-"), col("Dato_Contacto")))
    else:
        Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
        
    Price_Col = "pago_minimo"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Contacts_Min)

    Data_ = Data_.withColumn("Rango", \
            when((col("pago_minimo") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("pago_minimo") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("pago_minimo") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("pago_minimo") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("pago_minimo") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("pago_minimo") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("pago_minimo") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("pago_minimo") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("pago_minimo") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))

    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", 
                            regexp_replace(
                                concat(lit("$ "), format_number(col(Price_Col), 0)), 
                                ",", "."
                            ).cast("string"))
    
    if "etiqueta_dispositivo" in Data_.columns:
        Data_ = Data_.withColumnRenamed("etiqueta_dispositivo", "DEVICE_TAG")
    else:
        Data_ = Data_.withColumn("DEVICE_TAG", lit(""))
    
    Data_ = Data_.withColumn("LINK", 
                            concat(lit("https://pay.payjoy.com/p/device/"), col("DEVICE_TAG")))
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])
    
    Data_ = Data_.withColumn("now", current_date())
    
    if "fecha_ult_gestion" in Data_.columns:
        Data_ = Data_.withColumn("fecha_gestion_date",to_date(col("fecha_ult_gestion"), "yyyy-MM-dd HH:mm:ss.SSS"))
    else:
        Data_ = Data_.withColumn("fecha_gestion_date",to_date(col("fechagestion"), "yyyy-MM-dd HH:mm:ss.SSS"))
        
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_gestion_date")))

    if "nombre_cliente" in Data_.columns:
        Data_ = Data_.withColumn("NOMBRE CORTO", upper(col("nombre_cliente")))
        Data_ = change_name_column(Data_, "NOMBRE CORTO")
        name = "nombre_cliente"
    else:
        name = "nombrecompleto"
        Data_ = Data_.withColumn("NOMBRE CORTO", upper(col("nombrecompleto")))
        Data_ = change_name_column(Data_, "NOMBRE CORTO")
        
    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))
                    
    Data_ = Data_.withColumn("NOMBRE CORTO",  when(length(col("Name_0")) > 2, col("Name_0"))
                             .when(length(col("Name_1")) > 2, col("Name_1"))
                             .when(length(col("Name_2")) > 2, col("Name_2"))
                             .when(length(col("Name_3")) > 2, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Renamed_Column(Data_)
    
    Data_ = Data_.select(
        "Identificacion", 
        name, 
        "ID_Payjoy", 
        "bucket_dias_mora", 
        col(f"{Price_Col}").alias("pago_minimo"),
        "saldo_total", 
        "dias_mora", 
        "fabricante", 
        "tipo_base", 
        "ultimoperfil", 
        "mejorperfil", 
        "fecha_pago", 
        "fechagestion", 
        "Form_Moneda", 
        "NOMBRE CORTO", 
        "Dato_Contacto", 
        "DEVICE_TAG", 
        "LINK"
    )
    
    return Data_

def conversion_process (Data_Frame, output_directory, partitions, Contacts_Min):
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"
    
    Data_ = Data_Frame

    if "account" in Data_.columns:
        Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("account"), lit("-"), col("Dato_Contacto")))
    else:
        Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
        
    Price_Col = "pago_minimo"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Contacts_Min)

    Data_ = Data_.withColumn("Rango", \
            when((col("pago_minimo") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("pago_minimo") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("pago_minimo") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("pago_minimo") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("pago_minimo") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("pago_minimo") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("pago_minimo") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("pago_minimo") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("pago_minimo") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))

    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", 
                            regexp_replace(
                                concat(lit("$ "), format_number(col(Price_Col), 0)), 
                                ",", "."
                            ).cast("string"))
    
    Data_ = Data_.withColumn("Hora_Envio", lit(now.strftime("%H")))
    Data_ = Data_.withColumn("Hora_Real", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])
    
    Data_ = Data_.withColumn("now", current_date())
    
    if "fecha_ult_gestion" in Data_.columns:
        Data_ = Data_.withColumn("fecha_gestion_date",to_date(col("fecha_ult_gestion"), "yyyy-MM-dd HH:mm:ss.SSS"))
    else:
        Data_ = Data_.withColumn("fecha_gestion_date",to_date(col("fechagestion"), "yyyy-MM-dd HH:mm:ss.SSS"))
        
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_gestion_date")))

    if "nombre_cliente" in Data_.columns:
        Data_ = Data_.withColumn("NOMBRE CORTO", upper(col("nombre_cliente")))
        Data_ = change_name_column(Data_, "NOMBRE CORTO")
        name = "nombre_cliente"
    else:
        name = "nombrecompleto"
        Data_ = Data_.withColumn("NOMBRE CORTO", upper(col("nombrecompleto")))
        Data_ = change_name_column(Data_, "NOMBRE CORTO")
        
    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))
                    
    Data_ = Data_.withColumn("NOMBRE CORTO",  when(length(col("Name_0")) > 2, col("Name_0"))
                             .when(length(col("Name_1")) > 2, col("Name_1"))
                             .when(length(col("Name_2")) > 2, col("Name_2"))
                             .when(length(col("Name_3")) > 2, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Renamed_Column(Data_)
    
    Data_ = Data_.select("Identificacion", name, "ID_Payjoy", "bucket_dias_mora", f"{Price_Col}", \
                         "saldo_total", "valor_pago", "fabricante", "tipo_base", "ultimoperfil", "mejorperfil", "ultimoperfil_mes", "mejorperfil_mes", "fecha_pago", \
                         "fechagestion", "Form_Moneda", "NOMBRE CORTO", "Dato_Contacto", "Hora_Envio", "Hora_Real", "Fecha_Hoy", 
                         "etiqueta_dispositivo", "riesgo", "dias_mora", "grupo_tratamiento_2", "valor_cuota")
    
    return Data_

def Function_Filter(RDD, Contacts_Min):
    if Contacts_Min == "Celular":
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        RDD = Data_C
    elif Contacts_Min == "Fijo":
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_F
    elif Contacts_Min == "Correo":
        RDD = RDD.filter(col("Dato_Contacto").contains("@"))        
    else:
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_C.union(Data_F)
    return RDD

def change_name_column (Data_, Column):
    Data_ = Data_.withColumn(Column, upper(col(Column)))

    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))

    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",\
                    "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                     "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    
    Data_ = Data_.withColumn(Column, regexp_replace(Column, "[^A-Z& ]", ""))

    character_list = ["SEORES ","SEORA ","SEOR ","SEORA "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    Data_ = Data_.withColumn(Column,regexp_replace(col(Column), r'^(A\s+| )+', ''))
        
    return Data_