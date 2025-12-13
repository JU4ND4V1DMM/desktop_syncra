import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv 

spark = get_spark_session()

sqlContext = SQLContext(spark)

### Proceso con todas las funciones desarrolladas
def function_complete_telematics(path, output_directory, partitions, process_resource):
    
    print(f"Processing Telematics Puntored with resource: {process_resource}")
    
    Data_Frame = First_Changes_DataFrame(path)
    if process_resource == "EMAIL":
        Data_Frame = Email_Data(Data_Frame)
        Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="Correo")
    else:
        Data_Frame = Phone_Data(Data_Frame)
        if process_resource == "SMS":
            Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="Celular")
        elif process_resource == "BOT":
            Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="NA")
        elif process_resource == "IVR":
            Data_Frame = conversion_process(Data_Frame, output_directory, partitions, Contacts_Min="NA")
    
    Save_Data_Frame(Data_Frame, output_directory, partitions, process_resource)
    
    return Data_Frame


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    
    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("id_puntored", "ID_Puntored")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("canal_venta", "Canal")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, resource):

    Type_File = f"BD Puntored {resource}"
    delimiter = ";"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, partitions, delimiter)

    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 5)]
    columns_to_stack_min = ["numeromarcado"]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

def Email_Data(Data_):

    columns_to_stack = [f"email{i}" for i in range(1, 6)]
    column_new = ["email"]
    columns_to_drop = columns_to_stack #+ column_new
    Stacked_Data_Frame = Data_.select("*", *columns_to_drop)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_drop)}, {', '.join(columns_to_drop)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

def conversion_process (Data_Frame, output_directory, partitions, Contacts_Min):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"
    
    Data_ = Data_Frame

    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("id_puntored"), lit("-"), col("Dato_Contacto")))

    Price_Col = "saldo_inicial"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Contacts_Min)

    Data_ = Data_.withColumn("Rango", \
            when((col("saldo_inicial") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("saldo_inicial") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("saldo_inicial") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("saldo_inicial") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("saldo_inicial") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("saldo_inicial") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("saldo_inicial") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("saldo_inicial") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("saldo_inicial") <= 2000000), lit("9 Entre 1 a 2 millones")) \
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


    # Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "fecha_asignacion", "marca", \
    #                      "origen", f"{Price_Col}", "customer_type_id", "Form_Moneda", "nombrecompleto", \
    #                     "Rango", "referencia", "Dato_Contacto", "Hora_Envio", "Hora_Real", \
    #                     "Fecha_Hoy", "marca2", "descuento", "DEUDA_REAL", "fecha_vencimiento", "PRODUCTO", \
    #                     "fechapromesa", "tipo_pago", "mejorperfil_mes")
    
    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_ingreso")))

    Data_ = Data_.withColumn("NOMBRE CORTO", upper(col("nombrecompleto")))
    Data_ = change_name_column(Data_, "NOMBRE CORTO")
    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))
    
    print(Data_["NOMBRE CORTO"].dtype)

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))
                    
    Data_ = Data_.withColumn("NOMBRE CORTO",  when(length(col("Name_0")) > 2, col("Name_0"))
                             .when(length(col("Name_1")) > 2, col("Name_1"))
                             .when(length(col("Name_2")) > 2, col("Name_2"))
                             .when(length(col("Name_3")) > 2, col("Name_3"))
                             .otherwise(col("Name_1")))

    Data_ = Renamed_Column(Data_)
    
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