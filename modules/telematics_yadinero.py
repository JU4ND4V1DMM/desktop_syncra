import csv
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

### Process with all developed functions 📊
def function_complete_telematics(path, output_directory, partitions, process_resource):
    
    print(f"🚀 Processing Telematics YaDinero with resource: {process_resource}")
    
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
    
    # Filter rows where profile columns contain unwanted values 🚫
    filter_conditions = [
        "Al dia",
        "Fallecido", 
        "Numero Errado",
        "Paz y Salvo",
        "Posible Fraude",
        "Reclamacion",
        "Ya Pago"
    ]
    
    # Create filter condition to exclude rows where any profile column contains these values
    Data_Frame = Data_Frame.filter(
        ~col("mejorperfil").isin(filter_conditions) &
        ~col("ultimoperfil").isin(filter_conditions) &
        ~col("mejorperfil_mes").isin(filter_conditions) &
        ~col("ultimoperfil_mes").isin(filter_conditions)
    )
    
    columns_filter = ["Identificacion", "ID_YaDinero", "nombrecompleto", "dias_de_mora", "saldo_actual",
        "valor_desembolsado", "fecha_desembolso", "valor_a_pagar", "Form_Moneda", "fechagestion", "fechapromesa",
        "valorpromesa", "mejorperfil", "ultimoperfil", "mejorperfil_mes", "ultimoperfil_mes", "valor_pago",
        "fecha_pago", "Dato_Contacto", "NOMBRE CORTO", "Hora_Envio", "Hora_Real", "Fecha_Hoy"]
    
    Data_Frame = Data_Frame.select(columns_filter)
    
    Save_Data_Frame(Data_Frame, output_directory, partitions, process_resource)
    
    return Data_Frame

### General Changes 🔧
def First_Changes_DataFrame(Root_Path):
    # Read the complete file with Spark 📖
    print("📖 Reading CSV with Spark...")
    Data_Root = spark.read.csv(Root_Path, header=False, sep=";")
    
    print(f"📊 Original DataFrame columns: {len(Data_Root.columns)}")
    
    # Delete column at position 51 (index 50) if it exists 🗑️
    if len(Data_Root.columns) > 50:
        print(f"🗑️ Removing column at position 51 (index 50)")
        
        # Create list of columns excluding the 51st
        columns_before_51 = Data_Root.columns[:50]  # columns 0-49
        columns_after_51 = Data_Root.columns[51:]   # columns 52 onward
        
        # Combine without column 51
        Data_Root = Data_Root.select(*(columns_before_51 + columns_after_51))
    
    print(f"📊 DataFrame columns after removal: {len(Data_Root.columns)}")
    
    # Now read the header to get the names 📝
    with open(Root_Path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        raw_header = next(reader)
    
    # Also delete column 51 from the header 🗑️
    if len(raw_header) > 50:
        raw_header = raw_header[:50] + raw_header[51:]
    
    # Rename duplicated column names
    seen = {}
    final_cols = []
    for colname in raw_header:
        if colname in seen:
            seen[colname] += 1
            final_cols.append(f"{colname}_{seen[colname]}")
        else:
            seen[colname] = 0
            final_cols.append(colname)
    
    # Verify that the numbers match 🔍
    if len(Data_Root.columns) != len(final_cols):
        print(f"⚙️ Adjusting: DataFrame has {len(Data_Root.columns)} cols, "
              f"header has {len(final_cols)} names")
        # Take the minimum
        min_len = min(len(Data_Root.columns), len(final_cols))
        Data_Root = Data_Root.select(*Data_Root.columns[:min_len])
        final_cols = final_cols[:min_len]
    
    # Assign names
    Data_Root = Data_Root.toDF(*final_cols)
    
    # Cast all columns to StringType
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    
    return DF

### Clean special characters in the account column 🧹
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Renaming columns ✏️
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "ID_YaDinero")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")

    return Data_Frame

### Process to save the RDD 💾
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions, resource):

    Type_File = f"BD Ya Dinero {resource}"
    delimiter = ";"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, partitions, delimiter)

    return Data_Frame

### Dynamization of phone columns 📱
def Phone_Data(Data_):

    print("🔢 First count:", Data_.count())

    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 5)]
    columns_to_stack_min = ["numeromarcado", "celular", "segundo_celular", "celular_1"]
    all_columns_to_stack = columns_to_stack_min #+ columns_to_stack_celular + columns_to_stack_fijo
    columns_to_drop_contact = all_columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")

    print("🔢 Second count:", Data_.count())
    
    return Stacked_Data_Frame

def Email_Data(Data_):

    columns_to_stack = [f"email{i}" for i in range(1, 6)]
    column_new = ["correo", "email"]
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

    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    Price_Col = "valor_a_pagar"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Contacts_Min)

    Data_ = Data_.withColumn("Rango", \
            when((col("valor_a_pagar") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("valor_a_pagar") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("valor_a_pagar") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("valor_a_pagar") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("valor_a_pagar") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("valor_a_pagar") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("valor_a_pagar") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("valor_a_pagar") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("valor_a_pagar") <= 2000000), lit("9 Entre 1 a 2 millones")) \
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

    print("🔢 Third count:", Data_.count())
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    print("🔢 Fourth count:", Data_.count())
    
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

    Data_ = Data_.withColumn("NOMBRE CORTO", regexp_replace(col("NOMBRE CORTO"), "[^A-Z& ]", ""))
    
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
    
    print("🔢 Fifth count:", RDD.count())
    
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