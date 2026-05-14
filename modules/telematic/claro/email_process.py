import os
import modules.filter_base as filter_base
import pyspark
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, expr, when, lower, length, to_date
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
 
spark = None
sqlContext = None
def get_lazy_spark():
    """Inicializa Spark solo si no existe una sesión previa."""
    global spark, sqlContext
    if spark is None:
        from pyspark.sql import SQLContext # Import local para optimizar
        spark = get_spark_session()
        sqlContext = SQLContext(spark)
    return spark, sqlContext

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions, Wallet_Brand, Origins_Filter, Dates, Benefits, Value_Min, Value_Max, widget_filter):

    get_lazy_spark()
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Email_Data(Data_Frame)
    Data_Frame = EMAIL_Proccess(Data_Frame, Wallet_Brand, output_directory, partitions, \
                                Origins_Filter, Dates, Benefits, Value_Min, Value_Max, widget_filter)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")
    DF = change_character_account(DF, "cuenta2")
    
    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    return Data_

### Limpieza de correos
def change_email (Data_):
    
    columns_to_check = ["Dato_Contacto"]
    list_email_replace  = ["notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda", "notengo", "noposee", \
                           "nosirve", "notien", "noutili", "nomanej", "nolegust", "nohay", "nocorreo", "noindic", \
                           "nohay", "@gamil", "pendienteconfirmar", "sincorreo", "pendienteporcrearclaro", "correo.claro" \
                           "crearclaro"#, ":", "|", "porcrear", "+", "#", "@xxx", "-"
                           ]

    for column in columns_to_check:
        Data_ = Data_.withColumn(column, lower(col(column)))
    
    Data_ = Data_.withColumn(
        "Dato_Contacto",
        when(col("Dato_Contacto").contains("@"), col("Dato_Contacto")).otherwise("0")
    )

    for column in columns_to_check:
        for pattern in list_email_replace:
            regex_pattern = f".*{pattern}.*"
            Data_ = Data_.withColumn(column, regexp_replace(col(column), regex_pattern, "0"))
            regex_pattern = f"^{pattern}.*"
            Data_ = Data_.withColumn(column, regexp_replace(col(column), regex_pattern, "0"))

    character_list = ["sin segmento"]

    for character in character_list:
        Data_ = Data_.withColumn("Dato_Contacto", regexp_replace(col("Dato_Contacto"), \
        character, "0"))

    character_list = ['#', '$', '/','<', '>', "\\*", "¡", "!", "\\?" "¿", "-", "}", "\\{", "\\+", " "]

    for character in character_list:
        Data_ = Data_.withColumn("Dato_Contacto", regexp_replace(col("Dato_Contacto"), character, ""))

    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "Cuenta_Next")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Identificacion")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "Cuenta")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
    Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM")
    Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "Saldo_Asignado")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "Nombre_Completo")
    Data_Frame = Data_Frame.withColumnRenamed("min", "Min")
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("Fecha_Hoy", "Fecha_Envio")
    Data_Frame = Data_Frame.withColumnRenamed("customer_type_id", "Segmento")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")
    Data_Frame = Data_Frame.withColumnRenamed("estado_ranking", "RANKING STATUS")
    Data_Frame = Data_Frame.withColumnRenamed("cant_servicios", "CANTIDAD SERVICIOS")
    
    Data_Frame = Data_Frame.withColumn("FILTRO GENERAL", 
        when((length(col("Nombre_Completo")) < 4) | (col("Canal") == "DELIMITAR") | (col("FILTRO_REFERENCIA") == "SIN REFERENCIA"), lit("Ajustar antes de envio"))
        .otherwise(lit("Limpio para envio")))
    
    Data_Frame = Order_Columns(Data_Frame)
    
    return Data_Frame

def Order_Columns(Data_Frame):

    Data_Frame = Data_Frame.withColumn(
            "CRM",
            when(col("CRM") == "BSCS", "Cuenta Postpago")
            .when(col("CRM") == "ASCARD", "Equipo Financiado")
            .when(col("CRM") == "RR", "Cuenta Hogar")
            .when(col("CRM") == "SGA", "Cuenta Negocios")
            .otherwise(col("CRM"))
        )

    Columns_Email = ["Canal", "Dato_Contacto", "descuento", \
                    "Edad_Mora", "CRM", "Form_Moneda", "Nombre_Completo", \
                    "Referencia", "Cuenta", "marca2", "plan", "identificacion", "tipo_pago", "FILTRO_REFERENCIA", "Rango", "RANKING STATUS", "CANTIDAD SERVICIOS","FILTRO GENERAL", "Tipo Base"]

    Data_Frame_final = Data_Frame.select(Columns_Email)

    return Data_Frame_final

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, filename, partitions):
    
    delimiter = ";"
    
    Directory_to_Save = f"{Directory_to_Save}---- Bases para TELEMATICA ----"
    
    save_to_csv(Data_Frame, Directory_to_Save, filename, partitions, delimiter)

    return Data_Frame

### Dinamización de columnas de correos
def Email_Data(Data_):

    Data_ = Data_.withColumn("email_prueba", lit("muestrasepareada@guanabana.com"))

    columns_to_stack = [f"email{i}" for i in range(1, 3)]
    column_new = ["email", "email_prueba"]
    columns_to_drop = columns_to_stack + column_new
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Proceso de mensajería
def EMAIL_Proccess (Data_, Wallet_Brand, Directory_to_Save, partitions, Origins_Filter, Dates, Benefits, Value_Min, Value_Max, widget_filter):

    Data_ = filter_base.Function_Complete(Data_)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"BD Claro EMAIL"

    Data_ = Data_.withColumn("fecha_vencimiento", to_date(col("fecha_vencimiento"), "dd/MM/yyyy"))

    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))
    Data_ = Data_.filter(col("origen").isin(Origins_Filter))
    
    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))
    
    Data_ = Data_.filter(~col("Dato_Contacto").isNull() & (col("Dato_Contacto") != "0") & (col("Dato_Contacto") != ""))
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    at_count = (length(regexp_replace(col("Dato_Contacto"), "[^@]", "")))
    Data_ = Data_.withColumn("Canal", when(at_count == 1, lit("EMAIL")).otherwise(lit("DELIMITAR")))
    
    Price_Col = "Mod_init_cta"

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    Data_ = Data_.withColumn("DEUDA_REAL", format_number(col("DEUDA_REAL"), 0)) 

    Data_ = Function_Filter(Data_, Dates, Benefits, Value_Min, Value_Max) 

    Data_ = Data_.withColumn("Rango", \
            when((col("Mod_init_cta") <= 20000), lit("1 Menos a 20 mil")) \
                .when((col("Mod_init_cta") <= 50000), lit("2 Entre 20 a 50 mil")) \
                .when((col("Mod_init_cta") <= 100000), lit("3 Entre 50 a 100 mil")) \
                .when((col("Mod_init_cta") <= 150000), lit("4 Entre 100 a 150 mil")) \
                .when((col("Mod_init_cta") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
                .when((col("Mod_init_cta") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
                .when((col("Mod_init_cta") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
                .when((col("Mod_init_cta") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
                .when((col("Mod_init_cta") <= 2000000), lit("9 Entre 1 a 2 millones")) \
                .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Data_ = Data_.withColumn(
        "descuento", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("0"))
        .otherwise(col("descuento")))                                                         

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
    Data_ = Data_.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.withColumn("Form_Moneda", concat(lit("$ "), format_number(col(Price_Col), 0)))
    
    Data_ = Data_.withColumn("Hora_Envio", lit(now.strftime("%H")))
    Data_ = Data_.withColumn("Hora_Real", lit(now.strftime("%H:%M")))
    Data_ = Data_.withColumn("Fecha_Hoy", lit(now.strftime("%d/%m/%Y")))

    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "fecha_asignacion", "marca", \
                         "origen", f"{Price_Col}", "customer_type_id", "Form_Moneda", "nombrecompleto", \
                        "Rango", "referencia", "min", "Dato_Contacto", "Canal", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "marca2", "descuento", "DEUDA_REAL", "tipo_pago", "fecha_vencimiento", "plan", "cant_servicios", "estado_ranking", "Tipo Base")

    Data_ = change_email(Data_)
    Data_ = Data_.filter(col("Dato_Contacto") != "0")
    Data_ = Data_.orderBy(["origen","Dato_Contacto",'marca',"Canal"])
    Data_ = Data_.withColumn("FILTRO_REFERENCIA",  when(col("referencia") != "", lit("Con referencia")).otherwise(lit("SIN REFERENCIA")))
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, Type_File, partitions)
    
    return Data_

def Function_Filter(RDD, Dates, Benefits, Value_Min, Value_Max):

    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))           
    #RDD = RDD.withColumn("Referencia",  when(col("Referencia") != "", col("Referencia")).otherwise(lit("SIN REFERENCIA")))
    
    RDD = RDD.filter(col("Mod_init_cta") >= Value_Min)
    RDD = RDD.filter(col("Mod_init_cta") <= Value_Max)

    RDD = RDD.withColumn(
        "DTO_Filter", 
        when((col("descuento") == "0%") | (col("descuento") == "0") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("Sin Descuento"))
        .otherwise(lit("Con Descuento")))
    
    if Benefits == "Con Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Con Descuento")

    elif Benefits == "Sin Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Sin Descuento")

    else:
        RDD = RDD

    return RDD