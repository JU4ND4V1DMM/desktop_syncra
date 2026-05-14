import os
import modules.filter_base as filter_base
from skills import letters_sms
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date
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
def Function_Complete(path, output_directory, Partitions, Wallet_Brand, Origins_Filter, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):
    get_lazy_spark()
    Type_Proccess = "NORMAL"
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    Data_Frame = SMS_Proccess(Data_Frame, Wallet_Brand, output_directory, Type_Proccess, Partitions, Origins_Filter, \
                              Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)
    
    return Data_Frame


### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
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
    Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
    Data_Frame = Data_Frame.withColumnRenamed("Fecha_Hoy", "Fecha_Envio")
    Data_Frame = Data_Frame.withColumnRenamed("customer_type_id", "Segmento")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
    Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")
    Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")
    Data_Frame = Data_Frame.withColumnRenamed("mejorperfil_mes", "MEJOR PERFIL")
    Data_Frame = Data_Frame.withColumnRenamed("dias_transcurridos", "DIAS DE MORA")
    Data_Frame = Data_Frame.withColumnRenamed("estado_ranking", "RANKING STATUS")
    Data_Frame = Data_Frame.withColumnRenamed("cant_servicios", "CANTIDAD SERVICIOS")
    Data_Frame = Data_Frame.withColumnRenamed("Tipo Base", "TIPO BASE")

    Data_Frame = Data_Frame.select("Identificacion", "Cuenta_Next", "Cuenta", "Fecha_Asignacion", "Edad_Mora", \
                                   "CRM", "Saldo_Asignado", "Segmento",	"Form_Moneda", "Nombre_Completo", "Rango", \
                                    "Referencia", "Dato_Contacto", "Hora_Envio", "Hora_Real", \
                                    "Fecha_Envio", "marca2", "DCTO", "DEUDA_REAL", "FLP", "PRODUCTO", "fechapromesa", \
                                    "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA", "RANKING STATUS", "CANTIDAD SERVICIOS", \
                                    "NOMBRE CORTO", "TIPO BASE")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, Wallet_Brand, widget_filter):
        
    list_key = ["BD Claro SMS", "BD Claro SMS_TEXTO"]

    Directory_to_Save = f"{Directory_to_Save}---- Bases para TELEMATICA ----"
    
    for i in list_key:

        if i == "BD Claro SMS_TEXTO":
            Data_Frame = letters_sms.generate_sms_message_column(Data_Frame)
        else:
            Data_Frame = Data_Frame

        Type_File = f"{i}"
        delimiter = ";"
        
        save_to_csv(Data_Frame, Directory_to_Save, Type_File, Partitions, delimiter)
        
    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    columns_to_stack_min = ["min"]
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

### Proceso de mensajería
def SMS_Proccess (Data_Frame, Wallet_Brand, output_directory, Type_Proccess, Partitions, Origins_Filter, \
                              Dates, Benefits, Contacts_Min, Value_Min, Value_Max,  widget_filter):

    Data_Frame = filter_base.Function_Complete(Data_Frame)

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    Type_File = f"SMS__"
    
    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_Frame.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))
    Data_ = Data_.filter(col("origen").isin(Origins_Filter))

    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    Price_Col = "Mod_init_cta"     

    Data_ = Data_.withColumn(f"DEUDA_REAL", col(f"{Price_Col}").cast("double").cast("int"))
    
    Data_ = Function_Filter(Data_, Dates, Benefits, Contacts_Min, Value_Min, Value_Max)

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

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))
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

    Data_ = Data_.withColumn(
        "PRODUCTO",
        when(col("origen") == "BSCS", "Postpago")
        .when(col("origen") == "ASCARD", "Equipo")
        .when(col("origen") == "RR", "Hogar")
        .when(col("origen") == "SGA", "Negocios")
        .otherwise(col("origen"))
    )

    Data_ = Data_.select("identificacion", "cuenta", "cuenta2", "fecha_asignacion", "marca", \
                         "origen", f"{Price_Col}", "customer_type_id", "Form_Moneda", "nombrecompleto", \
                        "Rango", "referencia", "Dato_Contacto", "Hora_Envio", "Hora_Real", \
                        "Fecha_Hoy", "marca2", "descuento", "DEUDA_REAL", "fecha_vencimiento", "PRODUCTO", \
                        "fechapromesa", "tipo_pago", "mejorperfil_mes", "cant_servicios", "estado_ranking", "Tipo Base")
    
    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_vencimiento")))
    
    Data_ = Data_.withColumn("NOMBRE CORTO", col("nombrecompleto"))

    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))
    
    print(Data_["NOMBRE CORTO"].dtype)

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))
                    
    Data_ = Data_.withColumn("NOMBRE CORTO",  when(length(col("Name_0")) > 2, col("Name_0"))
                             .when(length(col("Name_1")) > 2, col("Name_1"))
                             .when(length(col("Name_2")) > 2, col("Name_2"))
                             .when(length(col("Name_3")) > 2, col("Name_3"))
                             .otherwise(col("Name_1")))
    
    Data_ = Data_.withColumn("FILTRO_REFERENCIA",  when(col("Referencia") != "", lit("Con referencia")).otherwise(lit("SIN REFERENCIA")))

    Data_ = Renamed_Column(Data_)
    #Data_ = Data_.withColumn(col("Form_Moneda"), concat(col("Form_Moneda")))
    #Data_ = Data_.withColumn(col("Form_Moneda"), regexp_replace(col("Form_Moneda"), "\\,", "."))
    Save_Data_Frame(Data_, output_directory, Partitions, Wallet_Brand, widget_filter)
    
    return Data_

def Function_Filter(RDD, Dates, Benefits, Contacts_Min, Value_Min, Value_Max):

    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))     

    RDD = RDD.withColumn("cuenta2", concat(col("cuenta2"), lit("-")))
    RDD = RDD.withColumn("cuenta", concat(col("cuenta"), lit("-")))

    Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    RDD = Data_C
    
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