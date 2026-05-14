import os
import pyspark
from pyspark.sql.window import Window
from modules import filter_base
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number, concat_ws
from pyspark.sql.functions import expr, when, to_date, datediff, current_date, split, length, collect_list, row_number
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


### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, partitions, filter_brands, filter_origins, Dates, Today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max):
    get_lazy_spark()
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)
    
    Data_Frame = Task_Process(Data_Frame, output_directory, partitions, filter_brands, filter_origins, Dates, Today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True,sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("marca", "MARCA")
    Data_Frame = Data_Frame.withColumnRenamed("marca2", "MARCA REAL")
    Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
    Data_Frame = Data_Frame.withColumnRenamed("phone1", "Telefono 1")
    Data_Frame = Data_Frame.withColumnRenamed("phone2", "Telefono 2")
    Data_Frame = Data_Frame.withColumnRenamed("phone3", "Telefono 3")
    Data_Frame = Data_Frame.withColumnRenamed("identificacion", "Documento")
    Data_Frame = Data_Frame.withColumnRenamed("origen", "CRM Origen")
    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "CUENTA REAL")
    Data_Frame = Data_Frame.withColumnRenamed("dias_transcurridos", "DIAS DE MORA")
    Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE")
    Data_Frame = Data_Frame.withColumnRenamed("mejorperfil_mes", "MEJOR PERFIL MES")
    Data_Frame = Data_Frame.withColumnRenamed("fechagestion_contactodirecto", "FECHA CONTACTO")

    Data_Frame = Data_Frame.select("Telefono 1", "Telefono 2", "Telefono 3", "**", \
                                   "Documento", "CRM Origen", "**2", "CUENTA REAL", "MARCA", \
                                    "MARCA REAL", "DIAS DE MORA", "DESCUENTO", "NOMBRE", "MEJOR PERFIL MES", "FECHA CONTACTO")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, partitions):

    Type_File = "Predictivo"
    delimiter = ";"
    
    Directory_to_Save = f"{Directory_to_Save}---- Bases para CARGUE ----"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, partitions, delimiter)
        
    return Data_Frame

### Dinamización de columnas de contacto
def Phone_Data(Data_):

    columns_to_stack_min = ["min"]
    columns_to_stack_celular = [f"celular{i}" for i in range(1, 11)]
    columns_to_stack_fijo = [f"fijo{i}" for i in range(1, 4)]
    all_columns_to_stack = columns_to_stack_celular + columns_to_stack_fijo + columns_to_stack_min
    columns_to_drop_contact = all_columns_to_stack
    stacked_contact_data_frame = Data_.select("*", *all_columns_to_stack)

    stacked_contact_data_frame = stacked_contact_data_frame.select(
        "*",
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Contacto")
    )
    Data_ = stacked_contact_data_frame.drop(*columns_to_drop_contact)

    return Data_

def Phone_Data_Div(Data_Frame):

    for i in range(1, 10):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("**").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 10)])
    pivoted_data = consolidated_data.select("**", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 10)])

    list_columns = ["**", "identificacion", "origen", "**2", "cuenta2", \
                    "marca", "marca2", "fecha_vencimiento", "fecha_asignacion", "fechagestion_contactodirecto", "Mod_init_cta", \
                    "descuento", "tipo_pago", "nombrecompleto", "mejorperfil_mes"]

    Data_Frame = Data_Frame.select(list_columns)
    Data_Frame = Data_Frame.join(pivoted_data, "**", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame, list_columns


def Filter_Phone(Data_Frame, list_columns):

    list_phone = ["phone1", "phone2", "phone3"]
    list_select_1 = list_columns + list_phone
    RDD_1 = Data_Frame.select(list_select_1)
    
    list_phone2 = ["phone4", "phone5", "phone6"]
    list_select_2 = list_columns + list_phone2
    RDD_2 = Data_Frame.select(list_select_2)
    for i in range(3):
        RDD_2 = RDD_2.withColumnRenamed(f"phone{i+3}", f"phone{i}")
    
    list_phone3 = ["phone7", "phone8", "phone9"]
    list_select_3 = list_columns + list_phone3
    RDD_3 = Data_Frame.select(list_select_3)
    for i in range(3):
        RDD_3 = RDD_3.withColumnRenamed(f"phone{i+6}", f"phone{i}")

    RDD = RDD_1.union(RDD_2)
    Data_Frame = RDD.union(RDD_3)

    return Data_Frame

### Proceso de filtrado de líneas
def Task_Process(Data_, Directory_to_Save, partitions, filter_brands, filter_origins, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max):

    Data_ = filter_base.Function_Complete(Data_)
    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))

    Data_ = Data_.withColumn(
        "tipo_pago", 
        when(((col("tipo_pago").isNull()) | (col("tipo_pago") == "")), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))
    
    Data_ = Data_.withColumn(
        "fechagestion_contactodirecto", 
        when((col("fechagestion_contactodirecto").isNull() | (col("fechagestion_contactodirecto") == "")), lit("2000-04-30"))
        .otherwise(col("fechagestion_contactodirecto")))
    
    Data_ = Data_.filter(col("marca").isin(filter_brands))
    Data_ = Data_.filter(col("origen").isin(filter_origins))

    Data_ = Function_Filter(Data_, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max)
    
    Data_ = Data_.withColumn("**2", lit(""))

    Data_ = Data_.withColumn("**", col("cuenta").cast("string"))
    
    Data_ = Data_.withColumn(
        "tipo_pago", 
        when((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))

    Data_ = Data_.withColumn(
        "descuento", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("0"))
        .otherwise(col("descuento")))

    Data_ = Data_.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    for col_name, data_type in Data_.dtypes:
        if data_type == "double":
            Data_ = Data_.withColumn(col_name, col(col_name).cast(StringType()))

    Data_ = Data_.select("Dato_Contacto", "**", "identificacion", "origen", "**2", "cuenta2", \
                         "marca", "marca2", "fecha_vencimiento", "fecha_asignacion", "fechagestion_contactodirecto", "Mod_init_cta", \
                         "descuento", "tipo_pago", "nombrecompleto", "mejorperfil_mes")
    
    Data_ = Data_.dropDuplicates(["Dato_Contacto"])
    Order_Columns = ["Mod_init_cta", "origen","Dato_Contacto",'marca', "fechagestion_contactodirecto"]

    for Column in Order_Columns:
        Data_ = Data_.orderBy(col(Column).desc())
    
    windowSpec = Window.partitionBy("identificacion").orderBy("**","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))    

    Data_, list_columns = Phone_Data_Div(Data_)
    #Data_ = Filter_Phone(Data_, list_columns)
    Data_ = Data_.withColumn("now", current_date())
    Data_ = Data_.withColumn("dias_transcurridos", datediff(col("now"), col("fecha_vencimiento")))
    
    Data_ = Renamed_Column(Data_)
    Save_Data_Frame(Data_, Directory_to_Save, partitions)
    
    return Data_

def Function_Filter(RDD, Dates, today_IVR, Benefits, Contacts_Min, Value_Min, Value_Max):
    
    RDD = RDD.filter(col("fechagestion_contactodirecto") != today_IVR)
    
    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))           
    RDD = RDD.filter(col("Referencia") != "")

    if Contacts_Min == "Celular":
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        RDD = Data_C

    elif Contacts_Min == "Fijo":
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_F
    
    else:
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_C.union(Data_F)
    
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