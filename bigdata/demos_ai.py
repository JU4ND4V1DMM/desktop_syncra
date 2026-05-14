import os
import pyspark
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws, broadcast
from pyspark.sql.functions import expr, when, row_number, collect_list, sum, length
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
from web.temp_parquet import save_temp_log

Ram = "12g"
TimeOUT = "200"

os.environ["PYSPARK_DRIVER_MEMORY"] = Ram 
os.environ["PYSPARK_EXECUTOR_MEMORY"] = Ram  
os.environ["PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT"] = TimeOUT

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
def function_complete_demographic(path, output_directory, Partitions, month_data, year_data):
    
    get_lazy_spark()

    files = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".csv")]
    Data_Frame = spark.read.option("header", "true").option("sep", ";").csv(files)
    Data_Frame = Data_Frame.select([col(c).cast(StringType()).alias(c) for c in Data_Frame.columns])
    
    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Data_Frame.withColumn("Dato_Contacto", col("dato"))
    
    Data_Email = Data_Frame
    Data_Frame = ORDER_Process(Data_Frame)
    Data_Email = ORDER_Process_Email(Data_Email)
    Data = UnionDataframes(Data_Frame, Data_Email, month_data, year_data)
    Save_Data_Frame (Data, output_directory, Partitions)
    
### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])
    DF = change_character_account(DF, "cuenta")

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), \
        character, ""))

    Data_ = Data_.withColumn(Column, concat(col(Column), lit("-")))
    
    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame):

    Data_Frame = Data_Frame.withColumnRenamed("cuenta", "Cuenta_Next")

    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions):
    
    Type_File = f"TratamientoDemograficos_Mensual_"

    delimiter = ";"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, Partitions, delimiter)

    return Data_Frame

### Desdinamización de líneas
def Phone_Data_Div(Data_Frame):

    for i in range(1, 21):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 21)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 21)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "Filtro")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame

### Desdinamización de líneas
def Email_Data_Div(Data_Frame):

    for i in range(1, 6):
        Data_Frame = Data_Frame.withColumn(f"email{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"email{i}").alias(f"email_list{i}") for i in range(1, 6)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"email_list{i}")).alias(f"email{i}") for i in range(1, 6)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "Filtro")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame

def cruice(RDD, Value_Compare):

    for position in range(1, 21):
        
        condition = (col(f"phone{position}") == "") & (col(f"phone{position}").isNotNull())
        RDD = RDD.withColumn(f"phone{position}", when(condition, Value_Compare).otherwise(col(f"phone{position}")))

    return RDD

def cruice_email(RDD, Value_Compare):

    for position in range(1, 6):
        
        condition = (col(f"email{position}") == "") & (col(f"email{position}").isNotNull())
        RDD = RDD.withColumn(f"email{position}", when(condition, Value_Compare).otherwise(col(f"email{position}")))

    return RDD

### Proceso de reordenación
def ORDER_Process(Data_):
    
    Data_Email = Data_
    Data_C = Data_.filter(col("Dato_Contacto") >= 3000000009)
    Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
    Data_F = Data_.filter(col("Dato_Contacto") >= 6010000000)
    Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
    
    Data_ = Data_C.union(Data_F)

    VALUE = "000000000"
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
    Data_ = Data_.withColumn("Cruce_DOCS", concat(col("cuenta"), lit("-"), col("identificacion")))
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    windowSpec = Window.partitionBy("Cruce_DOCS").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))

    Data_ = Phone_Data_Div(Data_)  

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))

    list_columns = ["identificacion", "cuenta", "phone1", "phone2", "phone3", "phone4", "phone5", "phone6", 
                    "phone7", "phone8", "phone9", "phone10", "phone11", "phone12", "phone13", "phone14",
                    "phone15", "phone16", "phone17", "phone18", "phone19", "phone20"]

    
    Data_ = Data_.select(list_columns)

    Data_ = cruice(Data_, VALUE)

    for length_column in range(1, 21):

        condition_null = (col(f"phone{length_column}") == "000000000")
        condition_mobile = (col(f"phone{length_column}") <= 3599999999)
        Data_ = Data_.withColumn(f"New_Filter{length_column}", when(condition_null, 3)\
                                 .when(condition_mobile, 1).otherwise(2))

    Data_ = Data_.withColumn("order_filter", concat("New_Filter1", "New_Filter2", "New_Filter3", "New_Filter4", "New_Filter5", "New_Filter6",\
                     "New_Filter7", "New_Filter8", "New_Filter9", "New_Filter10", "New_Filter11", "New_Filter12", "New_Filter13",\
                        "New_Filter14", "New_Filter15", "New_Filter16", "New_Filter17", "New_Filter18", "New_Filter19", "New_Filter20"))
                        
    Data_ = Data_.withColumn("order_filter", (col("order_filter") * 1))
    Data_ = Data_.orderBy(["order_filter"], ascending = True)

    for position in range(1, 21):

        condition = (col(f"phone{position}") == VALUE)
        Data_ = Data_.withColumn(f"phone{position}", when(condition, "").otherwise(col(f"phone{position}")))
        Data_ = Data_
    
    Data_ = Data_.select(list_columns)

    Data_ = Data_.withColumn("NumMovil", expr(
         " + ".join([f"IF(phone{i} BETWEEN 3000000009 AND 3599999999, 1, 0)" for i in range(1, 17)])
     ))

    Data_ = Data_.withColumn("NumFijo", expr(
        " + ".join([f"IF(phone{i} BETWEEN 6000000009 AND 6599999999, 1, 0)" for i in range(1, 17)])
    ))
    
    print(Data_.columns)
    
    return Data_

def ORDER_Process_Email(Data_):
    

    Data_ = Data_.filter(col("Dato_Contacto").contains("@"))

    VALUE = "000000000"
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))
    Data_ = Data_.withColumn("Cruce_DOCS", concat(col("cuenta"), lit("-"), col("identificacion")))
    
    Data_ = Data_.dropDuplicates(["Cruce_Cuentas"])

    windowSpec = Window.partitionBy("Cruce_DOCS").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))

    Data_ = Email_Data_Div(Data_)

    Data_ = Data_.withColumn("cuenta", col("cuenta").cast("string"))

    list_columns = ["identificacion", "cuenta", "email1", "email2", "email3", "email4", "email5"]

    Data_ = Data_.select(list_columns)

    Data_ = cruice_email(Data_, VALUE)

    for position in range(1, 6):

        condition = (col(f"email{position}") == VALUE)
        Data_ = Data_.withColumn(f"email{position}", when(condition, "").otherwise(col(f"email{position}")))
        Data_ = Data_
    
    Data_ = Data_.select(list_columns)

    Data_ = Data_.withColumn("NumEmail", expr(
        " + ".join([f"IF(email{i} LIKE '%@%', 1, 0)" for i in range(1, 6)])
    ))
    
    print(Data_.columns)
    
    return Data_

def UnionDataframes(DF_Mins, DF_Emails, month_data, year_data):
    try:
        DF_Emails = DF_Emails.withColumnRenamed("identificacion", "identificacion_email")
        Data_Frame = DF_Mins.join(DF_Emails, on="cuenta", how="full_outer")
        
        Data_Frame = Data_Frame.withColumn(
            "Filtro Demografico",
            when((col("NumMovil") > 0) & (col("NumFijo") > 0) & (col("NumEmail") > 0), "Celular Fijo Email")
            .when((col("NumMovil") > 0) & (col("NumFijo") > 0), "Celular Fijo")
            .when((col("NumMovil") > 0) & (col("NumEmail") > 0), "Celular Email")
            .when(col("NumMovil") > 0, "Celular")
            .when((col("NumFijo") > 0) & (col("NumEmail") > 0), "Fijo Email")
            .when(col("NumFijo") > 0, "Fijo")
            .when(col("NumEmail") > 0, "Email")
            .otherwise("No posee Demograficos")
        )
        
        Data_Frame = Data_Frame.withColumn("identificacion", when(col("identificacion").isNull(), 
                                                                  col("identificacion_email")).otherwise(col("identificacion")))
        
        Data_Frame = Data_Frame.drop("identificacion_email")
        
        Data_Frame = Data_Frame.withColumn("MES DE ASIGNACION", lit(month_data))
        Data_Frame = Data_Frame.withColumn("PERIODO DE ASIGNACION", lit(year_data))       
        
        column_order = [col for col in Data_Frame.columns if col not in ["phone1", "phone2", "phone3", "phone4", "phone5", 
                                                                        "phone6", "phone7", "phone8", "phone9", "phone10", 
                                                                        "phone11", "phone12", "phone13", "phone14", "phone15", 
                                                                        "phone16", "phone17", "phone18", "phone19", "phone20", 
                                                                        "email1", "email2", "email3", "email4", "email5"]] + [
                        "phone1", "phone2", "phone3", "phone4", "phone5", "phone6", "phone7", "phone8", "phone9", "phone10", 
                        "phone11", "phone12", "phone13", "phone14", "phone15", "phone16", "phone17", "phone18", "phone19", "phone20",
                        "email1", "email2", "email3", "email4", "email5"
        ]

        Data_Frame = Data_Frame.select(column_order)

        Data_Frame = Data_Frame.withColumn(
                "NumMovil", when((col("NumMovil") == 0) | col("NumMovil").isNull(), "").otherwise(col("NumMovil"))
            ).withColumn(
                "NumFijo", when((col("NumFijo") == 0) | col("NumFijo").isNull(), "").otherwise(col("NumFijo"))
            ).withColumn(
                "NumEmail", when((col("NumEmail") == 0) | col("NumEmail").isNull(), "").otherwise(col("NumEmail"))
            )
            
        return Data_Frame
    
    except Exception as e:
        
        print(f"Error en UnionDataframes: {e}")
        return None