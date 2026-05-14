from datetime import datetime
import os
from web.pyspark_session import get_spark_session
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, when, expr, concat, lit, row_number, collect_list, concat_ws, trim, split, length
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
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

def function_complete_task_WEB(input_folder, output_folder, Partitions):

    get_lazy_spark()
    files = []

    for root, _, file_names in os.walk(input_folder):
        for file_name in file_names:
            if file_name.endswith('.csv') or file_name.endswith('.txt'):
                print(f"{_}")
                files.append(os.path.join(root, file_name))

    consolidated_df = None
    for file in files:
        if file.endswith('.csv'):
            df = spark.read.csv(file, header=True, inferSchema=True)
        elif file.endswith('.txt'):
            df = spark.read.option("delimiter", "\t").csv(file, header=True, inferSchema=True)

        if consolidated_df is None:
            consolidated_df = df
        else:
            consolidated_df = consolidated_df.unionByName(df, allowMissingColumns=True)

    if consolidated_df is not None:
        
        selected_columns = [
            "last_local_call_time", "status", "vendor_lead_code", "phone_number", "address1"]

        consolidated_df = consolidated_df.select(*selected_columns)

        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), "T", " "))
        consolidated_df = consolidated_df.withColumn("last_local_call_time", regexp_replace(col("last_local_call_time"), ".000-05:00", ""))

        consolidated_df = consolidated_df.withColumn(
            "TIPOLOGIA",
            when(col("status") == "AB", "Buzon de voz lleno")
            .when(col("status") == "PM", "Se inicia mensaje y cuelga")                          #Efectivo
            .when(col("status") == "AA", "Maquina contestadora")
            .when(col("status") == "ADC", "Numero fuera de servicio")
            .when(col("status") == "DROP", "No contesta")
            .when(col("status") == "NA", "No contesta")
            .when(col("status") == "NEW", "Contacto sin marcar")
            .when(col("status") == "INCALL", "Llamada en curso")                                  #Solo de predictivo
            .when(col("status") == "ERI", "Error de agente")                                    #Solo de predictivo
            .when(col("status") == "MNC", "Marcacion Next Call")                                #Solo de predictivo
            .when(col("status") == "TOUT", "Error de salida temporal")                                     #Solo de predictivo
            .when(col("status") == "PDROP", "Error desde el operador")
            .when(col("status") == "PU", "Cuelga llamada")                                      
            .when(col("status") == "SVYEXT", "Llamada transferida al agente")                   #Efectivo
            .when(col("status") == "XFER", "Se reprodujo mensaje completo")                     #Efectivo
            .when(col("status") == "SVYHU", "Cuelga durante una transferencia")                 #Efectivo
            .otherwise("error")
        )

        consolidated_df = consolidated_df.withColumn(
            "perfil",
            when(col("status") == "AB", "No Contestan")
            .when(col("status") == "PM", "Volver a Llamar")                             #Efectivo
            .when(col("status") == "AA", "No Contestan")
            .when(col("status") == "ADC", "No Contestan")
            .when(col("status") == "DROP", "No Contestan")
            .when(col("status") == "NA", "No Contestan")
            .when(col("status") == "NEW", "Volver a Llamar")
            .when(col("status") == "INCALL", "Interesado en Pagar")                                  #Solo de predictivo
            .when(col("status") == "ERI", "No Contestan")                                    #Solo de predictivo
            .when(col("status") == "MNC", "Interesado en Pagar")                                #Solo de predictivo
            .when(col("status") == "TOUT", "No Contestan")                                     #Solo de predictivo
            .when(col("status") == "PDROP", "No Contestan")
            .when(col("status") == "PU", "Colgo")                                      
            .when(col("status") == "SVYEXT", "Interesado en Pagar")                     #Efectivo
            .when(col("status") == "XFER", "Interesado en Pagar")                       #Efectivo
            .when(col("status") == "SVYHU", "Interesado en Pagar")                      #Efectivo
            .otherwise("error")
        )

        consolidated_df = consolidated_df.withColumn(
            "Filtro_BATCH",
            when(col("status") == "AB", "No efectivo")
            .when(col("status") == "DROP", "No efectivo")
            .when(col("status") == "NA", "No efectivo")
            .when(col("status") == "AA", "No efectivo")
            .when(col("status") == "ADC", "No efectivo")                              
            .when(col("status") == "ERI", "No efectivo")                                             
            .when(col("status") == "TOUT", "No efectivo")                           
            .when(col("status") == "PDROP", "No efectivo")   
            .when(col("status") == "NEW", "Sin marcar")           
            .otherwise("Efectivo")
        )

        consolidated_df = consolidated_df.withColumn(
            "Filtro_BATCH",
            when(length(col("address1")) < 3, lit("Sin cuenta"))       
            .otherwise(col("Filtro_BATCH"))
        )

        consolidated_df = consolidated_df.withColumn("accion", lit("Hacer llamada"))
        consolidated_df = consolidated_df.dropDuplicates()

        consolidated_df = Function_ADD(consolidated_df)
        
        delimiter = ";"
        Type_Proccess = "Resultado BATCH"
        
        save_to_csv(consolidated_df, output_folder, Type_Proccess, Partitions, delimiter)

        #Effectiveness_df(consolidated_df, output_folder2_)
    else:
        pass
    return consolidated_df

def Function_ADD(RDD):

    RDD = RDD.withColumn("usuario", lit("Predictivo"))
    RDD = RDD.withColumn("gestion", lit("No contacto"))

    List_IVR = []
    List_TRANS = []

    RDD = RDD.withColumn("fecha_promesa", lit(""))
    RDD = RDD.withColumn("valor_promesa", lit(""))
    RDD = RDD.withColumn("numero_cuotas", lit(""))

    RDD = RDD.withColumnRenamed("last_local_call_time", "fechagestion")
    RDD = RDD.withColumnRenamed("address1", "cuenta_promesa")
    RDD = RDD.withColumnRenamed("vendor_lead_code", "identificacion")
    RDD = RDD.withColumnRenamed("phone_number", "numeromarcado")

    RDD = RDD.withColumn("fechagestion", split(col("fechagestion"), " "))
    RDD = RDD.withColumn("fechagestion", (RDD["fechagestion"][0]))

    RDD = RDD.withColumn("CRUCE", concat(col("numeromarcado"), col("identificacion")))
    
    list_columns = ["gestion", "usuario", "fechagestion", "accion", "perfil", "numeromarcado", "identificacion",\
                    "cuenta_promesa", "fecha_promesa", "valor_promesa", "numero_cuotas", "Filtro_BATCH", "TIPOLOGIA",\
                    "CRUCE"]

    RDD = RDD.select(list_columns)

    return RDD