from pyspark.sql import SparkSession, SQLContext
from web.pyspark_session import get_spark_session
from pyspark.sql.functions import col, concat, lit, when
from datetime import datetime
import os
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

def Union_Files_BOT(Path, Outpath, partitions):
    get_lazy_spark()
    
    required_columns = [
        "last_attempt", "attempts", "disposition", "phone", "name", 
        "origin", "debtdays", "document", "debtamount", "Edad de Mora", 
        "Mejor Gestion", "duration", "ans_colgado", "ans_validacion_identidad", 
        "ans_validacion_pago", "CUENTA", "CUENTA_NEXT"
    ]
    
    # Initialize an empty DataFrame
    consolidated_df = None

    # Iterate over each CSV file in the specified directory
    for filename in os.listdir(Path):
        print(filename)
        if filename.endswith(".csv"):
            file_path = os.path.join(Path, filename)
            df = spark.read.option("header", "true").option("sep", ",").csv(file_path)

            # Add missing required columns
            for column in required_columns:
                if column not in df.columns:
                    print(column)
                    df = df.withColumn(column, lit(None))
            df = df.select(required_columns)

            # Process the DataFrame as per your original logic
            df = df.withColumn("Key", concat(col("phone"), col("CUENTA"), col("disposition")))
            df = df.dropDuplicates(["Key"])
            df = df.withColumnRenamed("last_attempt", "LLAMADA") \
                   .withColumnRenamed("attempts", "MARCACIONES") \
                   .withColumnRenamed("disposition", "TIPOLOGIA") \
                   .withColumnRenamed("phone", "TELEFONO") \
                   .withColumnRenamed("name", "NOMBRE") \
                   .withColumnRenamed("origin", "ORIGEN") \
                   .withColumnRenamed("debtdays", "DIAS_MORA") \
                   .withColumnRenamed("document", "DOCUMENTO") \
                   .withColumnRenamed("debtamount", "VALOR_DEUDA") \
                   .withColumnRenamed("Edad de Mora", "EDAD_MORA") \
                   .withColumnRenamed("Mejor Gestion", "MEJOR_GESTION_NEXT") \
                   .withColumnRenamed("duration", "SEGUNDOS EN LLAMADA") \
                   .withColumnRenamed("ans_colgado", "FILTRO_CUELGUE") \
                   .withColumnRenamed("ans_validacion_identidad", "FILTRO_IDENTIDAD") \
                   .withColumnRenamed("ans_validacion_pago", "FILTRO_PAGO")

            # Add additional columns
            df = df.withColumn("FILTRO__PRINCIPAL", lit(""))
            List_Columns = [
                "FILTRO__PRINCIPAL", "LLAMADA", "MARCACIONES", "TIPOLOGIA", "TELEFONO",
                "NOMBRE", "CUENTA", "CUENTA_NEXT", "ORIGEN", "DIAS_MORA",
                "DOCUMENTO", "VALOR_DEUDA", "EDAD_MORA", "MEJOR_GESTION_NEXT",
                "FILTRO_CUELGUE", "FILTRO_IDENTIDAD", "FILTRO_PAGO", "SEGUNDOS EN LLAMADA"
            ]

            # Ensure all required columns are present
            for column in List_Columns:
                if column not in df.columns:
                    df = df.withColumn(column, lit(""))

            df = df.select(List_Columns)

            # Apply transformations to the DataFrame
            df = df.withColumn(
                "FILTRO__PRINCIPAL", when((col("FILTRO_PAGO") == "confirmacion_pago") | (col("FILTRO_PAGO") == "propuesta_pago_valida"), lit("Compromiso de Pago con BOT"))
                .when((col("FILTRO_PAGO") == "ya_pago"), lit("Cliente al dia"))
                .when(((col("TIPOLOGIA") == "ANSWER") & (((col("FILTRO_PAGO") != "confirmacion_pago") & (col("FILTRO_PAGO") != "propuesta_pago_valida") & (col("FILTRO_PAGO") != "ya_pago")) | (col("FILTRO_PAGO").isNull()))), lit("Efectivo sin Compromiso de Pago"))
                .otherwise(lit("No efectivo")))

            df = df.withColumn(
                "TIPOLOGIA", when((col("TIPOLOGIA") == "INVALID"), lit("INVALIDO"))
                .when((col("TIPOLOGIA") == "BUSY"), lit("OCUPADO"))
                .when((col("FILTRO_PAGO") == "respuesta_invalida") | (col("FILTRO_PAGO") == "negacion_pago"), lit("RENUENTE"))
                .when((col("TIPOLOGIA") == "ANSWER") & (col("FILTRO_IDENTIDAD") == "confirmar_identidad"), lit("TITULAR"))
                .when((col("TIPOLOGIA") == "ANSWER") & (col("FILTRO_IDENTIDAD") == "negar_identidad"), lit("EQUIVOCADO"))
                .when((col("TIPOLOGIA") == "ANSWER") & (col("FILTRO_IDENTIDAD") == "familiar_conocido"), lit("TERCERO"))
                .when((col("TIPOLOGIA") == "ANSWER") & (col("FILTRO_IDENTIDAD") == "no_disponible"), lit("TERCERO"))
                .when((col("TIPOLOGIA") == "ANSWER") & ((col("FILTRO_IDENTIDAD") == "repetir_pregunta") | (col("FILTRO_IDENTIDAD") == "respuesta_invalida")), lit("SIN CONFIRMAR TITULARIDAD"))
                .when((col("TIPOLOGIA") == "ANSWER") & (col("FILTRO_IDENTIDAD").isNull()), lit("CONTESTADO"))
                .when((col("TIPOLOGIA") == "NO RING"), lit("NO TIMBRA"))
                .when((col("TIPOLOGIA") == "NO ANSWER"), lit("NO RESPONDIDO"))
                .when((col("TIPOLOGIA") == "VOICEMAIL"), lit("BUZON DE VOZ"))
                .otherwise(lit("NO MARCADO")))

            df = df.withColumn(
                "TIPOLOGIA", when((col("TIPOLOGIA") == "TITULAR") & (col("FILTRO__PRINCIPAL") == "Cliente al dia"), lit("YA PAGO"))
                .when((col("TIPOLOGIA") == "TITULAR") & (col("FILTRO__PRINCIPAL") == "Compromiso de Pago con BOT"), lit("PROMESA"))
                .otherwise(col("TIPOLOGIA")))

            # Consolidate the DataFrame
            if consolidated_df is None:
                consolidated_df = df
            else:
                consolidated_df = consolidated_df.union(df)

    delimiter = ";"
    Type_Proccess = "Resultado BOT IPCom"
    
    save_to_csv(consolidated_df, Outpath, Type_Proccess, partitions, delimiter)

    return consolidated_df