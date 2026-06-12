from web.pyspark_session import get_spark_session
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, trim, format_number
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

def Union_Files_Demo(Path, Outpath, partitions):
    
    get_lazy_spark()
    
    files = [os.path.join(Path, file) for file in os.listdir(Path) if file.endswith(".csv")]

    Data_Frame = spark.read.option("header", "true").option("sep", ";").csv(files)

    Data_Frame = Data_Frame.withColumn("Key", concat(col("dato"), col("cuenta")))
    Data_Frame = Data_Frame.dropDuplicates(["key"])
    Data_Frame = Data_Frame.orderBy(["dato"], ascending = True)
    
    if "Marca" not in Data_Frame.columns:
        Data_Frame = Data_Frame.withColumn("Marca", lit("Sin asignar marca"))
        
    Data_Frame = Data_Frame.select("identificacion","cuenta","ciudad","depto","dato","tipodato","Marca")
    
    # Data_Frame = Data_Frame.filter(col("tipodato") == "telefono")
    # Data_Frame = Data_Frame.filter(col("dato") <= 3599999999)
    # Data_Frame = Data_Frame.filter(col("dato") >= 3000000009)
    # Data_Frame = Data_Frame.select("dato")
    # Data_Frame = Data_Frame.dropDuplicates(["dato"])

    delimiter = ";"
    Type_Proccess = "Demograficos Consolidados"
    
    save_to_csv(Data_Frame, Outpath, Type_Proccess, partitions, delimiter)
    
    return Data_Frame