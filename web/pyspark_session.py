from pyspark.sql import SparkSession
import os
import shutil
import string
import psutil

def get_system_memory():
    """Obtiene la memoria RAM total del sistema en GB"""
    memory_gb = psutil.virtual_memory().total / (1024 ** 3)  # Convertir bytes a GB
    return memory_gb

def get_disk_with_most_free_space():
    best_drive = None
    max_free = 0
    for drive_letter in string.ascii_uppercase:
        drive = f"{drive_letter}:/"
        if os.path.exists(drive):
            try:
                total, used, free = shutil.disk_usage(drive)
                if free > max_free:
                    max_free = free
                    best_drive = drive
            except PermissionError:
                continue
    return best_drive

def try_create_spark_session(config_level):
    try:
        # Obtener memoria total del sistema
        total_memory_gb = get_system_memory()
        
        if config_level == "advanced": 
            # 85% de la memoria total
            memory_gb = int(total_memory_gb * 0.85)
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("spark.local.dir", "C:/tmp/hive") \
                .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
                .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
                .config("spark.driver.memory", f'{memory_gb}g') \
                .config("spark.executor.memory", f'{memory_gb}g') \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
                .getOrCreate()
            spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            print(f"✔ Spark avanzado inicializado correctamente. Memoria: {memory_gb}GB ({total_memory_gb:.1f}GB total)")
            return spark

        elif config_level == "medium":
            # 60% de la memoria total
            memory_gb = int(total_memory_gb * 0.60)
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("spark.driver.memory", f'{memory_gb}g') \
                .config("spark.executor.memory", f'{memory_gb}g') \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
                .getOrCreate()
            spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            print(f"✔ Spark medio inicializado correctamente. Memoria: {memory_gb}GB ({total_memory_gb:.1f}GB total)")
            return spark

        elif config_level == "basic":
            spark = SparkSession.builder \
                .appName("GlobalSparkApp") \
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
                .getOrCreate()
            print(f"✔ Spark básico inicializado correctamente. Memoria automática ({total_memory_gb:.1f}GB total)")
            return spark

    except Exception as e:
        print(f"⚠ Falló configuración {config_level}: {e}")
        return None

def get_spark_session():
    configs = ["advanced", "medium", "basic"]
    for config in configs:
        spark = try_create_spark_session(config)
        if spark is not None:
            return spark
    raise RuntimeError("❌ No fue posible inicializar ninguna sesión de Spark.")