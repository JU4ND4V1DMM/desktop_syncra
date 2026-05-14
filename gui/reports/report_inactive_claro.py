import os
import math
import polars as pl

TIPOLOGIAS_OBJETIVO = [
    "Archivos de No Gestión",
    "Exclusión de Cuenta o inactivacion reparto",
    "Exclusión de Documento o inactivacion reparto",
    "Pagos sin Aplicar"
]

def read_all_csvs(folder_path):
    dfs = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)

                df = pl.read_csv(
                    full_path,
                    separator=";",
                    infer_schema_length=0,  
                    schema_overrides={}
                )

                dfs.append(df)

    if dfs:
        return pl.concat(dfs, how="diagonal")
    else:
        return pl.DataFrame()
    
def save_large_files(df, base_path, base_name):
    MAX_ROWS = 1_048_000

    total_rows = df.height
    parts = math.ceil(total_rows / MAX_ROWS)

    # 📁 asegurar carpeta
    os.makedirs(base_path, exist_ok=True)

    # 🧱 1. PARQUET (completo)
    parquet_path = os.path.join(base_path, f"{base_name}.parquet")
    df.write_parquet(parquet_path)
    print(f"Parquet generado: {parquet_path}")

    # 🔧 CSV: modificar columna cuenta
    df_csv = df.with_columns(
        pl.col("cuenta").cast(pl.Utf8) + pl.lit("-")
    )
    # 🟢 CASO SIMPLE
    if parts == 1:
        csv_path = os.path.join(base_path, f"{base_name}.csv")

        content = df_csv.write_csv(separator=";")

        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        print(f"CSV generado: {csv_path}")
        return


    # 🔥 CASO GRANDE
    print(f"Dividiendo en {parts} partes...")

    for i in range(parts):
        start = i * MAX_ROWS

        chunk_csv = df_csv.slice(start, MAX_ROWS)

        csv_path = os.path.join(base_path, f"{base_name}_parte_{i+1}.csv")

        content = chunk_csv.write_csv(separator=";")

        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        print(f"CSV generado: {csv_path}")

def transform_inactive_logic(df):
    marca_df = (
        df.group_by("cuenta")
        .agg([
            pl.col("marca").unique().sort().str.join(", ").alias("marca"),
            pl.col("marca").n_unique().alias("cantidad_marcas")
        ])
        .with_columns(
            pl.when(pl.col("cantidad_marcas") > 1)
            .then(pl.lit("MULTIPLE"))
            .otherwise(pl.lit("UNICA"))
            .alias("tipo_marca")
        )
    )
    print(
        df.group_by("cuenta")
        .agg(pl.col("marca").n_unique().alias("cantidad_marcas"))
        .filter(pl.col("cantidad_marcas") > 1)
    )
    df = df.filter(pl.col("tipo_base").is_in(TIPOLOGIAS_OBJETIVO))

    df = df.with_columns(
        pl.col("fecha_evento").dt.date().alias("fecha_dia")
    )

    agg = (
        df.group_by(["cuenta", "tipo_base"])
        .agg([
            pl.col("fecha_evento").min().alias("fecha_inicio"),
            pl.col("fecha_evento").max().alias("fecha_fin"),
            pl.col("fecha_dia").n_unique().alias("dias_diferentes"),
            pl.col("fecha_dia")
              .unique()
              .sort()
              .dt.strftime("%Y-%m-%d")
              .str.join(", ")
              .alias("fechas")
        ])
    )

    result = (
        agg.with_columns(pl.lit("Si").alias("flag"))
        .pivot(
            values=[
                "flag",
                "fecha_inicio",
                "fecha_fin",
                "dias_diferentes",
                "fechas"
            ],
            index="cuenta",
            columns="tipo_base"
        )
    )

    result = result.join(marca_df, on="cuenta", how="left")

    ordered_cols = ["cuenta", "marca", "cantidad_marcas", "tipo_marca"]

    for tipo in TIPOLOGIAS_OBJETIVO:
        ordered_cols.extend([
            f"flag_{tipo}",
            f"dias_diferentes_{tipo}",
            f"fecha_inicio_{tipo}",
            f"fecha_fin_{tipo}",
            f"fechas_{tipo}",
        ])

    # ⚠️ solo columnas existentes (por si alguna tipología no aparece)
    ordered_cols = [col for col in ordered_cols if col in result.columns]

    result = result.select(ordered_cols)

    result = result.rename({
        col: col.replace("flag_", "")
        for col in result.columns
        if col.startswith("flag_")
    })

    return result

def report_inactive_claro(input, output):
    try:
        df = read_all_csvs(input)

        # 🔹 convertir fecha
        df = df.with_columns(
            pl.col("fecha_evento").str.strptime(
                pl.Datetime("us"),
                "%Y-%m-%d %H:%M:%S%.f",
                strict=False
            )
        )

        df = df.filter(pl.col("fecha_evento").is_not_null())

        result = transform_inactive_logic(df)

        folder = os.path.abspath(os.path.join(output, "------ CUENTAS INACTIVAS -------"))
        os.makedirs(folder, exist_ok=True)

        print("Guardando en:", folder)

        save_large_files(result, folder, "reporte_inactivos")

        print(f"Archivos generados en: {folder}")
        return result

    except Exception as e:
        print(e)
        return "Ha ocurrido un error"