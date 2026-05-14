import polars as pl
from pathlib import Path
from datetime import datetime, timedelta


CAMPAIGN_KEYWORDS = {
    "CLARO": ["CLARO"],
    "PAYJOY": ["PAYJOY"],
    "HABI": ["HABI"],
    "YADINERO": ["YADINERO"],
    "PUNTORED": ["PUNTORED"],
    "CREDIVECI": ["CREDIVECI"],
    "GM": ["GM"],
}


def normalize_text(value):
    return str(value).upper().strip() if value else ""


def classify_campaign(campaign_name):
    campaign_name = normalize_text(campaign_name)

    for group_name, keywords in CAMPAIGN_KEYWORDS.items():
        if any(normalize_text(keyword) in campaign_name for keyword in keywords):
            return group_name

    return "OTROS"


def duration_to_seconds(value):
    try:
        h, m, s = map(int, str(value).split(":"))
        return h * 3600 + m * 60 + s
    except:
        return 0


def seconds_to_hms(seconds):

    seconds = int(seconds)

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    return f"{hours:02}:{minutes:02}:{secs:02}"


def read_all_csv(input_folder):

    files = list(Path(input_folder).glob("*.csv"))
    dataframes = []

    for file in files:

        try:

            columns = pl.read_csv(
                file,
                separator=";",
                n_rows=1
            ).columns

            df = pl.read_csv(
                file,
                separator=";",
                infer_schema_length=0,
                dtypes={col: pl.Utf8 for col in columns},
                ignore_errors=True
            )

            dataframes.append(df)

        except:
            pass

    if not dataframes:
        raise Exception("No valid CSV files found")

    return pl.concat(dataframes, how="vertical_relaxed")


def process_calls(input_folder, output_folder):

    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    df = read_all_csv(input_folder)

    df = df.with_columns(

        pl.col("NOMBRE DE LA CAMPAÑA")
        .map_elements(classify_campaign, return_dtype=pl.Utf8)
        .alias("TIPO_CAMPANA"),

        pl.col("DURACIÓN")
        .map_elements(duration_to_seconds, return_dtype=pl.Int64)
        .alias("DURACION_SEGUNDOS")
    )

    result = (

        df.group_by(["TIPO_CAMPANA", "ESTADO"])

        .agg(

            pl.len().alias("REGISTROS"),

            pl.col("DURACION_SEGUNDOS")
            .sum()
            .alias("TOTAL_SEGUNDOS")
        )

        .with_columns(

            pl.col("TOTAL_SEGUNDOS")
            .map_elements(seconds_to_hms, return_dtype=pl.Utf8)
            .alias("DURACION_TOTAL")
        )

        .select([
            "TIPO_CAMPANA",
            "ESTADO",
            "REGISTROS",
            "DURACION_TOTAL",
            "TOTAL_SEGUNDOS"
        ])

        .sort(
            ["TIPO_CAMPANA", "REGISTROS"],
            descending=[False, True]
        )
    )

    output_file = output_folder / "resultado_ejecucion_blaster_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))

    result.write_csv(output_file, separator=";")

    print(result)
    print(f"\nArchivo generado: {output_file}")