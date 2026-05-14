import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

FOLDER_CONFIG = {
    "input_subfolders": ["Asignacion", "ReporteClientes", "ReporteGestion"], 
    "output_folder": "output",
    "excel_prefix": "REPORT_DAILY_PAYJOY"
}

COLUMN_CONFIG = {
    "assignments": {"id": "numero_identificacion", "bucket": "bucket_dias_mora"},
    "clients": {"id": "identificacion", "bucket": "bucket_dias_mora"}
}

MAPPINGS = {
    "type_call_answer": {
        "ya pago": "Contestada", "promesa": "Contestada", "volver a llamar": "Contestada",
        "interesado en pagar": "Contestada", "dificultad de pago": "Contestada", "abono": "Contestada",
        "recordatorio": "Contestada", "reclamacion": "Contestada", "renuente": "Contestada",
        "mensaje con tercero": "Contestada", "colgo": "Contestada", "no asume deuda": "Contestada",
        "tercero no toma mensaje": "Contestada", "posible fraude": "Contestada",
        "numero errado": "Gestionado", "fallecido": "Gestionado", "no contestan": "Gestionado",
        "email": "Gestionado", "mensaje": "Gestionado", "al dia": "Gestionado",
        "promesa parcial": "Gestionado", "no contesta": "Gestionado", "sin gestion": "Sin Gestion"
    },
    "type_correct": {"numero errado": "Si"},
    "contact_efecty": {
        "ya pago": "Contacto Efectivo", "promesa": "Contacto Efectivo", "volver a llamar": "Contacto Efectivo",
        "interesado en pagar": "Contacto Efectivo", "dificultad de pago": "Contacto Efectivo", 
        "abono": "Contacto Efectivo", "al dia": "Contacto Efectivo", "promesa parcial": "Contacto Efectivo"
    },
    "promise": {"promesa": "Si", "interesado en pagar": "Si", "abono": "Si"},
    "cancelation_request": {"reclamación": "Si"},
    "debt_rejection": {
        "cliente no reconoce el cobro": "Si", "fraude": "Si", 
        "préstamo a nombre de otra persona": "Si", "prestamo a nombre de otraopersona": "Si"
    },
    "payments_made": {
        "cliente indica que ya pagó": "Si", "cliente indica que ya pago": "Si", 
        "ya realizó el pago total del crédito": "Si", "ya realizó el pago total del credito": "Si"
    },
    "stolen_equipment": {
        "robo o pérdida de dispositivo": "Si", "robo o perdida de dispositivoo": "Si"
    },
    "warranty_issues": {
        "devolución del equipo": "Si", "devolución del equipoo": "Si", 
        "dispositivo en garantía": "Si", "dispositivo en garantia": "Si"
    }
}

def group_buckets(bucket_name):
    bucket_str = str(bucket_name).strip()
    if not bucket_str or bucket_str == "0": 
        return "DPD 0"
    
    if "+" in bucket_str: 
        return "DPD +121"
    
    if "-" in bucket_str:
        try:
            first_part = bucket_str.split("-")[0]
            import re
            numbers = re.findall(r'\d+', first_part)
            if numbers:
                first_val = int(numbers[0])
                if first_val > 120:
                    return "DPD +121"
        except ValueError:
            pass

    return bucket_str

def get_bucket_weight(bucket_name):
    weights = {
        "TOTAL": 0, "DPD 1-15": 1, "DPD 16-30": 2, "DPD 31-60": 3,
        "DPD 61-90": 4, "DPD 91-120": 5, "DPD 1-120": 6, "DPD +121": 7
    }
    return weights.get(str(bucket_name), 99)

def process_management(df: pl.DataFrame):
    print("🛠️ Normalizing Management Data...")
    df = df.with_columns([
        pl.col("ultimo_perfil_cliente").str.to_lowercase().fill_null("sin gestion").alias("_temp_profile"),
        pl.col("motivo_no_pago_historico").str.to_lowercase().fill_null("").alias("_temp_reason"),
        pl.col("contacto_historico").str.to_lowercase().fill_null("").alias("_temp_contact"),
        pl.col("tiempogestion").cast(pl.Float64, strict=False).fill_null(0.0)
    ])
    
    df = df.with_columns(
        pl.when(pl.col("_temp_contact").str.contains("batch"))
        .then(pl.lit(10.0))
        .otherwise(pl.col("tiempogestion"))
        .alias("tiempogestion")
    )

    df = df.with_columns(pl.col("tiempogestion").alias("tiempogestion_sec"))
    
    df = df.with_columns([
        pl.col("_temp_profile").replace(MAPPINGS["type_call_answer"], default="Desconocido").alias("type_call_answer"),
        pl.col("_temp_profile").replace(MAPPINGS["type_correct"], default="No").alias("type_correct"),
        pl.col("_temp_profile").replace(MAPPINGS["contact_efecty"], default="No Contacto").alias("contact_efecty"),
        pl.col("_temp_profile").replace(MAPPINGS["promise"], default="No").alias("promise"),
        pl.col("_temp_reason").replace(MAPPINGS["cancelation_request"], default="No").alias("cancelation_request"),
        pl.col("_temp_reason").replace(MAPPINGS["debt_rejection"], default="No").alias("debt_rejection"),
        pl.col("_temp_reason").replace(MAPPINGS["payments_made"], default="No").alias("payments_made"),
        pl.col("_temp_reason").replace(MAPPINGS["stolen_equipment"], default="No").alias("stolen_equipment"),
        pl.col("_temp_reason").replace(MAPPINGS["warranty_issues"], default="No").alias("warranty_issues"),
        pl.col("bucket_dias_mora").map_elements(group_buckets, return_dtype=pl.String).alias("bucket_dias_mora"),
        pl.when(pl.col("tiempogestion_sec") >= 15).then(pl.lit("Si")).otherwise(pl.lit("No")).alias("time_call")
    ])
    
    return df.drop(["_temp_profile", "_temp_reason", "_temp_contact"])

def calculate_kpi_dashboard(assignments_df, clients_df, management_df):
    print("📊 Calculating KPI Dashboard metrics...")
    
    assignments_df = assignments_df.with_columns(pl.col(COLUMN_CONFIG["assignments"]["bucket"]).map_elements(group_buckets, return_dtype=pl.String))
    clients_df = clients_df.with_columns(pl.col(COLUMN_CONFIG["clients"]["bucket"]).map_elements(group_buckets, return_dtype=pl.String))

    management_df = management_df.with_columns(
        pl.col("bucket_dias_mora").map_elements(group_buckets, return_dtype=pl.String)
    )

    reachable_joined = assignments_df.join(
        clients_df.select([COLUMN_CONFIG["clients"]["id"]]), 
        left_on=COLUMN_CONFIG["assignments"]["id"], 
        right_on=COLUMN_CONFIG["clients"]["id"], 
        how="inner"
    )

    base_assign = assignments_df.group_by("bucket_dias_mora").agg(pl.len().alias("Assigned Portfolio")).rename({"bucket_dias_mora": "bucket"})
    base_reachable = reachable_joined.group_by("bucket_dias_mora").agg(pl.len().alias("Reachable Portfolio")).rename({"bucket_dias_mora": "bucket"})

    buckets_mgmt = management_df.group_by("bucket_dias_mora").agg([
        pl.col("cuenta").filter(pl.col("type_call_answer") != "Desconocido").n_unique().alias("Contacted Portfolio"),
        pl.col("asesor_gestion").filter(~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"])).n_unique().alias("Active Agents"),
        pl.col("type_call_answer").filter(pl.col("type_call_answer").is_in(["Contestada", "Gestionado"])).count().alias("Total Call Attempts"),
        pl.col("type_call_answer").filter(pl.col("type_call_answer") == "Contestada").count().alias("Total Answered Calls"),
        pl.col("type_correct").filter(pl.col("type_correct") == "Si").count().alias("Don’t Know on the Phone"),
        pl.col("time_call").filter(pl.col("time_call") == "Si").count().alias("Total Answered Calls (Excl. Short Calls)"),
        pl.col("cuenta").filter(pl.col("type_call_answer") == "Contestada").n_unique().alias("Unique Customers Reached"),
        pl.col("cuenta").filter(pl.col("time_call") == "Si").n_unique().alias("Unique Customers Reached (Excl. Short Calls)"),
        pl.col("contact_efecty").filter(pl.col("contact_efecty") == "Contacto Efectivo").count().alias("Total RPCs (Right Party Contacts)"),
        pl.col("cuenta").filter(pl.col("contact_efecty") == "Contacto Efectivo").n_unique().alias("Unique RPCs"),
        pl.col("promise").filter(pl.col("promise") == "Si").count().alias("Promises to Pay (PTP)"),
        pl.col("cancelation_request").filter(pl.col("cancelation_request") == "Si").count().alias("Request contract cancelation"),
        pl.col("debt_rejection").filter(pl.col("debt_rejection") == "Si").count().alias("Debt Not Recognized"),
        pl.col("payments_made").filter(pl.col("payments_made") == "Si").count().alias("Payment Already Made"),
        pl.col("stolen_equipment").filter(pl.col("stolen_equipment") == "Si").count().alias("Device Reported Stolen"),
        pl.col("warranty_issues").filter(pl.col("warranty_issues") == "Si").count().alias("Device Warranty Access Issue"),
        pl.col("tiempogestion_sec").filter((pl.col("tiempogestion_sec") > 0) & (~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"]))).mean().alias("Total AHT"),
        pl.col("tiempogestion_sec").filter((pl.col("tiempogestion_sec") > 0) & (~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"])) & (pl.col("type_call_answer") == "Contestada")).mean().alias("AHT (Excluding Short Calls)")
    ]).rename({"bucket_dias_mora": "bucket"})

    final_buckets = base_assign.join(base_reachable, on="bucket", how="full", coalesce=True) \
                               .join(buckets_mgmt, on="bucket", how="full", coalesce=True) \
                               .fill_null(0)

    total_data = {
        "bucket": "TOTAL",
        "Assigned Portfolio": len(assignments_df),
        "Reachable Portfolio": len(reachable_joined),
        "Contacted Portfolio": management_df.filter(pl.col("type_call_answer") != "Desconocido").select(pl.col("cuenta").n_unique()).item(),
        "Active Agents": management_df.filter(~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"])).select(pl.col("asesor_gestion").n_unique()).item(),
        "Total Call Attempts": management_df.filter(pl.col("type_call_answer").is_in(["Contestada", "Gestionado"])).height,
        "Total Answered Calls": management_df.filter(pl.col("type_call_answer") == "Contestada").height,
        "Don’t Know on the Phone": management_df.filter(pl.col("type_correct") == "Si").height,
        "Total Answered Calls (Excl. Short Calls)": management_df.filter(pl.col("time_call") == "Si").height,
        "Unique Customers Reached": management_df.filter(pl.col("type_call_answer") == "Contestada").select(pl.col("cuenta").n_unique()).item(),
        "Unique Customers Reached (Excl. Short Calls)": management_df.filter(pl.col("time_call") == "Si").select(pl.col("cuenta").n_unique()).item(),
        "Total RPCs (Right Party Contacts)": management_df.filter(pl.col("contact_efecty") == "Contacto Efectivo").height,
        "Unique RPCs": management_df.filter(pl.col("contact_efecty") == "Contacto Efectivo").select(pl.col("cuenta").n_unique()).item(),
        "Promises to Pay (PTP)": management_df.filter(pl.col("promise") == "Si").height,
        "Request contract cancelation": management_df.filter(pl.col("cancelation_request") == "Si").height,
        "Debt Not Recognized": management_df.filter(pl.col("debt_rejection") == "Si").height,
        "Payment Already Made": management_df.filter(pl.col("payments_made") == "Si").height,
        "Device Reported Stolen": management_df.filter(pl.col("stolen_equipment") == "Si").height,
        "Device Warranty Access Issue": management_df.filter(pl.col("warranty_issues") == "Si").height,
        "Total AHT": management_df.filter((pl.col("tiempogestion_sec") > 0) & (~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"]))).select(pl.col("tiempogestion_sec").mean()).item(),
        "AHT (Excluding Short Calls)": management_df.filter((pl.col("tiempogestion_sec") > 0) & (~pl.col("asesor_gestion").str.to_lowercase().is_in(["predictivo", "bot"])) & (pl.col("type_call_answer") == "Contestada")).select(pl.col("tiempogestion_sec").mean()).item(),
    }
    
    total_row = pl.DataFrame([pl.Series(k, [v], dtype=final_buckets.schema[k]) for k, v in total_data.items() if k in final_buckets.columns])
    combined = pl.concat([total_row, final_buckets]).unique(subset=["bucket"])
    combined = combined.with_columns(pl.col("bucket").map_elements(get_bucket_weight, return_dtype=pl.Int32).alias("_weight")).sort("_weight").drop("_weight")

    combined = combined.with_columns([
        (pl.when(pl.col("Reachable Portfolio") > 0).then(pl.col("Contacted Portfolio") / pl.col("Reachable Portfolio")).otherwise(0)).alias("% Contacted / Reachable"),
        (pl.when(pl.col("Contacted Portfolio") > 0).then(pl.col("Unique Customers Reached") / pl.col("Contacted Portfolio")).otherwise(0)).alias("% Unique Reaches / Contacted Portfolio"),
        (pl.when(pl.col("Unique Customers Reached") > 0).then(pl.col("Unique RPCs") / pl.col("Unique Customers Reached")).otherwise(0)).alias("% Unique RPCs / Unique Reaches"),
        (pl.when(pl.col("Total RPCs (Right Party Contacts)") > 0).then(pl.col("Promises to Pay (PTP)") / pl.col("Total RPCs (Right Party Contacts)")).otherwise(0)).alias("% PTP / Total RPCs"),
        (pl.when(pl.col("Reachable Portfolio") > 0).then(pl.col("Promises to Pay (PTP)") / pl.col("Reachable Portfolio")).otherwise(0)).alias("% PTP / Reachable Portfolio"),
        (pl.when(pl.col("Active Agents") > 0).then(pl.col("Total Answered Calls") / pl.col("Active Agents")).otherwise(0)).alias("Total Answered Calls per Active Agent"),
        (pl.when(pl.col("Active Agents") > 0).then(pl.col("Total RPCs (Right Party Contacts)") / pl.col("Active Agents")).otherwise(0)).alias("Total RPCs per Active Agent"),
        (pl.when(pl.col("Active Agents") > 0).then(pl.col("Promises to Pay (PTP)") / pl.col("Active Agents")).otherwise(0)).alias("Promises to Pay per Active Agent"),
        pl.lit(0.0).alias("Number of SMS sent"), pl.lit(0.0).alias("Number of WA sent"), pl.lit(0.0).alias("Number of e-mails sent")
    ])

    return combined.fill_null(0).fill_nan(0)

def save_excel_final(kpi_df, management_df, output_path):
    try:
        filename = f"{FOLDER_CONFIG['excel_prefix']}_DASHBOARD_{datetime.now().strftime('%Y-%m-%d_%H%M')}.xlsx"
        full_path = output_path / filename
        
        with pd.ExcelWriter(full_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_fmt = workbook.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#00B050', 'border': 1, 'align': 'center'})
            cell_fmt = workbook.add_format({'border': 1})
            pct_fmt = workbook.add_format({'num_format': '0.00%', 'border': 1}) 
            dec_fmt = workbook.add_format({'num_format': '0.0', 'border': 1})
            time_fmt = workbook.add_format({'num_format': 'hh:mm:ss', 'border': 1})

            ws = workbook.add_worksheet('KPI_Dashboard')
            ws.freeze_panes(1, 0)
            
            headers = ["DCA", "Bucket", "Indicator", "Description", "Data Type", "Value"]
            for c, t in enumerate(headers): 
                ws.write(0, c, t, header_fmt)

            indicators = [
                ("Assigned Portfolio", "Total assigned", "Integer"), ("Reachable Portfolio", "Reachable", "Integer"), 
                ("Contacted Portfolio", "Contacted", "Integer"), ("Active Agents", "Active Agents", "Integer"), 
                ("Total Call Attempts", "Total Attempts", "Integer"), ("Total Answered Calls", "Answered", "Integer"),
                ("Don’t Know on the Phone", "Wrong Num", "Integer"), ("Total Answered Calls (Excl. Short Calls)", "Calls > 15s", "Integer"),
                ("Unique Customers Reached", "Unique Reaches", "Integer"), ("Unique Customers Reached (Excl. Short Calls)", "Unique > 15s", "Integer"),
                ("Total RPCs (Right Party Contacts)", "Total RPCs", "Integer"), ("Unique RPCs", "Unique RPCs", "Integer"),
                ("Promises to Pay (PTP)", "PTP", "Integer"), ("Request contract cancelation", "Cancellations", "Integer"),
                ("Debt Not Recognized", "No Debt", "Integer"), ("Payment Already Made", "Already Paid", "Integer"),
                ("Device Reported Stolen", "Stolen", "Integer"), ("Device Warranty Access Issue", "Warranty", "Integer"),
                ("% Contacted / Reachable", "Ratio", "Percentage"), ("% Unique Reaches / Contacted Portfolio", "Ratio", "Percentage"),
                ("% Unique RPCs / Unique Reaches", "Ratio", "Percentage"), ("% PTP / Total RPCs", "Ratio", "Percentage"),
                ("% PTP / Reachable Portfolio", "Ratio", "Percentage"), ("Total Answered Calls per Active Agent", "Average", "Decimal"),
                ("Total RPCs per Active Agent", "Average", "Decimal"), ("Promises to Pay per Active Agent", "Average", "Decimal"),
                ("Total AHT", "Seconds", "Time"), ("AHT (Excluding Short Calls)", "Seconds", "Time"),
                ("Number of SMS sent", "Total", "Integer"), ("Number of WA sent", "Total", "Integer"), ("Number of e-mails sent", "Total", "Integer")
            ]

            curr = 1
            for _, row in kpi_df.to_pandas().iterrows():
                for ind, desc, dtype in indicators:
                    ws.write(curr, 0, "DCA-001", cell_fmt)
                    ws.write(curr, 1, row['bucket'], cell_fmt)
                    ws.write(curr, 2, ind, cell_fmt)
                    ws.write(curr, 3, desc, cell_fmt)
                    ws.write(curr, 4, dtype, cell_fmt)
                    
                    val = row[ind]
                    
                    if dtype == "Percentage":
                        ws.write(curr, 5, val, pct_fmt)
                    elif dtype == "Decimal":
                        ws.write(curr, 5, val, dec_fmt)
                    elif dtype == "Time":
                        excel_time = float(val) / 86400.0 if (pd.notnull(val) and val != 0) else 0
                        ws.write(curr, 5, excel_time, time_fmt)
                    else:
                        ws.write(curr, 5, val, cell_fmt)
                    
                    curr += 1
                    
                    if ind == "AHT (Excluding Short Calls)":
                        curr += 1 

                curr += 2 

            ws.set_column('A:E', 20)
            ws.set_column('F:F', 15)

            management_df.to_pandas().to_excel(writer, sheet_name='Management_Raw', index=False)
            ws2 = writer.sheets['Management_Raw']
            for c, v in enumerate(management_df.columns): 
                ws2.write(0, c, v, header_fmt)

        print(f"✅ Success! File saved: {full_path}")
    except Exception as e:
        print(f"❌ Save Error: {e}")

def generate_report(in_folder, out_folder):
    start = datetime.now()
    path_in, path_out = Path(in_folder), Path(out_folder)
    path_out.mkdir(parents=True, exist_ok=True)
    
    def load(sub):
        p = path_in / sub
        f = list(p.glob("*.csv"))
        return pl.read_csv(f[0], separator=";", infer_schema_length=10000, ignore_errors=True) if f else None

    assign, clients, mgmt = load("Asignacion"), load("ReporteClientes"), load("ReporteGestion")
    if any(x is None for x in [assign, clients, mgmt]): 
        print("❌ Error: No se encontraron todos los archivos CSV necesarios")
        return

    mgmt = process_management(mgmt)
    dashboard = calculate_kpi_dashboard(assign, clients, mgmt)
    save_excel_final(dashboard, mgmt, path_out)
    print(f"\n✨ FINISHED IN: {(datetime.now()-start).total_seconds():.1f}s")