from flask import Flask, jsonify
import os, datetime, requests, json, pandas as pd
from google.cloud import storage
from io import BytesIO
from datetime import timedelta
from collections import defaultdict

app = Flask(__name__)

# === CONFIG ===
client = storage.Client()
RAW_BUCKET = "gal_raw"
STAGING_BUCKET = "gal_staging"

now = datetime.datetime.now(datetime.timezone.utc).date()
year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
formatted_date = now.isoformat()
end_date = now.strftime("%Y-%m-%d")
today_str = now.strftime("%Y_%m_%d")

HEADERS = {"Content-Type": "application/json"}

# === CLEANUP ===
def clear_daily_update(bucket_name, prefix):
    for blob in client.bucket(bucket_name).list_blobs(prefix=prefix):
        blob.delete()
        print(f"🧹 Удалено: {blob.name}")

# === STORAGE ===
def upload_json_and_parquet(result_json, source_path, file_prefix):
    json_name = f"{today_str}_{file_prefix}.json"
    parquet_name = json_name.replace(".json", ".parquet")
    paths = {
        "raw": f"{source_path}/archive/{year}/{month}/{json_name}",
        "stg_daily": f"{source_path}/daily_update/{parquet_name}",
        "stg_archive": f"{source_path}/archive/{year}/{month}/{parquet_name}"
    }

    clear_daily_update(STAGING_BUCKET, f"{source_path}/daily_update/")

    # JSON → GCS
    json_data = json.dumps(result_json, ensure_ascii=False)
    client.bucket(RAW_BUCKET).blob(paths["raw"]).upload_from_string(json_data, content_type="application/json")

    # JSON → DataFrame
    result = result_json.get("result", {})
    values = result.get("values", result) if isinstance(result, dict) else result
    if not values:
        print(f"⚠️ Нет данных для {file_prefix}")
        return

    if isinstance(values, list):
        df = pd.DataFrame(values)
    else:
        df = pd.json_normalize(values)

    if df.empty:
        print(f"⚠️ Пустой DataFrame для {file_prefix}")
        return

    # ====== DTYPE ENFORCEMENT ======
    # карты приведения типов для стабильной схемы Parquet
    FLOAT_COLS_MAP = {
        "unf_sales": ["price", "quantity", "sum"],
        "unf_orders": ["payment_percent", "price", "quantity", "sum"],
        "unf_receipt_payments": ["sum"],
    }
    ID_COLS = [
        "document_id","order_id","seller_id","buyer_id",
        "nomenclature_id","variation_id","unit_id","category_id",
        "receiver_id","sender_id","contract_id",
    ]
    DATE_COLS = ["date", "document_datetime"]

    def coerce_float(df, col):
        if col in df.columns:
            # Любые int/str → float64; нечисловое → NaN
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # деньги/проценты/кол-ва → float64
    for col in FLOAT_COLS_MAP.get(file_prefix, []):
        coerce_float(df, col)

    # ID → строка (стабильно string, чтобы не прыгало в INT64)
    for col in ID_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Даты → ISO-строки (удобно для PARSE_* в BQ)
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # при необходимости локализуем/нормализуем
            try:
                # если без tz — не трогаем; если есть tz — приведём к UTC
                if getattr(df[col].dt, "tz", None) is not None:
                    df[col] = df[col].dt.tz_convert("UTC")
            except Exception:
                pass
            # сохраняем как строку ISO (BigQuery затем парсит в DATE/TIMESTAMP)
            df[col] = df[col].astype("string")

    # ====== DataFrame → Parquet ======
    buf = BytesIO()
    # engine по умолчанию fastparquet/pyarrow — любой норм, но pyarrow стабильнее
    df.to_parquet(buf, index=False)  # можно указать engine="pyarrow"
    data = buf.getvalue()

    stg = client.bucket(STAGING_BUCKET)
    stg.blob(paths["stg_daily"]).upload_from_string(data, content_type="application/octet-stream")
    stg.blob(paths["stg_archive"]).upload_from_string(data, content_type="application/octet-stream")

SOURCES = [
    {
        "endpoint": "outgoing_payments",
        "params": {
            "auth_code": os.environ.get("AUTH_CODE", ""),        
            "begin_date": "2024-01-01",
            "end_date": end_date,
            "fields": "date, sender_id, sender_name, receiver_id, receiver_name, contract_id, contract_name, sum, document_id, document_number,"
            "document_datetime, document_type, intercompany, active, operation_type, currency_id, currency_name"},
        "source_path": "unf-test/outgoing_payments",
        "file_prefix": "unf_outgoing_payments"
    },
    
       {
        "endpoint": "receipt_payments",
        "params": {
            "auth_code": os.environ.get("AUTH_CODE", ""),
            "begin_date": "2024-01-01",
            "end_date": end_date,
            "fields": "document_id, document_number, document_datetime, document_type, date, receiver_name, sender_name, contract_name, sum,receiver_id, sender_id, contract_id, intercompany, active, operation_type, currency_id, currency_name",
        "conditions": [            {
             "field": "active",
             "cond": "eq",
             "value": True
            },
            {
             "field": "operation _type",
             "cond": "eq",
             "value": "Поставщику"
            },
            {
             "field": "intercompany",
             "cond": "eq",
             "value": False
            },
            {"field": "document_id", "cond": "in", "value": document_ids}]
        },
        "source_path": "unf-test/receipt_payments",
        "file_prefix": "unf_receipt_payments"
    }
    
]

# 🚀 Единый запуск по GET /
@app.route("/", methods=["GET"])
def trigger_all():
    results = []
    for src in SOURCES:
        try:
            url = f"http://192.168.53.27/api_test/{src['endpoint']}"
            resp = requests.get(url, headers={"Content-Type": "application/json"}, json=src["params"], timeout=3600)
            if resp.status_code == 200:
                upload_json_and_parquet(resp.json(), src["source_path"], src["file_prefix"])
                results.append({src["endpoint"]: "✅"})
            else:
                results.append({src["endpoint"]: f"❌ {resp.status_code}"})
        except Exception as e:
            results.append({src["endpoint"]: f"❌ {str(e)}"})

   

    return jsonify(results), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
