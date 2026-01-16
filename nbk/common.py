import pytz
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

def get_tz():
    return pytz.timezone("America/Sao_Paulo")

def get_zoneinfo():
    return ZoneInfo('America/Sao_Paulo')

def add_reference_date_parameters(base) -> None:
    base.text("date_reference", "", "Date Reference")
    base.dropdown("reprocess_everything", "False", ["True", "False"], "Reprocess Everything")
    return

def get_reference_dates(base) -> list[str]:

    date_reference_str = base.get("date_reference")
    reprocess_everything = True if base.get("reprocess_everything") == 'True' else False

    tz = get_tz()
    zi = get_zoneinfo()
    
    yesterday = datetime.now(tz) - timedelta(days=1)
    first_day = datetime(2022, 6, 21).replace(tzinfo=zi)

    date_reference = datetime.fromisoformat(date_reference_str).replace(tzinfo=zi) if date_reference_str else yesterday

    dates = [date_reference]
    if reprocess_everything:
        for date in range((date_reference - first_day).days):
            dates.append(first_day + timedelta(days=date + 1))

    dates.sort(reverse=False)
    return dates

def use_schema_and_create_if_not_exists(spark, catalog: str, schema: str, prefix: str = 'precos-pmc-') -> bool:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} MANAGED LOCATION 's3://{prefix}{schema}'")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")
    return True

def read_sql_template(path: str, **kwargs):
    
    with open(path, 'r') as f:
        sql = f.read()

    return sql.format_map(kwargs)

def table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    return (
        spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
             .filter(f"tableName = '{table}'")
             .count() > 0
    )
