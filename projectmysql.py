import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd

# Constants
MYSQL_CONN_ID = "mysql_conn_id"
GCP_CONN_ID = "gcp_conn_id"
TARGET_PROJECT_ID = "composer1996"
TARGET_DATASET = "mysql_bq_ds"

# Extract tasks
@task()
def get_src_tables():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    sql = """SELECT table_name 
             FROM information_schema.tables 
             WHERE table_schema = 'your_database' 
             AND table_name IN ('DimProduct', 'DimProductSubcategory', 'DimProductCategory')"""
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict

@task()
def load_src_data(tbl_dict: dict):
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    all_tbl_name = []
    start_time = time.time()
    
    for k, v in tbl_dict['table_name'].items():
        all_tbl_name.append(v)
        sql = f'SELECT * FROM {v}'
        df = mysql_hook.get_pandas_df(sql)
        print(f'Importing rows for table {v}...')
        bigquery_hook.insert_rows(table=f'{TARGET_DATASET}.src_{v}', rows=df.to_dict('records'), 
                                  target_project_id=TARGET_PROJECT_ID)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    
    print("Data imported successfully")
    return all_tbl_name

# Transformation tasks
@task()
def transform_srcProduct():
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    sql = f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.src_DimProduct`'
    df = bigquery_hook.get_pandas_df(sql)
    
    revised = df[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey','WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName',
                  'StandardCost','FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint','ListPrice', 'Size', 'SizeRange', 'Weight',
                  'DaysToManufacture','ProductLine', 'DealerPrice', 'Class', 'Style', 'ModelName', 'EnglishDescription', 'StartDate','EndDate', 'Status']]
    revised.fillna({'WeightUnitMeasureCode': '0', 'ProductSubcategoryKey': '0', 'SizeUnitMeasureCode': '0',
                    'StandardCost': '0', 'ListPrice': '0', 'ProductLine': 'NA', 'Class': 'NA', 'Style': 'NA',
                    'Size': 'NA', 'ModelName': 'NA', 'EnglishDescription': 'NA', 'DealerPrice': '0', 'Weight': '0'}, inplace=True)
    revised = revised.rename(columns={"EnglishDescription": "Description", "EnglishProductName":"ProductName"})
    
    bigquery_hook.insert_rows(table=f'{TARGET_DATASET}.stg_DimProduct', rows=revised.to_dict('records'), 
                              target_project_id=TARGET_PROJECT_ID)
    return {"table(s) processed": "Data imported successfully"}

@task()
def transform_srcProductSubcategory():
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    sql = f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.src_DimProductSubcategory`'
    df = bigquery_hook.get_pandas_df(sql)
    
    revised = df[['ProductSubcategoryKey', 'EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey', 'ProductCategoryKey']]
    revised = revised.rename(columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"})
    
    bigquery_hook.insert_rows(table=f'{TARGET_DATASET}.stg_DimProductSubcategory', rows=revised.to_dict('records'), 
                              target_project_id=TARGET_PROJECT_ID)
    return {"table(s) processed": "Data imported successfully"}

@task()
def transform_srcProductCategory():
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    sql = f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.src_DimProductCategory`'
    df = bigquery_hook.get_pandas_df(sql)
    
    revised = df[['ProductCategoryKey', 'ProductCategoryAlternateKey', 'EnglishProductCategoryName']]
    revised = revised.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})
    
    bigquery_hook.insert_rows(table=f'{TARGET_DATASET}.stg_DimProductCategory', rows=revised.to_dict('records'), 
                              target_project_id=TARGET_PROJECT_ID)
    return {"table(s) processed": "Data imported successfully"}

# Load
@task()
def prdProduct_model():
    bigquery_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    
    pc = bigquery_hook.get_pandas_df(f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.stg_DimProductCategory`')
    p = bigquery_hook.get_pandas_df(f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.stg_DimProduct`')
    p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(float).astype(int)
    ps = bigquery_hook.get_pandas_df(f'SELECT * FROM `{TARGET_PROJECT_ID}.{TARGET_DATASET}.stg_DimProductSubcategory`')
    
    merged = p.merge(ps, on='ProductSubcategoryKey').merge(pc, on='ProductCategoryKey')
    
    bigquery_hook.insert_rows(table=f'{TARGET_DATASET}.prd_DimProductCategory', rows=merged.to_dict('records'), 
                              target_project_id=TARGET_PROJECT_ID)
    return {"table(s) processed": "Data imported successfully"}

# DAG definition
with DAG(dag_id="product_etl_dag", schedule_interval="0 9 * * *", start_date=datetime(2024, 6, 26), catchup=False, tags=["product_model"]) as dag:
    
    with TaskGroup("extract_dimProducts_load", tooltip="Extract and load source data") as extract_load_src:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        src_product_tbls >> load_dimProducts

    with TaskGroup("transform_src_product", tooltip="Transform and stage data") as transform_src_product:
        transform_srcProduct = transform_srcProduct()
        transform_srcProductSubcategory = transform_srcProductSubcategory()
        transform_srcProductCategory = transform_srcProductCategory()

    with TaskGroup("load_product_model", tooltip="Final Product model") as load_product_model:
        prd_Product_model = prdProduct_model()

    extract_load_src >> transform_src_product >> load_product_model
