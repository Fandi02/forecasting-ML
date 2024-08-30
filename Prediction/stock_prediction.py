import sys
import os

# Menambahkan direktori project ke sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
from ReadDatabase.connection_database import get_spark_session  
from prophet import Prophet

class Sales:
    def __init__(self, product_id, product_name, unit_stock, product_warehouse, product_employee, product_customer, date):
        self.product_id = product_id
        self.product_name = product_name
        self.unit_stock = unit_stock
        self.product_warehouse = product_warehouse
        self.product_employee = product_employee
        self.product_customer = product_customer
        self.date = date

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def predict(df, periods):
    df_temp = df[['ds', 'y']].copy()
    df_temp['ds'] = pd.to_datetime(df_temp['ds'])
    model = Prophet()
    model.add_seasonality(name='monthly', period=30, fourier_order=5)  # Optional: add custom seasonality
    model.fit(df_temp)
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    forecast['yhat'] = forecast['yhat'].apply(lambda x: max(int(round(x)), 0))  # Ensure no negative predictions

    result = pd.merge(df_temp, forecast[['ds', 'yhat']], how='outer', on='ds')

    result['y'] = result['y'].fillna(result['yhat'])
    
    return result[['ds', 'y']]

def stock_prediction(product_id, periods):

    if product_id is None:
        return None

    config = load_config("appsettings.json")

    query = f"(select s.szProductId, s.szLocationType, s.decQtyOnHand, s.szUomId, s.dtmCreated, p.szId, p.szName from dms_inv_stockonhand s join dms_inv_product p on s.szProductId = p.szId where p.szId like '{product_id}' order by s.dtmCreated) as product"
    spark_response = get_spark_session(config, query)

    convert_to_pandas = spark_response.toPandas()

    # get created and doc status
    df = pd.DataFrame(convert_to_pandas)
    df['ds'] = pd.to_datetime(df['dtmCreated']).dt.date
    df['y'] = df['szLocationType']

    # group warehouse
    df_warehouse_only = df[df['szLocationType'] == 'WAREHOUSE']
    df_warehouse_grouped = df_warehouse_only.groupby(['ds'])['decQtyOnHand'].sum().reset_index(name="TotalStockWarehouse")

    # group customer
    df_customer_only = df[df['szLocationType'] == 'CUSTOMER']
    df_customer_grouped = df_customer_only.groupby(['ds'])['decQtyOnHand'].sum().reset_index(name="TotalStockCustomer")

    # group employee
    df_employee_only = df[df['szLocationType'] == 'EMPLOYEE']
    df_employee_grouped = df_employee_only.groupby(['ds'])['decQtyOnHand'].sum().reset_index(name="TotalStockEmployee")

    # Warehouse
    df_warehouse = df_warehouse_grouped[['ds', 'TotalStockWarehouse']].rename(columns={'TotalStockWarehouse': 'y'})
    forecast_warehouse = predict(df_warehouse, periods)

    # Customer
    df_customer = df_customer_grouped[['ds', 'TotalStockCustomer']].rename(columns={'TotalStockCustomer': 'y'})
    forecast_customer = predict(df_customer, periods)

    # Employee
    df_employee = df_employee_grouped[['ds', 'TotalStockEmployee']].rename(columns={'TotalStockEmployee': 'y'})
    forecast_employee = predict(df_employee, periods)
    
    data_prediction = Sales(convert_to_pandas['szId'], convert_to_pandas['szName'], convert_to_pandas['szUomId'], forecast_warehouse, forecast_employee, forecast_customer, df['ds'])

    return data_prediction