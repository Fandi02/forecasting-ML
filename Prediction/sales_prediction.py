import sys
import os

# Menambahkan direktori project ke sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
from ReadDatabase.connection_database import get_spark_session  
from prophet import Prophet

class Sales:
    def __init__(self, employee_id, employee_name, doc_draft, doc_void, doc_applied, date):
        self.employee_id = employee_id
        self.employee_name = employee_name
        self.doc_draft = doc_draft
        self.doc_void = doc_void
        self.doc_applied = doc_applied
        self.date = date

def load_config(file_path):
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def predict(df, periods):
    df_temp = df[['ds', 'y']].copy()
    df_temp['ds'] = pd.to_datetime(df_temp['ds'])
    model = Prophet()
    model.add_seasonality(name='monthly', period=30, fourier_order=10)  # Optional: add custom seasonality
    model.fit(df_temp)
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    forecast['yhat'] = forecast['yhat'].apply(lambda x: max(int(round(x)), 0))  # Ensure no negative predictions

    result = pd.merge(df_temp, forecast[['ds', 'yhat']], how='outer', on='ds')

    result['y'] = result['y'].fillna(result['yhat'])
    
    return result[['ds', 'y']]

def sales_prediction(employee_id, periods):

    if employee_id is None:
        return None

    config = load_config("appsettings.json")

    # # Drop columns
    # columns_employee_to_drop = ['iInternalId', 'iId', 'szDescription', 'szDivisionId', 'szDepartmentId', 'szUserCreatedId', 'szUserUpdatedId', 'dtmCreated', 'dtmLastUpdated', 'szBranchId', 'szGender', 'dtmBirth', 'dtmJoin', 'dtmStop', 'szIdCard', 'szPassword', 'szSupervisorId']
    # columns_doc_to_drop = ['iInternalId', 'iId', 'dtmDoc', 'szCustomerId', 'szOrderTypeId', 'bCash', 'bInvoiced', 'szPaymentTermId', 'szDocSoId', 'szCarrier', 'szVehicleId', 'szHelper1', 'szHelper2', 'bDirectWarehouse', 'szWarehouseId', 'szStockTypeId', 'szCustomerPO', 'dtmCustomerPO', 'szSalesId', 'szDocStockOutCustomerId', 'szReturnFromId', 'szVehicle2', 'szDriver2', 'szVehicle3', 'szDriver3', 'szDescription', 'szPromoDesc', 'intPrintedCount', 'szBranchId', 'szCompanyId', 'szUserCreatedId', 'szUserUpdatedId', 'dtmLastUpdated', 'dtmMobileTransaction', 'szMobileId', 'szManualNo']

    # # Get table employee
    # employee = f"(select * from dms_pi_employee where szId like '{employee_id}') as employee"
    # spark_df_employee = get_spark_session(config, employee)

    # if spark_df_employee.count() == 0:
    #     return None

    # for col in columns_employee_to_drop:
    #     if col in spark_df_employee.columns:
    #         spark_df_employee = spark_df_employee.drop(col)

    # df_employee = spark_df_employee.toPandas()

    # # Get table docs
    # docs = "(SELECT * FROM dms_sd_docdo) AS docdos"
    # spark_df_docs = get_spark_session(config, docs)

    # for col in columns_doc_to_drop:
    #     if col in spark_df_docs.columns:
    #         spark_df_docs = spark_df_docs.drop(col)

    # df_docs = spark_df_docs.toPandas()

    # # merge
    # merged_df = pd.merge(df_employee, df_docs, how='inner', left_on='szId', right_on='szEmployeeId')

    query = f"(select e.szId, e.szName, doc.szDocId, doc.szEmployeeId, doc.szDocStatus, doc.dtmCreated from dms_pi_employee e join dms_sd_docdo doc on e.szId = doc.szEmployeeId where e.szId like '{employee_id}' order by doc.dtmCreated) as employee"
    spark_response = get_spark_session(config, query)

    convert_to_pandas = spark_response.toPandas()

    # get created and doc status
    df = pd.DataFrame(convert_to_pandas)
    df['ds'] = pd.to_datetime(df['dtmCreated']).dt.date
    df['y'] = df['szDocStatus']

    # group draft
    df_draft_only = df[df['szDocStatus'] == 'Draft']
    df_draft_grouped = df_draft_only.groupby(['ds']).size().reset_index(name="Draft")

    # group void
    df_void_only = df[df['szDocStatus'] == 'Void']
    df_void_grouped = df_void_only.groupby(['ds']).size().reset_index(name="Void")

    # group applied
    df_applied_only = df[df['szDocStatus'] == 'Applied']
    df_applied_grouped = df_applied_only.groupby(['ds']).size().reset_index(name="Applied")

    # Draft
    df_draft = df_draft_grouped[['ds', 'Draft']].rename(columns={'Draft': 'y'})
    forecast_draft = predict(df_draft, periods)

    # Void
    df_void = df_void_grouped[['ds', 'Void']].rename(columns={'Void': 'y'})
    forecast_void = predict(df_void, periods)

    # Applied
    df_applied = df_applied_grouped[['ds', 'Applied']].rename(columns={'Applied': 'y'})
    forecast_applied = predict(df_applied, periods)
    
    data_prediction = Sales(convert_to_pandas['szId'], convert_to_pandas['szName'], forecast_draft, forecast_void, forecast_applied, df['ds'])

    return data_prediction
