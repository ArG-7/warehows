
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import io

import pandas as pd
import requests
import pandas
from google.cloud import bigquery
import os
import json
import datetime

import sentry_sdk
from sentry_sdk.integrations.gcp import GcpIntegration


sentry_sdk.init(
    dsn="https://c7bfe13908e549c4a6370b6900ab48c5@o4503935593283584.ingest.sentry.io/4503936022413312",
    integrations=[
        GcpIntegration(),
    ],

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    traces_sample_rate=1.0,
)


today = datetime.date.today()
date_to_use=today.strftime("%Y%m15")
print(date_to_use)
client = bigquery.Client()
finalrows=[]

def post_data():
    final_data = pd.DataFrame()
    query_job = client.query("""
with cte2 as (
with cte as (
select customer_id, 
value  as domain 
from `boldspace-main`.public.customer_metadata
WHERE `source` ='semrush' and key='domain')
SELECT * except(c) replace (c as domain)  FROM cte,
unnest (split(domain)) c),
cte3 as (
select 
customer_id, 
value  as country
from `boldspace-main`.public.customer_metadata 
WHERE `source` ='semrush' and key='country'),
cte4 as (
select 
cte2.customer_id,
domain,
ifnull(country,'GB') country,
'country' as geo_type
from cte2 left join cte3 on cte3.customer_id=cte2.customer_id
)
SELECT cte4.customer_id, cte4.domain,cte4.country,cte4.geo_type from cte4
left join `boldspace-main`.public.customers cust
on cust.id=cte4.customer_id
where cust.status <> 2
""")
    results = query_job.result()
    for row in results:
        customer_id=row.customer_id
        domain=row.domain
        country=row.country
        geo_type=row.geo_type
        final_data=final_data.append(call_data(customer_id,domain,country,geo_type))
    print(final_data)
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("target", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("display_date", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("geo", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("traffic", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("users", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("pages_per_visit", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("Customer_id", bigquery.enums.SqlTypeNames.INT64),
    ], )

    table_id = 'boldspace-main.raw.allSemrushTopCountries'
    job = client.load_table_from_dataframe(final_data[["target","display_date","geo","traffic","users","pages_per_visit","Customer_id"]], table_id, job_config=job_config)
    job.result()
    print(final_data)


def call_data(customer_id,domain,country,geo_type):
    url="https://api.semrush.com/analytics/ta/api/v3/geo?target={0}&geo_type={1}&display_limit=5&export_columns=target,display_date,geo,traffic,users,pages_per_visit&key=c875504232e1d42afe98203fbdaff22d&sort_order=traffic_desc".format(domain,geo_type)
    response = requests.request("GET", url)
    r = response.content
    rawData = pd.read_csv(io.StringIO(r.decode('utf-8')),delimiter=';',header=0)
    rawData['Customer_id']=customer_id
    return rawData

"""
def hello_pubsub(*args):
    post_data()
"""
@data_loader
def load_data(*args, **kwargs):
    post_data()
    return {}


@test
def test_output(df, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert df is not None, 'The output is undefined'
