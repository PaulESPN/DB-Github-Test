# Databricks notebook source
pip install azure-kusto-data azure-kusto-ingest

# COMMAND ----------

"""A simple example how to use KustoClient."""

from datetime import timedelta

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

######################################################
##                        AUTH                      ##
######################################################

cluster = "https://private-orionadxp.eastus.kusto.windows.net"
client_id = "1847609d-9e9a-4bca-bc6b-b1ba4c929819"
client_secret = ""
authority_id = "49793faf-eb3f-4d99-a0cf-aef7cce79dc1"

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)

client = KustoClient(kcsb)

######################################################
##                       QUERY                      ##
######################################################

# once authenticated, usage is as following
db = "p-appsvc-apimg-applogs-adxdb"
query = "ba_logs | take 10"

response = client.execute(db, query)
dataframe = dataframe_from_result_table(response.primary_results[0])

print(dataframe)