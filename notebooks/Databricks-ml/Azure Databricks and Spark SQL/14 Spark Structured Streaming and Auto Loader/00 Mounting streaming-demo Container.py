# Databricks notebook source
# MAGIC %md
# MAGIC Mount the Storage Account Container to DBFS

# COMMAND ----------

application_id = "d9435118-e6b2-4327-ace1-f807102e53fc"
tenant_id = "53460913-08c9-4e0e-b5e4-b4966acbf95b"
secret = "UVQ8Q~FtEdDABjVJMWhwxApb_QFJsQPz5_aY~cPt"


container_name = "streaming-demo"
storage_account_name = "datalakestorageateya"
mount_point = "/mnt/streaming"


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{application_id}",
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

