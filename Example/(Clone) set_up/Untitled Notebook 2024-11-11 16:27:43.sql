-- Databricks notebook source
# Define the SAS token and Azure Data Lake Storage Gen2 path
sas_token = "<your_sas_token>"  # Replace with your SAS token
storage_account_name = "<your_storage_account_name>"  # Replace with your Storage Account name
container_name = "<your_container_name>"  # Replace with your container name

# Build the ADLS Gen2 URL
file_system_url = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Define the mount point in Databricks (this is where your storage will be mounted)
mount_point = "/mnt/delta_lake"

# Mount the Azure Data Lake Gen2 container to Databricks
dbutils.fs.mount(
    source = file_system_url,
    mount_point = mount_point,
    extra_configs = {
        f"spark.hadoop.fs.azure.sas.{container_name}.{storage_account_name}.dfs.core.windows.net": sas_token
    }
)

# Check if the mount was successful by listing the files in the mount point
dbutils.fs.ls(mount_point)


-- COMMAND ----------

# Define the path to your Delta table (stored in ADLS Gen2 under the mounted path)
delta_table_path = "/mnt/delta_lake/your_delta_table_path"

# Read the Delta table
df = spark.read.format("delta").load(delta_table_path)

# Show the data
df.show()


-- COMMAND ----------


