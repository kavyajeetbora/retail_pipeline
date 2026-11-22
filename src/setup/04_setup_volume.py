# 3b. Create folders inside the landing volume

catalog = 'retail_ci'  # picks up the catalog set by USE CATALOG
landing = f"/Volumes/{catalog}/bronze/landing"

for folder in ["customers", "transaction"]:
    dbutils.fs.mkdirs(f"{landing}/{folder}")

display(dbutils.fs.ls(landing))