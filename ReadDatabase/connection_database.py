from pyspark.sql import SparkSession

def get_spark_session(config, query=""):
    db_settings = config["DatabaseSettings"]

    spark = SparkSession \
            .builder \
            .appName("Read MySQL Table Demo") \
            .master("local[*]") \
            .config("spark.jars", db_settings["JarPath"]) \
            .config("spark.executor.extraClassPath", db_settings["JarPath"]) \
            .config("spark.executor.extraLibraryPath", db_settings["JarPath"]) \
            .config("spark.driver.extraClassPath", db_settings["JarPath"]) \
            .enableHiveSupport() \
            .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    mysql_db_driver_class = db_settings["JarPath"]
    host_name = db_settings["HostName"]
    port = str(db_settings["Port"])
    user = db_settings["User"]
    password = db_settings["Password"]
    database_name = db_settings["DatabaseName"]

    mysql_jdbc_url = f"jdbc:mysql://{host_name}:{port}/{database_name}"

    df = spark.read.format("jdbc") \
        .option("url", mysql_jdbc_url) \
        .option("dirver", mysql_db_driver_class) \
        .option("dbtable", query) \
        .option("user", user) \
        .option("password", password) \
        .load()

    return df