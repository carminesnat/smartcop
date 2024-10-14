from pyspark.sql import SparkSession

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("ProcessRawToTrusted").getOrCreate()

# Configura o acesso ao MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "datalake")
hadoop_conf.set("fs.s3a.secret.key", "datalake")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Defina o caminho de entrada e saída com base na data
import pytz
from datetime import datetime

fuso_horario_brasilia = pytz.timezone('America/Sao_Paulo')
brasilia_time = datetime.now(fuso_horario_brasilia)
today = brasilia_time.strftime('%Y-%m-%d')

raw_path = f"s3a://raw/busposition/{today}"
trusted_path = f"s3a://trusted/busposition/{today}"

routes_trusted_path = f"s3a://trusted/routes/{today}/"
positions_trusted_path = f"s3a://trusted/positions/{today}/"


from pyspark.sql.functions import explode, col
# Exibe a lista de arquivos
files_df = spark.read.format("binaryFile").load(raw_path)
# Itera sobre os arquivos
for row in files_df.collect():
    raw_file_path = row["path"]
    raw_file_date = raw_file_path.split('_')[-1].split('.')[0]
    df_raw = spark.read.json(raw_file_path)
    
    # Explodir o array "l" para acessar as informações de cada linha de ônibus
    df_lines = df_raw.select(explode(col("l")).alias("linha"))

    # DataFrame com as informações de "c" até "qv"
    df_bus_lines = df_lines.select(
        col("linha.cl").alias("codigo_trajeto"),
        col("linha.sl").alias("sentido"),
        col("linha.c").alias("letreiro"),
        col("linha.lt0").alias("terminal_primario"),
        col("linha.lt1").alias("terminal_secundario"),
        col("linha.qv").alias("qnt_veiculos")
    )

    # Explodir o array "vs" para acessar as informações de cada veículo dentro da linha de ônibus
    df_vehicles = df_lines.select(
        col("linha.cl").alias("codigo_trajeto"),
        col("linha.sl").alias("sentido"),
        explode(col("linha.vs")).alias("vehicle")
    )

    # DataFrame com as informações de "p" até "px"
    df_vehicles_position = df_vehicles.select(
        col("codigo_trajeto"),
        col("sentido"),
        col("vehicle.p").alias("prefixo_veiculo"),
        col("vehicle.py").alias("latitude"),
        col("vehicle.px").alias("longitude")
    )
    
    # Salva os DataFrames no caminho definido
    #df_bus_lines.write.mode("overwrite").json(routes_trusted_path + 'routes_' + raw_file_date)
    #df_vehicles_position.write.mode("overwrite").json(positions_trusted_path + 'positions_' + raw_file_date)
