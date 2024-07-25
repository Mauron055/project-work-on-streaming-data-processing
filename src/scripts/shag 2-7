import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Настройки
KAFKA_BOOTSTRAP_SERVERS = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'
KAFKA_SECURITY_PROTOCOL = 'SASL_SSL'
KAFKA_SASL_JAAS_CONFIG = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="ваш_username" password="ваш_пароль";'
KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512'
KAFKA_TOPIC_IN = 'ваш_топик_in'
KAFKA_TOPIC_OUT = 'ваш_топик_out'

POSTGRES_URL = 'jdbc:postgresql://localhost:5432/de'
POSTGRES_DRIVER = 'org.postgresql.Driver'
POSTGRES_TABLE_FEEDBACK = 'subscribers_feedback'
POSTGRES_USER = 'jovyan'
POSTGRES_PASSWORD = 'jovyan'

# Настройка логирования
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Схема входного сообщения из Kafka
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

def create_spark_session():
    """Создает SparkSession."""
    spark = SparkSession.builder 
        .appName("RestaurantSubscribeStreamingService") 
        .config("spark.sql.session.timeZone", "UTC") 
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0") 
        .getOrCreate()
    return spark

def read_kafka_stream(spark):
    """Читает данные из Kafka-стрима."""
    restaurant_read_stream_df = spark.readStream 
        .format('kafka') 
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) 
        .option('kafka.security.protocol', KAFKA_SECURITY_PROTOCOL) 
        .option('kafka.sasl.jaas.config', KAFKA_SASL_JAAS_CONFIG) 
        .option('kafka.sasl.mechanism', KAFKA_SASL_MECHANISM) 
        .option('subscribe', KAFKA_TOPIC_IN) 
        .load()
    return restaurant_read_stream_df

def filter_stream_data(df):
    """Фильтрация данных из Kafka-стрима по времени действия акции."""
    current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))
    filtered_data = df.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value"
    ).withColumn("value", from_json(col("value"), incomming_message_schema)) \
    .filter(col("value.adv_campaign_datetime_start") <= current_timestamp_utc) \
    .filter(col("value.adv_campaign_datetime_end") >= current_timestamp_utc) \
    .select("value.*")
    return filtered_data

def read_subscribers_data(spark):
    """Читает данные о подписчиках из PostgreSQL."""
    subscribers_restaurant_df = spark.read \
                        .format('jdbc') \
                          .option('url', POSTGRES_URL) \
                        .option('driver', POSTGRES_DRIVER) \
                        .option('dbtable', 'subscribers_restaurants') \
                        .option('user', POSTGRES_USER) \
                        .option('password', POSTGRES_PASSWORD) \
                        .load()
    return subscribers_restaurant_df

def join_and_transform_data(filtered_data, subscribers_data):
    """Объединяет и преобразовывает данные."""
    result_df = filtered_data.join(subscribers_data, on=filtered_data["restaurant_id"] == subscribers_data["restaurant_id"], how="inner") \
            .select(
                filtered_data["restaurant_id"],
                filtered_data["adv_campaign_id"],
                filtered_data["adv_campaign_content"],
                filtered_data["adv_campaign_owner"],
                filtered_data["adv_campaign_owner_contact"],
                filtered_data["adv_campaign_datetime_start"],
                filtered_data["adv_campaign_datetime_end"],
                subscribers_data["client_id"],
                filtered_data["datetime_created"],
                lit(int(round(unix_timestamp(current_timestamp())))).alias("trigger_datetime_created")
            )
    return result_df

def write_to_postgresql(df):
    """Сохраняет данные в PostgreSQL."""
    try:
        df.write \
            .format('jdbc') \
            .option('url', POSTGRES_URL) \
            .option('driver', POSTGRES_DRIVER) \
            .option('dbtable', POSTGRES_TABLE_FEEDBACK) \
            .option('user', POSTGRES_USER) \
            .option('password', POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")

def write_to_kafka(df):
    """Сохраняет данные в Kafka."""
    try:
        df_kafka = df.select(to_json(struct(
            "restaurant_id",
            "adv_campaign_id",
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "client_id",
            "datetime_created",
            "trigger_datetime_created"
        )).alias('value'))
        df_kafka \
            .writeStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('kafka.security.protocol', KAFKA_SECURITY_PROTOCOL) \
            .option('kafka.sasl.jaas.config', KAFKA_SASL_JAAS_CONFIG) \
            .option('kafka.sasl.mechanism', KAFKA_SASL_MECHANISM) \
            .option('topic', KAFKA_TOPIC_OUT) \
            .outputMode('append') \
            .start()
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")

def save_to_postgresql_and_kafka(df):
    """Сохраняет данные в PostgreSQL и Kafka."""
    df.persist()
    write_to_postgresql(df)
    write_to_kafka(df)
    df.unpersist()

def main():
    """Основная функция."""
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream(spark)
    filtered_data = filter_stream_data(restaurant_read_stream_df)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    save_to_postgresql_and_kafka(result_df)
    spark.stop()

if __name__ == "__main__":
    main()
