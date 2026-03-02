from pyspark.sql import SparkSession
import logging
import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASS")

DB_URL = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
DB_PROPERTIES = {
    "user": db_user,
    "password": db_pass,
    "driver": "org.postgresql.Driver"
}

# Create logger
logger = logging.getLogger(__name__)

def load_to_postgres():
    # Get Spark Cluster 
    spark = SparkSession.builder \
        .appName("Election2026-ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    GOLD_PATH = "/data/gold/gold_"

    logger.info("Start Reading file.....")
    # ================================ Read files =========================================
    dim_province = spark.read.csv(GOLD_PATH + "dim_province.csv", header=True, inferSchema=True)
    dim_constituency = spark.read.csv(GOLD_PATH + "dim_constituency.csv", header=True, inferSchema=True)
    dim_zone = spark.read.csv(GOLD_PATH + "dim_zone.csv", header=True, inferSchema=True)
    dim_party = spark.read.csv(GOLD_PATH + "dim_party.csv", header=True, inferSchema=True)
    dim_mp_candidate = spark.read.csv(GOLD_PATH + "dim_mp_candidate.csv", header=True, inferSchema=True)
    dim_party_candidate = spark.read.csv(GOLD_PATH + "dim_party_candidate.csv", header=True, inferSchema=True)
    fact_vote_constituency = spark.read.csv(GOLD_PATH + "fact_vote_constituency.csv", header=True, inferSchema=True)
    fact_vote_party = spark.read.csv(GOLD_PATH + "fact_vote_party.csv", header=True, inferSchema=True)
    # =======================================================================================

    # ================================ Load into PostgreSQL =========================================
    dim_province.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_province", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_province to PostgreSQL successful ✅")

    dim_party.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_party", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_party to PostgreSQL successful ✅")

    dim_constituency.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_constituency", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_constituency to PostgreSQL successful ✅") 

    dim_zone.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_zone", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_zone to PostgreSQL successful ✅")

    dim_mp_candidate.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_mp_candidate", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_mp_candidate to PostgreSQL successful ✅")

    dim_party_candidate.write.jdbc(
        url=DB_URL, 
        table="election_db.dim_party_candidate", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load dim_party_candidate to PostgreSQL successful ✅")

    fact_vote_constituency.write.jdbc(
        url=DB_URL, 
        table="election_db.fact_vote_constituency", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load fact_vote_constituency to PostgreSQL successful ✅")

    fact_vote_party.write.jdbc(
        url=DB_URL, 
        table="election_db.fact_vote_party", 
        mode="append", 
        properties=DB_PROPERTIES
    )
    logger.info("Load fact_vote_party to PostgreSQL successful ✅")
    # =====================================================================================
    logger.info("Load all data to PostgreSQL successful ✅")

if __name__ == "__main__":
    load_to_postgres()