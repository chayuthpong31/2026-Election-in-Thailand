from pyspark.sql import SparkSession
import logging

# Create logger
logger = logging.getLogger(__name__)

def gold_aggregate():
    # Get Spark Cluster 
    spark = SparkSession.builder \
        .appName("Election2026-ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    SILVER_PATH = "/data/silver/silver_"
    GOLD_PATH = "/data/gold/gold_"

    # ================================ read all files ==================================

    logger.info("Start Reading Files...")
    province_df = spark.read.csv(SILVER_PATH + "info_province.csv", header=True, inferSchema=True)
    constituency_df = spark.read.csv(SILVER_PATH + "info_constituency.csv", header=True, inferSchema=True)
    mp_candidate_df = spark.read.csv(SILVER_PATH + "info_mp_candidate.csv", header=True, inferSchema=True)
    party_df = spark.read.csv(SILVER_PATH + "info_party_overview.csv", header=True, inferSchema=True)
    party_candidate_df = spark.read.csv(SILVER_PATH + "info_party_candidate.csv", header=True, inferSchema=True)
    party_vote_summary_df = spark.read.csv(SILVER_PATH + "party_vote_summary.csv", header=True, inferSchema=True)
    province_summary_df = spark.read.csv(SILVER_PATH + "province_summary.csv", header=True, inferSchema=True)
    province_party_result_df = spark.read.csv(SILVER_PATH + "province_party_results.csv", header=True, inferSchema=True)
    candidate_votes_detail_df = spark.read.csv(SILVER_PATH + "candidate_votes_detailed.csv", header=True, inferSchema=True)
    district_candidates_result_df = spark.read.csv(SILVER_PATH + "district_candidates_results.csv", header=True, inferSchema=True)
    district_party_result_df = spark.read.csv(SILVER_PATH + "district_party_results.csv", header=True, inferSchema=True)

    # ======================================================================================

    # ================================ Aggregation ==================================

    # =============================================================================================
if __name__ == "__main__":
    gold_aggregate()