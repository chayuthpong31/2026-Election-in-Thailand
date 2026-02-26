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
    province_summary_df = spark.read.csv(SILVER_PATH + "province_summary.csv", header=True, inferSchema=True)
    district_candidates_result_df = spark.read.csv(SILVER_PATH + "district_candidates_results.csv", header=True, inferSchema=True)
    district_party_result_df = spark.read.csv(SILVER_PATH + "district_party_results.csv", header=True, inferSchema=True)

    # ======================================================================================

    # ================================ Create dim and fact table ==================================
    dim_province = province_df.join(province_summary_df, province_df.prov_id == province_summary_df.prov_id, 'left')
    dim_constituency = constituency_df
    dim_party = party_df
    dim_mp_candidate = mp_candidate_df
    dim_party_candidate = party_candidate_df

    fact_vote_constituency = district_candidates_result_df
    fact_vote_party = district_party_result_df
    # =============================================================================================

    # ================================ Save files ==================================
    dim_province.toPandas().to_csv(GOLD_PATH + "dim_province.csv", index=False, encoding='utf-8-sig')
    dim_constituency.toPandas().to_csv(GOLD_PATH + "dim_constituency.csv", index=False, encoding='utf-8-sig')
    dim_party.toPandas().to_csv(GOLD_PATH + "dim_party.csv", index=False, encoding='utf-8-sig')
    dim_mp_candidate.toPandas().to_csv(GOLD_PATH + "dim_mp_candidate.csv", index=False, encoding='utf-8-sig')
    dim_party_candidate.toPandas().to_csv(GOLD_PATH + "dim_party_candidate.csv", index=False, encoding='utf-8-sig')
    fact_vote_constituency.toPandas().to_csv(GOLD_PATH + "fact_vote_constituency.csv", index=False, encoding='utf-8-sig')
    fact_vote_party.toPandas().to_csv(GOLD_PATH + "fact_vote_party.csv", index=False, encoding='utf-8-sig')
    # ================================ Save files ==================================
    logging.info("Save all files successfull....✅")

if __name__ == "__main__":
    gold_aggregate()