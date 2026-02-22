from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import logging

# Create logger
logger = logging.getLogger(__name__)

def silver_transform():
    # Get Spark Cluster 
    spark = SparkSession.builder \
        .appName("Election2026-ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    BRONZE_PATH = "/data/bronze/bronze_"
    SILVER_PATH = "/data/silver/silver_"
    # ================================ read all files ==================================

    logger.info("Start Reading Files...")
    province_df = spark.read.csv(BRONZE_PATH + "info_province.csv", header=True, inferSchema=True)
    constituency_df = spark.read.csv(BRONZE_PATH + "info_constituency.csv", header=True, inferSchema=True)
    mp_candidate_df = spark.read.csv(BRONZE_PATH + "info_mp_candidate.csv", header=True, inferSchema=True)
    party_df = spark.read.csv(BRONZE_PATH + "info_party_overview.csv", header=True, inferSchema=True)
    party_candidate_df = spark.read.csv(BRONZE_PATH + "info_party_candidate.csv", header=True, inferSchema=True)
    party_vote_summary_df = spark.read.csv(BRONZE_PATH + "party_vote_summary.csv", header=True, inferSchema=True)
    province_summary_df = spark.read.csv(BRONZE_PATH + "province_summary.csv", header=True, inferSchema=True)
    province_party_result_df = spark.read.csv(BRONZE_PATH + "province_party_results.csv", header=True, inferSchema=True)
    candidate_votes_detail_df = spark.read.csv(BRONZE_PATH + "candidate_votes_detailed.csv", header=True, inferSchema=True)
    district_candidates_result_df = spark.read.csv(BRONZE_PATH + "district_candidates_results.csv", header=True, inferSchema=True)
    district_party_result_df = spark.read.csv(BRONZE_PATH + "district_party_results.csv", header=True, inferSchema=True)

    # ======================================================================================

    # ================================= Transform =========================================
    logger.info("Start Transform Data...")
    # Cast column type
    constituency_df = constituency_df.withColumn("registered_vote", constituency_df["registered_vote"].cast(IntegerType()))
    mp_candidate_df = mp_candidate_df.withColumn("mp_app_party_id", mp_candidate_df["mp_app_party_id"].cast(IntegerType()))
    province_party_result_df = province_party_result_df.withColumn("party_party_id", province_party_result_df["party_party_id"].cast(IntegerType()))
    district_candidates_result_df = district_candidates_result_df.withColumn("party_id", district_candidates_result_df["party_id"].cast(IntegerType()))

    # Drop unuse column
    party_vote_summary_df = party_vote_summary_df.drop("party_list_count")
    province_df = province_df.drop("eng")

    # Rename Column
    mp_candidate_df = (mp_candidate_df
                        .withColumnRenamed("mp_app_id","mp_candidate_id")
                        .withColumnRenamed("mp_app_no","mp_candidate_no")
                        .withColumnRenamed("mp_app_party_id","mp_candidate_party_id")
                        .withColumnRenamed("mp_app_name", "mp_candidate_name"))
    
    party_df = party_df.withColumnRenamed("id","party_id")
    province_party_result_df = (province_party_result_df
                                    .withColumnRenamed("party_party_id","party_id")
                                    .withColumnRenamed("party_party_list_vote","party_list_vote")
                                    .withColumnRenamed("party_party_cons_votes","party_cons_votes")
                                    .withColumnRenamed("party_party_list_vote_percent","party_list_vote_percent")
                                    .withColumnRenamed("party_party_cons_votes_percent","party_cons_votes_percent"))
    
    candidate_votes_detail_df = (candidate_votes_detail_df
                                    .withColumnRenamed("mp_app_id","mp_candidate_id")
                                    .withColumnRenamed("mp_app_vote","mp_candidate_vote")
                                    .withColumnRenamed("mp_app_vote_percent","mp_candidate_vote_percent")
                                    .withColumnRenamed("mp_app_rank","mp_candidate_rank"))

    district_candidates_result_df = (district_candidates_result_df
                                    .withColumnRenamed("mp_app_id","mp_candidate_id")
                                    .withColumnRenamed("mp_app_vote","mp_candidate_vote")
                                    .withColumnRenamed("mp_app_vote_percent","mp_candidate_vote_percent")
                                    .withColumnRenamed("mp_app_rank","mp_candidate_rank"))
    
    district_party_result_df = (district_party_result_df
                                    .withColumnRenamed("party_party_id","party_id")
                                    .withColumnRenamed("party_party_list_vote","party_list_vote")
                                    .withColumnRenamed("party_party_list_vote_percent","party_list_vote_percent"))
    
    # ====================================================================================

    # ======================================== Save files =========================================
    
    province_df.toPandas().to_csv(SILVER_PATH + "province.csv", index=False)
    constituency_df.toPandas().to_csv(SILVER_PATH + "constituency.csv", index=False)
    mp_candidate_df.toPandas().to_csv(SILVER_PATH + "mp_candidate.csv", index=False) 
    party_df.toPandas().to_csv(SILVER_PATH + "party.csv", index=False) 
    party_candidate_df.toPandas().to_csv(SILVER_PATH + "party_candidate.csv", index=False)
    party_vote_summary_df.toPandas().to_csv(SILVER_PATH + "party_vote_summary.csv", index=False)
    province_summary_df.toPandas().to_csv(SILVER_PATH + "province_summary.csv", index=False)
    province_party_result_df.toPandas().to_csv(SILVER_PATH + "province_party_result.csv", index=False)
    candidate_votes_detail_df.toPandas().to_csv(SILVER_PATH + "candidate_votes_detail.csv", index=False)
    district_candidates_result_df.toPandas().to_csv(SILVER_PATH + "district_candidates_result.csv", index=False)
    district_party_result_df.toPandas().to_csv(SILVER_PATH + "district_party_result.csv", index=False)

    logger.info("Save files Successfull...")
    # =============================================================================================

if __name__ == "__main__":
    silver_transform()