from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import logging
import json
from pyspark.sql import functions as F

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

    logger.info(f"{'='*20} Start Transform Data {'='*20}")

    # ======================= Transfrom info_province data ==========================================
    file_path = BRONZE_PATH + "info_province.json"

    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    data = raw_json_data["province"]
    province_df = spark.createDataFrame(data)

    # drop unused column
    province_df = province_df.drop("eng", "total_registered_vote", "total_vote_stations")

    province_df.toPandas().to_csv(SILVER_PATH + "info_province.csv", index=False, encoding='utf-8-sig')
    logger.info("Transform info_province successfull ✅")

    # ======================================================================================

    # ======================= Transfrom info_constituency data ==========================================
    file_path = BRONZE_PATH + "info_constituency.json"

    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    constituency_df = spark.createDataFrame(raw_json_data)
    constituency_df_zone_exploded = constituency_df.withColumn("zone", F.explode("zone"))

    # Cast column type
    constituency_df_zone_exploded = constituency_df_zone_exploded.withColumn("registered_vote", constituency_df_zone_exploded["registered_vote"].cast(IntegerType()))
    
    constituency_df_zone_exploded.toPandas().to_csv(SILVER_PATH + "info_constituency.csv", index=False, encoding='utf-8-sig')
    logger.info("Transform info_constituency successfull ✅")
    # ======================================================================================

    # ======================= Transform info_mp_candidate data ==========================================
    file_path = BRONZE_PATH + "info_mp_candidate.json"

    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    mp_candidate_df = spark.createDataFrame(raw_json_data)

    # Cast column type
    mp_candidate_df = mp_candidate_df.withColumn("mp_app_party_id", mp_candidate_df["mp_app_party_id"].cast(IntegerType()))
    
    # Rename Column
    mp_candidate_df = (mp_candidate_df
                        .withColumnRenamed("mp_app_id","mp_candidate_id")
                        .withColumnRenamed("mp_app_no","mp_candidate_no")
                        .withColumnRenamed("mp_app_party_id","mp_candidate_party_id")
                        .withColumnRenamed("mp_app_name", "mp_candidate_name"))

    # Drop null mp_candidate_party_id
    mp_candidate_df = mp_candidate_df.dropna(subset=["mp_candidate_party_id"])
    
    mp_candidate_df.toPandas().to_csv(SILVER_PATH + "mp_candidate.csv", index=False) 
    logger.info("Transform info_mp_candidate successfull ✅")
    # ======================================================================================

    # ======================= Transform info_party_overview data ==========================================
    file_path = BRONZE_PATH + "info_party_overview.json"

    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    party_df = spark.createDataFrame(raw_json_data)

    # Rename Column
    party_df = party_df.withColumnRenamed("id","party_id")

    party_df.toPandas().to_csv(SILVER_PATH + "party.csv", index=False) 
    logger.info("Transform info_party_overview successfull ✅")
    # ======================================================================================

    # ======================= Transform info_party_candidate data ==========================================
    file_path = BRONZE_PATH + "info_party_candidate.json"
    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in raw_json_data])
    df = spark.read.json(json_rdd)

    df_final = df.withColumn("candidate", F.explode("party_list_candidates")) \
                .select(
                    "party_no", 
                    "candidate.*"  
                )

    # Drop null 
    df_final = df_final.dropna(subset=["party_no","list_no"])

    df_final.toPandas().to_csv(SILVER_PATH + "party_candidate.csv", index=False, encoding='utf-8-sig')

    logger.info("Transform info_party_candidate successfull ✅")
    # ======================================================================================

    # ======================= Transform stats_cons data ==========================================
    file_path = BRONZE_PATH + "stats_cons.json"
    df = spark.read.option("multiLine", "true").json(file_path)

    df_raw = df.select(F.explode("result_province").alias("data")).select("data.*")

    df_prov_summary = df_raw.drop("result_party", "constituencies")

    df_prov_party = df_raw.select("prov_id", F.explode("result_party").alias("party")) \
        .select("prov_id", "party.*") \
        .withColumn("party_id", F.col("party_id").cast(IntegerType())) \
        .withColumnRenamed("party_list_vote", "party_list_vote") 

    df_district_candidates = df_raw.select("prov_id", F.explode("constituencies").alias("cons")) \
        .select("prov_id", F.col("cons.cons_id"), F.explode("cons.candidates").alias("cand")) \
        .select("prov_id", "cons_id", "cand.*") \
        .withColumn("party_id", F.col("party_id").cast(IntegerType())) \
        .withColumnRenamed("mp_app_id", "mp_candidate_id") \
        .withColumnRenamed("mp_app_vote", "mp_candidate_vote")

    df_district_party = df_raw.select("prov_id", F.explode("constituencies").alias("cons")) \
        .select("prov_id", F.col("cons.cons_id"), F.explode("cons.result_party").alias("p_res")) \
        .select("prov_id", "cons_id", "p_res.*") \
        .withColumnRenamed("party_id", "party_id")

    # Drop null value
    df_district_candidates = df_district_candidates.dropna(subset=["prov_id","cons_id","mp_candidate_id","party_id"])
    df_district_party = df_district_party.dropna(subset=["prov_id","cons_id","party_id"])
    df_district_party = df_district_party.filter(F.col("cons_id") != "BKK_0")

    # Save files
    df_prov_summary.toPandas().to_csv(SILVER_PATH + "province_summary.csv", index=False, encoding='utf-8-sig')
    df_prov_party.toPandas().to_csv(SILVER_PATH + "province_party_result.csv", index=False, encoding='utf-8-sig')
    df_district_candidates.toPandas().to_csv(SILVER_PATH + "district_candidates_result.csv", index=False, encoding='utf-8-sig')
    df_district_party.toPandas().to_csv(SILVER_PATH + "district_party_result.csv", index=False, encoding='utf-8-sig')

    logger.info("All Extract & Transform process successful ✅")
    # ======================================================================================

    # ======================= Transform stats_party data ==========================================
    file_path = BRONZE_PATH + "stats_party.json"
    with open(file_path, "r", encoding="utf-8") as f:
        raw_json_data = json.load(f)

    data = raw_json_data["result_party"]

    json_rdd = spark.sparkContext.parallelize([json.dumps(record) for record in data])
    df_raw = spark.read.json(json_rdd)

    df_party = df_raw.drop("candidates", "party_list_count")

    df_party.toPandas().to_csv(SILVER_PATH + "party_vote_summary.csv", index=False, encoding='utf-8-sig')
    logger.info("Extract party_vote_summary successful ✅")


    if "candidates" in df_raw.columns:
        
        df_candidates = df_raw.filter(F.col("candidates").isNotNull()) \
                            .withColumn("cand", F.explode("candidates")) \
                            .select("party_id", "cand.*") 

        df_candidates.toPandas().to_csv(SILVER_PATH + "candidate_votes_detail.csv", index=False, encoding='utf-8-sig')
        logger.info("Transform candidate_votes_detailed successful ✅")
    else:
        pass

    # ======================================================================================
    logger.info("Save files Successfull...")
    
    
if __name__ == "__main__":
    silver_transform()