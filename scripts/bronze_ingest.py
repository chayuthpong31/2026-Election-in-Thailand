from pyspark.sql import SparkSession
import requests
import pandas as pd
import logging

# Create logger
logger = logging.getLogger(__name__)

def bronze_ingest():
    # Get Spark Cluster 
    spark = SparkSession.builder \
        .appName("Election2026-ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    try:
        logger.info(f"{'='*20} Start Extracting Data {'='*20}")
        # ======================= Extract info_province data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_province.json")
        raw_data = response.json()

        data = raw_data["province"]
        df = pd.DataFrame(data)

        df.to_csv("/data/bronze/bronze_info_province.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract info_province successfull ✅")

        # ======================= Extract info_constituency data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_constituency.json")
        raw_data = response.json()

        df = pd.DataFrame(raw_data)
        df_zone_exploded = df.explode('zone')

        df_zone_exploded.to_csv("/data/bronze/bronze_info_constituency.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract info_constituency successfull ✅")

        # ======================= Extract info_party_overview data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_party_overview.json")
        raw_data = response.json()

        df = pd.DataFrame(raw_data)

        df.to_csv("/data/bronze/bronze_info_party_overview.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract info_party_overview successfull ✅")

        # ======================= Extract info_mp_candidate data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_mp_candidate.json")
        raw_data = response.json()

        df = pd.DataFrame(raw_data)

        df.to_csv("/data/bronze/bronze_info_mp_candidate.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract info_mp_candidate successfull ✅")

        # ======================= Extract info_party_candidate data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_party_candidate.json")
        raw_data = response.json()

        df = pd.DataFrame(raw_data)
        df_exploded = df.explode('party_list_candidates')
        candidates_info = df_exploded['party_list_candidates'].apply(pd.Series)
        df_final = pd.concat([df_exploded['party_no'], candidates_info], axis=1)

        df_final.to_csv("/data/bronze/bronze_info_party_candidate.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract info_party_candidate successfull ✅")

        # ======================= Extract stats_cons data ==========================================
        response = requests.get("https://stats-ectreport69.ect.go.th/data/records/stats_cons.json")
        raw_data = response.json()
        data = raw_data["result_province"]
        df = pd.DataFrame(data)

        # 1. prov_summary
        df_prov_summary = pd.json_normalize(data).drop(columns=['result_party', 'constituencies'], errors='ignore')

        # 2. prov_party
        df_prov_party = pd.json_normalize(
            data, 
            record_path=['result_party'], 
            meta=['prov_id'],
            record_prefix='party_'
        )

        # 3. district_candidates (explode 2 layer: constituencies -> candidates)
        df_district_candidates = pd.json_normalize(
            data,
            record_path=['constituencies', 'candidates'],
            meta=['prov_id', ['constituencies', 'cons_id']],
            errors='ignore'
        ).rename(columns={'constituencies.cons_id': 'cons_id'})

        # 4. district_party (explode 2 layer: constituencies -> result_party)
        df_district_party = pd.json_normalize(
            data,
            record_path=['constituencies', 'result_party'],
            meta=['prov_id', ['constituencies', 'cons_id']],
            record_prefix='party_',
            errors='ignore'
        ).rename(columns={'constituencies.cons_id': 'cons_id'})

        
        df_prov_summary.to_csv("/data/bronze/bronze_province_summary.csv", index=False)
        logger.info("Extract province_summary successfull ✅")

        df_prov_party.to_csv("/data/bronze/bronze_province_party_results.csv", index=False)
        logger.info("Extract province_party_results successfull ✅")

        df_district_candidates.to_csv("/data/bronze/bronze_district_candidates_results.csv", index=False)
        logger.info("Extract district_candidates_results successfull ✅")

        df_district_party.to_csv("/data/bronze/bronze_district_party_results.csv", index=False)
        logger.info("Extract district_party_results successfull ✅")

        # ======================= Extract stats_party data ==========================================
        response = requests.get("https://stats-ectreport69.ect.go.th/data/records/stats_party.json")
        raw_data = response.json()
        data = raw_data["result_party"]

        df_party = pd.DataFrame(data).drop(columns=['candidates'], errors='ignore')

        df_party.to_csv("/data/bronze/bronze_party_vote_summary.csv", index=False, encoding='utf-8-sig')
        logger.info("Extract party_vote_summary successfull ✅")

        data_with_candidates = [
            item for item in data 
            if "candidates" in item and item["candidates"] is not None and len(item["candidates"]) > 0
        ]

        if len(data_with_candidates) > 0:
            df_candidates = pd.json_normalize(
                data_with_candidates, 
                record_path=['candidates']
            )
            
            df_candidates.to_csv("/data/bronze/bronze_candidate_votes_detailed.csv", index=False, encoding='utf-8-sig')
            logger.info("Extract candidate_votes_detailed successfull ✅")
        else:
            pass

    except Exception as e:
        logger.info(f"Connect failed: {e}")

if __name__ == "__main__":
    bronze_ingest()