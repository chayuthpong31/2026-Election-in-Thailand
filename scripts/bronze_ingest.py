import requests
import logging
import json
from pathlib import Path

# Create logger
logger = logging.getLogger(__name__)

def bronze_ingest(): 

    BRONZE_PATH = "/data/bronze/bronze_"

    try:
        logger.info(f"{'='*20} Start Extracting Data {'='*20}")
        # ======================= Extract info_province data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_province.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "info_province.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract info_province successfull ✅")

        # ======================= Extract info_constituency data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_constituency.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "info_constituency.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract info_constituency successfull ✅")

        # ======================= Extract info_party_overview data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_party_overview.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "info_party_overview.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract info_party_overview successfull ✅")

        # ======================= Extract info_mp_candidate data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_mp_candidate.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "info_mp_candidate.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract info_mp_candidate successfull ✅")

        # ======================= Extract info_party_candidate data ==========================================
        response = requests.get("https://static-ectreport69.ect.go.th/data/data/refs/info_party_candidate.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "info_party_candidate.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract info_party_candidate successfull ✅")

        # ======================= Extract stats_cons data ==========================================
        response = requests.get("https://stats-ectreport69.ect.go.th/data/records/stats_cons.json")
        raw_data = response.json()

        target_path = Path(BRONZE_PATH + "stats_cons.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract stats_cons successfull ✅")

        # ======================= Extract stats_party data ==========================================
        response = requests.get("https://stats-ectreport69.ect.go.th/data/records/stats_party.json")
        raw_data = response.json()
        target_path = Path(BRONZE_PATH + "stats_party.json")
        target_path.parent.mkdir(parents=True, exist_ok=True)

        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)

        logger.info("Extract stats_party successfull ✅")

    except Exception as e:
        logger.info(f"Connect failed: {e}")

if __name__ == "__main__":
    bronze_ingest()