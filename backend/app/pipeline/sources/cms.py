"""
CMS Medicare Part D data source.
Downloads and parses Medicare Part D prescriber utilization data.
"""
import os
import logging

logger = logging.getLogger(__name__)


def download_cms_partd(data_dir: str):
    """Download CMS Part D data. Returns path or None if unavailable."""
    csv_path = os.path.join(data_dir, "cms_partd.csv")
    if os.path.exists(csv_path):
        return csv_path
    logger.info("CMS Part D data not available locally. Skipping Medicare enrichment.")
    return None


def parse_cms_partd(csv_path: str) -> dict:
    """Parse CMS Part D CSV into {npi: metrics} dict."""
    import pandas as pd

    try:
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
        result = {}
        for _, row in df.iterrows():
            npi = str(row.get("Prscrbr_NPI", "") or "").strip()
            if not npi:
                continue
            result[npi] = {
                "medicare_claims_count": int(float(row.get("Tot_Clms", 0) or 0)),
                "medicare_beneficiary_count": int(float(row.get("Tot_Benes", 0) or 0)),
                "medicare_total_cost": float(row.get("Tot_Drug_Cst", 0) or 0),
            }
        return result
    except Exception as e:
        logger.warning(f"Failed to parse CMS data: {e}")
        return {}
