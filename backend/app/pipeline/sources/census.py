"""
Census / geographic data source.
Provides county-level and RUCC data for geographic enrichment.
"""
import os
import logging

logger = logging.getLogger(__name__)


def download_geographic_data(data_dir: str) -> dict:
    """Load geographic reference data. Returns {fips: county_info} dict."""
    csv_path = os.path.join(data_dir, "county_data.csv")
    if not os.path.exists(csv_path):
        logger.info("No geographic reference data found. Skipping.")
        return {}

    import pandas as pd
    df = pd.read_csv(csv_path, dtype=str)
    result = {}
    for _, row in df.iterrows():
        fips = str(row.get("FIPS", "")).strip()
        if fips:
            result[fips] = {
                "county_name": row.get("County"),
                "state": row.get("State"),
                "rucc_code": row.get("RUCC_2013"),
            }
    return result
