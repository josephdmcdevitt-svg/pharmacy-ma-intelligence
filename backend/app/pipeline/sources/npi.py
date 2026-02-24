"""
NPI data source — downloads and parses the NPPES NPI registry.
Filters for pharmacy taxonomy codes only.
"""
import os
import logging
import zipfile
import glob
import pandas as pd

logger = logging.getLogger(__name__)

NPPES_FULL_URL = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_2024.zip"

# Pharmacy taxonomy codes
PHARMACY_TAXONOMIES = {
    "183500000X",  # Pharmacist
    "3336C0002X",  # Community/Retail Pharmacy
    "3336C0003X",  # Compounding Pharmacy
    "3336C0004X",  # Long Term Care Pharmacy
    "3336H0001X",  # Home Infusion Therapy Pharmacy
    "3336I0012X",  # Institutional Pharmacy
    "3336L0003X",  # Mail Order Pharmacy
    "3336M0002X",  # Military/U.S. Coast Guard Pharmacy
    "3336M0003X",  # Managed Care Organization Pharmacy
    "3336N0007X",  # Nuclear Pharmacy
    "3336S0011X",  # Specialty Pharmacy
    "333600000X",  # Pharmacy
}


def download_nppes(data_dir: str) -> str:
    """Download the NPPES full data file. Returns path to the CSV."""
    os.makedirs(data_dir, exist_ok=True)
    zip_path = os.path.join(data_dir, "nppes_full.zip")
    csv_pattern = os.path.join(data_dir, "npidata_pfile_*.csv")

    # Check if CSV already exists
    existing = glob.glob(csv_pattern)
    if existing:
        logger.info(f"Using existing NPPES CSV: {existing[0]}")
        return existing[0]

    # Download
    import httpx
    logger.info(f"Downloading NPPES data from {NPPES_FULL_URL}")
    logger.info("This is a large file (~700MB) and may take 30-60 minutes...")

    with httpx.stream("GET", NPPES_FULL_URL, follow_redirects=True, timeout=3600) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    if downloaded % (50 * 1024 * 1024) < 1024 * 1024:
                        logger.info(f"  Downloaded {downloaded / 1024 / 1024:.0f}MB / {total / 1024 / 1024:.0f}MB ({pct:.1f}%)")

    # Extract
    logger.info("Extracting NPPES ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.startswith("npidata_pfile_") and name.endswith(".csv"):
                z.extract(name, data_dir)
                csv_path = os.path.join(data_dir, name)
                logger.info(f"Extracted: {csv_path}")
                return csv_path

    raise RuntimeError("No NPI data CSV found in ZIP archive")


def parse_nppes(csv_path: str, chunk_size: int = 10000):
    """
    Parse the NPPES CSV, yielding chunks of pharmacy records.
    Filters for pharmacy taxonomy codes.
    """
    logger.info(f"Parsing NPPES CSV: {csv_path}")

    cols = [
        "NPI", "Entity Type Code", "Provider Organization Name (Legal Business Name)",
        "Provider Other Organization Name", "Provider Other Organization Name Type Code",
        "Provider First Line Business Practice Location Address",
        "Provider Second Line Business Practice Location Address",
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Provider Business Practice Location Address Postal Code",
        "Provider Business Practice Location Address Telephone Number",
        "Provider Business Practice Location Address Fax Number",
        "Healthcare Provider Taxonomy Code_1",
        "Healthcare Provider Taxonomy Code_2",
        "Healthcare Provider Taxonomy Code_3",
        "Authorized Official Last Name",
        "Authorized Official First Name",
        "Authorized Official Title or Position",
        "Authorized Official Telephone Number",
    ]

    for chunk in pd.read_csv(csv_path, usecols=cols, chunksize=chunk_size, low_memory=False, dtype=str):
        records = []
        for _, row in chunk.iterrows():
            # Check if any taxonomy code is pharmacy
            taxos = [
                str(row.get("Healthcare Provider Taxonomy Code_1", "") or "").strip(),
                str(row.get("Healthcare Provider Taxonomy Code_2", "") or "").strip(),
                str(row.get("Healthcare Provider Taxonomy Code_3", "") or "").strip(),
            ]
            matching = [t for t in taxos if t in PHARMACY_TAXONOMIES]
            if not matching:
                continue

            # Only organizations (entity type 2)
            if str(row.get("Entity Type Code", "")).strip() != "2":
                continue

            auth_first = str(row.get("Authorized Official First Name", "") or "").strip()
            auth_last = str(row.get("Authorized Official Last Name", "") or "").strip()
            auth_name = f"{auth_first} {auth_last}".strip() if auth_first or auth_last else None

            record = {
                "npi": str(row["NPI"]).strip(),
                "organization_name": str(row.get("Provider Organization Name (Legal Business Name)", "") or "").strip() or None,
                "dba_name": str(row.get("Provider Other Organization Name", "") or "").strip() or None,
                "entity_type": "organization",
                "address_line1": str(row.get("Provider First Line Business Practice Location Address", "") or "").strip() or None,
                "address_line2": str(row.get("Provider Second Line Business Practice Location Address", "") or "").strip() or None,
                "city": str(row.get("Provider Business Practice Location Address City Name", "") or "").strip() or None,
                "state": str(row.get("Provider Business Practice Location Address State Name", "") or "").strip() or None,
                "zip": str(row.get("Provider Business Practice Location Address Postal Code", "") or "").strip()[:5] or None,
                "phone": str(row.get("Provider Business Practice Location Address Telephone Number", "") or "").strip() or None,
                "fax": str(row.get("Provider Business Practice Location Address Fax Number", "") or "").strip() or None,
                "taxonomy_code": matching[0],
                "authorized_official_name": auth_name,
                "authorized_official_title": str(row.get("Authorized Official Title or Position", "") or "").strip() or None,
                "authorized_official_phone": str(row.get("Authorized Official Telephone Number", "") or "").strip() or None,
            }
            records.append(record)

        if records:
            logger.info(f"  Parsed chunk: {len(records)} pharmacy records from {len(chunk)} total rows")
            yield records
