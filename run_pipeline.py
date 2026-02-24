"""
Pipeline runner — parses NPI data and loads pharmacies into SQLite.
Run this directly: python3 run_pipeline.py
"""
import pandas as pd
import sqlite3
import os
import re
import hashlib
import time
from datetime import datetime
from pathlib import Path

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
DB_PATH = APP_DIR / "pharmacy_intel.db"

# Pharmacy taxonomy codes
PHARMACY_TAXONOMIES = {
    "183500000X", "3336C0002X", "3336C0003X", "3336C0004X",
    "3336H0001X", "3336I0012X", "3336L0003X", "3336M0002X",
    "3336M0003X", "3336N0007X", "3336S0011X", "333600000X",
}

CHAIN_MAP = {
    "CVS": r"\bCVS\b",
    "WALGREENS": r"\bWALGREEN",
    "WALMART": r"\bWALMART\b",
    "RITE AID": r"\bRITE\s*AID\b",
    "KROGER": r"\bKROGER\b",
    "COSTCO": r"\bCOSTCO\b",
    "SAM'S CLUB": r"\bSAM'?S\s+CLUB\b",
    "TARGET": r"\bTARGET\b",
    "PUBLIX": r"\bPUBLIX\b",
    "H-E-B": r"\bH[\-\s]?E[\-\s]?B\b",
    "ALBERTSONS": r"\bALBERTSON",
    "SAFEWAY": r"\bSAFEWAY\b",
    "MEIJER": r"\bMEIJER\b",
    "WINN-DIXIE": r"\bWINN[\-\s]?DIXIE\b",
    "OMNICARE": r"\bOMNICARE\b",
    "PHARMERICA": r"\bPHARMERICA\b",
    "GENOA": r"\bGENOA\b",
    "EXPRESS SCRIPTS": r"\bEXPRESS\s+SCRIPTS\b",
    "OPTUM RX": r"\bOPTUM\s+RX\b",
    "AMAZON PHARMACY": r"\bAMAZON\s+PHARMACY\b",
}

INSTITUTIONAL_PATTERNS = [
    r"\bHOSPITAL\b", r"\bMEDICAL\s+CENTER\b", r"\bNURSING\b",
    r"\bLONG[\-\s]?TERM\s+CARE\b", r"\bLTC\b", r"\bINFUSION\b",
    r"\bCORRECTIONAL\b", r"\bVETERANS?\b",
]


def normalize_phone(phone):
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def classify(name, dba):
    combined = f"{(name or '').upper()} {(dba or '').upper()}"
    for parent, pattern in CHAIN_MAP.items():
        if re.search(pattern, combined):
            return 1, 0, parent
    return 0, 1, None


def check_institutional(name, dba):
    combined = f"{(name or '').upper()} {(dba or '').upper()}"
    for p in INSTITUTIONAL_PATTERNS:
        if re.search(p, combined):
            return 1
    return 0


def get_ownership_type(name):
    name = (name or "").upper()
    if "LLC" in name:
        return "LLC"
    elif "INC" in name or "INCORPORATED" in name:
        return "Corporation"
    elif "LLP" in name or "PARTNERSHIP" in name:
        return "Partnership"
    elif "PC" in name or "PLLC" in name:
        return "Professional Corporation"
    return "Unknown"


def run():
    # Find CSV
    csv_path = None
    for f in DATA_DIR.glob("npidata_pfile_*.csv"):
        if "fileheader" not in str(f):
            csv_path = f
            break

    if not csv_path:
        print("ERROR: No NPI CSV found in data/ directory")
        return

    print(f"Using: {csv_path.name}")
    print(f"File size: {csv_path.stat().st_size / 1024 / 1024 / 1024:.2f} GB")
    print()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-200000")  # 200MB cache

    # Create tables if needed
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pharmacies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT UNIQUE NOT NULL,
            organization_name TEXT, dba_name TEXT, entity_type TEXT,
            address_line1 TEXT, address_line2 TEXT, city TEXT, state TEXT,
            zip TEXT, county TEXT, phone TEXT, fax TEXT,
            taxonomy_code TEXT, taxonomy_description TEXT,
            is_chain INTEGER DEFAULT 0, is_independent INTEGER DEFAULT 1,
            is_institutional INTEGER DEFAULT 0, chain_parent TEXT,
            authorized_official_name TEXT, authorized_official_title TEXT,
            authorized_official_phone TEXT, ownership_type TEXT,
            medicare_claims_count INTEGER, medicare_beneficiary_count INTEGER,
            medicare_total_cost REAL, latitude REAL, longitude REAL,
            dedup_key TEXT, first_seen TEXT, last_refreshed TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pharmacies_npi ON pharmacies(npi);
        CREATE INDEX IF NOT EXISTS idx_pharmacies_state ON pharmacies(state);
        CREATE INDEX IF NOT EXISTS idx_pharmacies_name ON pharmacies(organization_name);
        CREATE INDEX IF NOT EXISTS idx_pharmacies_independent ON pharmacies(is_independent);

        CREATE TABLE IF NOT EXISTS pharmacy_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT, organization_name TEXT, change_type TEXT,
            field_changed TEXT, old_value TEXT, new_value TEXT, detected_at TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT, completed_at TEXT, status TEXT DEFAULT 'pending',
            records_processed INTEGER DEFAULT 0, records_added INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0, changes_detected INTEGER DEFAULT 0,
            error_log TEXT
        );
    """)

    now = datetime.utcnow().isoformat()
    conn.execute("INSERT INTO pipeline_runs (started_at, status) VALUES (?, ?)", (now, "running"))
    conn.commit()
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    cols = [
        "NPI", "Entity Type Code",
        "Provider Organization Name (Legal Business Name)",
        "Provider Other Organization Name",
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

    start_time = time.time()
    total_rows = 0
    pharmacy_count = 0
    batch = []
    batch_size = 5000
    chunk_size = 100000

    print("=" * 60)
    print("STAGE 1: Parsing NPI records for pharmacies...")
    print("=" * 60)

    for chunk in pd.read_csv(str(csv_path), usecols=cols, chunksize=chunk_size, low_memory=False, dtype=str):
        total_rows += len(chunk)

        for _, row in chunk.iterrows():
            taxos = [
                str(row.get("Healthcare Provider Taxonomy Code_1", "") or "").strip(),
                str(row.get("Healthcare Provider Taxonomy Code_2", "") or "").strip(),
                str(row.get("Healthcare Provider Taxonomy Code_3", "") or "").strip(),
            ]
            matching = [t for t in taxos if t in PHARMACY_TAXONOMIES]
            if not matching:
                continue
            if str(row.get("Entity Type Code", "")).strip() != "2":
                continue

            pharmacy_count += 1
            npi = str(row["NPI"]).strip()
            org = str(row.get("Provider Organization Name (Legal Business Name)", "") or "").strip().upper() or None
            dba = str(row.get("Provider Other Organization Name", "") or "").strip().upper() or None
            city = str(row.get("Provider Business Practice Location Address City Name", "") or "").strip() or None
            state = str(row.get("Provider Business Practice Location Address State Name", "") or "").strip().upper() or None
            zip_code = str(row.get("Provider Business Practice Location Address Postal Code", "") or "").strip()[:5] or None
            phone = normalize_phone(str(row.get("Provider Business Practice Location Address Telephone Number", "") or "").strip())
            fax = normalize_phone(str(row.get("Provider Business Practice Location Address Fax Number", "") or "").strip())
            addr1 = str(row.get("Provider First Line Business Practice Location Address", "") or "").strip().upper() or None
            addr2 = str(row.get("Provider Second Line Business Practice Location Address", "") or "").strip() or None

            auth_first = str(row.get("Authorized Official First Name", "") or "").strip()
            auth_last = str(row.get("Authorized Official Last Name", "") or "").strip()
            auth_name = f"{auth_first} {auth_last}".strip() if auth_first or auth_last else None
            auth_title = str(row.get("Authorized Official Title or Position", "") or "").strip() or None
            auth_phone = normalize_phone(str(row.get("Authorized Official Telephone Number", "") or "").strip())

            is_chain, is_indep, parent = classify(org, dba)
            inst = check_institutional(org, dba)
            own = get_ownership_type(org)
            dedup = hashlib.md5(f"{(org or '').upper()}|{(addr1 or '').upper()}|{(zip_code or '')[:5]}".encode()).hexdigest()

            batch.append((
                npi, org, dba, "organization", addr1, addr2, city, state,
                zip_code, phone, fax, matching[0], is_chain, is_indep, inst,
                parent, auth_name, auth_title, auth_phone, own, dedup,
                now, now,
            ))

            if len(batch) >= batch_size:
                conn.executemany("""
                    INSERT OR REPLACE INTO pharmacies (
                        npi, organization_name, dba_name, entity_type,
                        address_line1, address_line2, city, state, zip, phone, fax,
                        taxonomy_code, is_chain, is_independent, is_institutional,
                        chain_parent, authorized_official_name, authorized_official_title,
                        authorized_official_phone, ownership_type, dedup_key,
                        first_seen, last_refreshed
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, batch)
                conn.commit()
                batch = []

        elapsed = time.time() - start_time
        rate = total_rows / elapsed if elapsed > 0 else 0
        print(f"  Scanned {total_rows:>10,} NPI rows | Found {pharmacy_count:>8,} pharmacies | {rate:,.0f} rows/sec | {elapsed:.0f}s")

    # Flush remaining
    if batch:
        conn.executemany("""
            INSERT OR REPLACE INTO pharmacies (
                npi, organization_name, dba_name, entity_type,
                address_line1, address_line2, city, state, zip, phone, fax,
                taxonomy_code, is_chain, is_independent, is_institutional,
                chain_parent, authorized_official_name, authorized_official_title,
                authorized_official_phone, ownership_type, dedup_key,
                first_seen, last_refreshed
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        conn.commit()

    print()
    print("=" * 60)
    print("STAGE 2: Multi-location clustering...")
    print("=" * 60)
    result = conn.execute("""
        UPDATE pharmacies SET is_chain = 1, is_independent = 0, chain_parent = 'Multi-Location Operator'
        WHERE is_independent = 1 AND organization_name IN (
            SELECT organization_name FROM pharmacies
            WHERE is_independent = 1
            GROUP BY organization_name
            HAVING COUNT(*) >= 10
        )
    """)
    conn.commit()
    print(f"  Updated {result.rowcount} records as multi-location operators")

    # Final stats
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    independent = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    chains = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_chain = 1").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM pharmacies WHERE state IS NOT NULL").fetchone()[0]

    elapsed = time.time() - start_time

    conn.execute(
        """UPDATE pipeline_runs SET status=?, completed_at=?, records_processed=?,
           records_added=? WHERE id=?""",
        ("completed", datetime.utcnow().isoformat(), pharmacy_count, pharmacy_count, run_id),
    )
    conn.commit()
    conn.close()

    print()
    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Total NPI rows scanned:  {total_rows:,}")
    print(f"  Pharmacy records loaded: {total:,}")
    print(f"  Independent:             {independent:,}")
    print(f"  Chain:                   {chains:,}")
    print(f"  States covered:          {states}")
    print(f"  Time elapsed:            {elapsed:.0f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    run()
