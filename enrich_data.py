"""
Data Enrichment Script for Pharmacy Acquisition Intelligence Platform

Pulls real data from public federal sources to enrich the pharmacy database:
1. CMS Medicare Part D Prescriber data (real claim counts by NPI)
2. U.S. Census ACS demographics by ZIP/ZCTA
3. HRSA Health Professional Shortage Area (HPSA) designations
4. Recalculates acquisition scores based on real data only

Usage:
    cd "Claude random/M&A dash"
    python enrich_data.py
"""
import sqlite3
import requests
import time
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "pharmacy_intel.db"

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CMS MEDICARE PART D — Real prescription claim data by NPI
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_medicare_partd():
    """Pull real Medicare Part D prescriber data from CMS API by NPI."""
    print("\n=== CMS Medicare Part D Enrichment ===")
    conn = get_db()

    # Get NPIs that need Medicare data
    npis = conn.execute("""
        SELECT npi FROM pharmacies
        WHERE (medicare_claims_count IS NULL OR medicare_claims_count = 0)
          AND npi IS NOT NULL
    """).fetchall()
    npi_list = [r[0] for r in npis]
    print(f"Found {len(npi_list)} pharmacies needing Medicare data")

    if not npi_list:
        print("All pharmacies already have Medicare data.")
        conn.close()
        return

    # CMS Part D API endpoint
    base_url = "https://data.cms.gov/data-api/v1/dataset/4c25a35d-c715-43d0-afda-c5dbc3e4e4fb/data"

    updated = 0
    errors = 0
    batch_size = 10

    for i in range(0, len(npi_list), batch_size):
        batch = npi_list[i:i + batch_size]
        for npi in batch:
            try:
                resp = requests.get(
                    base_url,
                    params={"filter[Prscrbr_NPI]": npi, "size": 1},
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        rec = data[0]
                        claims = int(rec.get("Tot_Clms", 0) or 0)
                        benes = int(rec.get("Tot_Benes", 0) or 0)
                        cost = float(rec.get("Tot_Drug_Cst", 0) or 0)
                        brand_claims = int(rec.get("Brnd_Tot_Clms", 0) or 0)
                        generic_claims = int(rec.get("Gnrc_Tot_Clms", 0) or 0)
                        opioid_claims = int(rec.get("Opioid_Tot_Clms", 0) or 0)
                        antibiotic_claims = int(rec.get("Antbtc_Tot_Clms", 0) or 0)
                        avg_cost = cost / claims if claims > 0 else None

                        conn.execute("""
                            UPDATE pharmacies SET
                                medicare_claims_count = ?,
                                medicare_beneficiary_count = ?,
                                medicare_total_cost = ?,
                                medicare_brand_claims = ?,
                                medicare_generic_claims = ?,
                                medicare_opioid_claims = ?,
                                medicare_antibiotic_claims = ?,
                                medicare_avg_cost_per_claim = ?
                            WHERE npi = ?
                        """, (claims, benes, cost, brand_claims, generic_claims,
                              opioid_claims, antibiotic_claims, avg_cost, npi))
                        updated += 1
                elif resp.status_code == 429:
                    print("  Rate limited, waiting 30s...")
                    time.sleep(30)
                    continue
            except requests.exceptions.RequestException as e:
                errors += 1
                if errors <= 3:
                    print(f"  Error for NPI {npi}: {e}")

        conn.commit()
        pct = min(100, ((i + batch_size) / len(npi_list)) * 100)
        print(f"  Progress: {pct:.0f}% ({updated} updated, {errors} errors)")
        time.sleep(0.5)  # Rate limiting

    conn.close()
    print(f"Medicare enrichment complete: {updated} updated, {errors} errors")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. CENSUS ACS — Demographics by ZIP/ZCTA
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_census():
    """Pull Census ACS 5-year estimate demographics by ZCTA."""
    print("\n=== Census ACS Enrichment ===")
    conn = get_db()

    # Get unique ZIPs that need census data
    zips = conn.execute("""
        SELECT DISTINCT zip FROM pharmacies
        WHERE zip IS NOT NULL
          AND (zip_population IS NULL
               OR zip_median_age IS NULL
               OR zip_pct_uninsured IS NULL)
    """).fetchall()
    zip_list = [r[0][:5] for r in zips if r[0] and len(r[0]) >= 5]
    zip_list = list(set(zip_list))
    print(f"Found {len(zip_list)} ZIPs needing census data")

    if not zip_list:
        print("All ZIPs already have census data.")
        conn.close()
        return

    # Census ACS API (no key needed for small requests, but key recommended)
    # Variables: B01003_001E=population, B01002_001E=median age,
    # B19013_001E=median income, B27010_001E=total for insurance,
    # S2701_C05_001E=% uninsured, B18101_001E=disability
    base_url = "https://api.census.gov/data/2022/acs/acs5"

    variables = [
        "B01003_001E",  # Total population
        "B01002_001E",  # Median age
        "B19013_001E",  # Median household income
        "B09021_001E",  # Population 65+
        "B18101_001E",  # Total disability status
        "B17001_002E",  # Below poverty level
        "B25001_001E",  # Total housing units
        "B27001_001E",  # Health insurance total
    ]

    updated = 0
    errors = 0

    # Process in batches of ZCTAs
    batch_size = 50
    for i in range(0, len(zip_list), batch_size):
        batch = zip_list[i:i + batch_size]
        zcta_str = ",".join(batch)

        try:
            resp = requests.get(
                base_url,
                params={
                    "get": ",".join(variables),
                    "for": f"zip code tabulation area:{zcta_str}",
                },
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                headers = data[0]
                for row in data[1:]:
                    record = dict(zip(headers, row))
                    zcta = record.get("zip code tabulation area", "")

                    pop = safe_int(record.get("B01003_001E"))
                    median_age = safe_float(record.get("B01002_001E"))
                    median_income = safe_int(record.get("B19013_001E"))
                    pop_65 = safe_int(record.get("B09021_001E"))
                    disability_total = safe_int(record.get("B18101_001E"))
                    poverty = safe_int(record.get("B17001_002E"))
                    housing = safe_int(record.get("B25001_001E"))
                    insurance_total = safe_int(record.get("B27001_001E"))

                    pct_65 = (pop_65 / pop * 100) if pop and pop_65 else None
                    pct_disabled = (disability_total / pop * 100) if pop and disability_total else None
                    pct_poverty = (poverty / pop * 100) if pop and poverty else None

                    conn.execute("""
                        UPDATE pharmacies SET
                            zip_population = COALESCE(?, zip_population),
                            zip_median_age = COALESCE(?, zip_median_age),
                            zip_median_income = COALESCE(?, zip_median_income),
                            zip_pct_65_plus = COALESCE(?, zip_pct_65_plus),
                            zip_pct_disabled = COALESCE(?, zip_pct_disabled),
                            zip_pct_poverty = COALESCE(?, zip_pct_poverty),
                            zip_total_households = COALESCE(?, zip_total_households)
                        WHERE zip LIKE ?
                    """, (pop, median_age, median_income, pct_65, pct_disabled,
                          pct_poverty, housing, f"{zcta}%"))
                    updated += 1

            elif resp.status_code == 204:
                pass  # No data for these ZCTAs
            else:
                errors += 1
                if errors <= 3:
                    print(f"  Census API returned {resp.status_code}")

        except requests.exceptions.RequestException as e:
            errors += 1
            if errors <= 3:
                print(f"  Census API error: {e}")

        conn.commit()
        pct = min(100, ((i + batch_size) / len(zip_list)) * 100)
        print(f"  Progress: {pct:.0f}% ({updated} ZIPs updated)")
        time.sleep(0.3)

    conn.close()
    print(f"Census enrichment complete: {updated} updated, {errors} errors")


def safe_int(val):
    try:
        return int(val) if val and val not in ("-", "null", "N") else None
    except (ValueError, TypeError):
        return None

def safe_float(val):
    try:
        return float(val) if val and val not in ("-", "null", "N") else None
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 3. HRSA HPSA — Health Professional Shortage Areas
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_hpsa():
    """Check HRSA HPSA designations for pharmacy locations."""
    print("\n=== HRSA HPSA Enrichment ===")
    conn = get_db()

    # Get unique state+county combos
    locations = conn.execute("""
        SELECT DISTINCT state, county FROM pharmacies
        WHERE state IS NOT NULL AND county IS NOT NULL
          AND hpsa_designated IS NULL OR hpsa_designated = 0
    """).fetchall()
    print(f"Found {len(locations)} state/county combos to check")

    if not locations:
        print("All locations already checked for HPSA.")
        conn.close()
        return

    # HRSA API
    base_url = "https://data.hrsa.gov/data/download"
    # Alternative: BCD API
    hpsa_url = "https://data.hrsa.gov/api/hpsas"

    updated = 0
    errors = 0

    for state, county in locations:
        try:
            resp = requests.get(
                hpsa_url,
                params={
                    "state": state,
                    "county": county,
                    "disciplineId": 5,  # Pharmacy
                    "status": "D",  # Designated
                },
                timeout=15,
            )

            if resp.status_code == 200:
                data = resp.json()
                if data and len(data) > 0:
                    # Find the best HPSA score for this area
                    best_score = max((int(h.get("hpsaScore", 0) or 0) for h in data), default=0)
                    conn.execute("""
                        UPDATE pharmacies SET
                            hpsa_designated = 1,
                            hpsa_score = ?,
                            medically_underserved = 1
                        WHERE state = ? AND county = ?
                    """, (best_score, state, county))
                    updated += 1
            elif resp.status_code == 429:
                time.sleep(10)
                continue

        except requests.exceptions.RequestException as e:
            errors += 1
            if errors <= 5:
                print(f"  HPSA error for {state}/{county}: {e}")

        time.sleep(0.2)

    conn.commit()
    conn.close()
    print(f"HPSA enrichment complete: {updated} areas designated, {errors} errors")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. COMPETITION DENSITY — Calculate from NPI data already in DB
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_competition():
    """Calculate competition metrics from existing pharmacy data."""
    print("\n=== Competition Density Calculation ===")
    conn = get_db()

    # Count pharmacies per ZIP
    conn.execute("""
        UPDATE pharmacies SET
            zip_pharmacy_count = (
                SELECT COUNT(*) FROM pharmacies p2
                WHERE p2.zip = pharmacies.zip AND p2.npi_deactivation_date IS NULL
            ),
            zip_chain_count = (
                SELECT COUNT(*) FROM pharmacies p2
                WHERE p2.zip = pharmacies.zip AND p2.is_chain = 1 AND p2.npi_deactivation_date IS NULL
            ),
            zip_independent_count = (
                SELECT COUNT(*) FROM pharmacies p2
                WHERE p2.zip = pharmacies.zip AND p2.is_independent = 1 AND p2.npi_deactivation_date IS NULL
            )
        WHERE zip IS NOT NULL
    """)

    # Pharmacies per 10K population
    conn.execute("""
        UPDATE pharmacies SET
            zip_pharmacies_per_10k = CASE
                WHEN zip_population > 0 AND zip_pharmacy_count > 0
                THEN ROUND(CAST(zip_pharmacy_count AS REAL) / zip_population * 10000, 2)
                ELSE NULL
            END
        WHERE zip_population IS NOT NULL AND zip_pharmacy_count IS NOT NULL
    """)

    # Competition score (lower = less competition = better for acquisition)
    conn.execute("""
        UPDATE pharmacies SET
            competition_score = CASE
                WHEN zip_pharmacies_per_10k IS NOT NULL THEN
                    CASE
                        WHEN zip_pharmacies_per_10k <= 1.0 THEN 100
                        WHEN zip_pharmacies_per_10k <= 2.0 THEN 80
                        WHEN zip_pharmacies_per_10k <= 3.0 THEN 60
                        WHEN zip_pharmacies_per_10k <= 5.0 THEN 40
                        WHEN zip_pharmacies_per_10k <= 8.0 THEN 20
                        ELSE 10
                    END
                ELSE NULL
            END
    """)

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE zip_pharmacy_count IS NOT NULL").fetchone()[0]
    conn.close()
    print(f"Competition calculated for {count:,} pharmacies")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. ACQUISITION SCORE — Calculated from real data only
# ═══════════════════════════════════════════════════════════════════════════════

def recalculate_scores():
    """Recalculate acquisition scores using only real, verified data."""
    print("\n=== Recalculating Acquisition Scores ===")
    conn = get_db()

    # Get max values for normalization
    max_claims = conn.execute(
        "SELECT MAX(medicare_claims_count) FROM pharmacies WHERE is_independent = 1"
    ).fetchone()[0] or 1
    max_income = conn.execute(
        "SELECT MAX(zip_median_income) FROM pharmacies WHERE zip_median_income > 0"
    ).fetchone()[0] or 1

    # Score each pharmacy
    pharmacies = conn.execute("""
        SELECT id, medicare_claims_count, competition_score, zip_pct_65_plus,
               years_in_operation, hpsa_designated, zip_median_income,
               zip_pop_growth_pct, nearest_walgreens_miles
        FROM pharmacies WHERE is_independent = 1
    """).fetchall()

    scored = 0
    for p in pharmacies:
        pid = p[0]
        claims = p[1] or 0
        competition = p[2] or 50
        pct_65 = p[3] or 0
        years = p[4] or 0
        hpsa = p[5] or 0
        income = p[6] or 0
        growth = p[7] or 0
        wg_miles = p[8]  # may be None

        # Medicare Claims (25%) — normalized to 0-100 scale
        claims_score = min(100, (claims / max_claims * 100)) if max_claims > 0 else 0

        # Competition Density (20%) — already 0-100, lower density = higher score
        comp_score = competition

        # Aging Population (15%) — % 65+ scaled
        age_score = min(100, pct_65 * 4) if pct_65 else 0  # 25% = perfect score

        # Retirement Risk (15%) — years in operation
        if years >= 30:
            retire_score = 100
        elif years >= 25:
            retire_score = 85
        elif years >= 20:
            retire_score = 70
        elif years >= 15:
            retire_score = 50
        elif years >= 10:
            retire_score = 30
        else:
            retire_score = 10

        # HPSA Designation (10%) — binary bonus
        hpsa_score = 100 if hpsa else 0

        # Income / Payer Mix (8%) — normalized
        income_score = min(100, (income / max_income * 100)) if max_income > 0 and income else 0

        # Population Growth (7%)
        if growth and growth > 5:
            growth_score = 100
        elif growth and growth > 2:
            growth_score = 75
        elif growth and growth > 0:
            growth_score = 50
        elif growth and growth > -2:
            growth_score = 25
        else:
            growth_score = 10

        # Weighted total (7 factors sum to 100%)
        total = (
            claims_score * 0.25 +
            comp_score * 0.20 +
            age_score * 0.15 +
            retire_score * 0.15 +
            hpsa_score * 0.10 +
            income_score * 0.08 +
            growth_score * 0.07
        )

        # ── Walgreens Distance Adjustment (post-hoc penalty/bonus) ────────
        # Rationale: Pharmacies far from a Walgreens are harder to integrate
        # into a retail chain acquisition and likely lack chain-level foot traffic.
        # Being very close to a Walgreens is a mild positive (proven market),
        # but being far away is a significant red flag.
        if wg_miles is not None:
            if wg_miles > 15:
                # Severe penalty for being too far from any Walgreens footprint.
                # Scale: 16 mi = -15 pts, 30 mi = -20 pts, 50+ mi = -25 pts max.
                penalty = min(25, 15 + (wg_miles - 15) * 0.4)
                total = max(0, total - penalty)
            else:
                # Small bonus for being within 15 miles — closer = slightly better,
                # but capped at 5 points so it doesn't dominate the score.
                # 0 mi = +5, 7.5 mi = +2.5, 15 mi = 0
                bonus = max(0, 5 * (1 - wg_miles / 15))
                total = min(100, total + bonus)

        conn.execute(
            "UPDATE pharmacies SET acquisition_score = ? WHERE id = ?",
            (round(total, 1), pid),
        )
        scored += 1

    conn.commit()
    conn.close()
    print(f"Scored {scored:,} pharmacies")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("Pharmacy Acquisition Intelligence — Data Enrichment")
    print("=" * 60)

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("Run the main app first to initialize the database.")
        sys.exit(1)

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    conn.close()
    print(f"\nDatabase: {DB_PATH}")
    print(f"Total pharmacies: {total:,}")

    if total == 0:
        print("No pharmacies in database. Run the pipeline first.")
        sys.exit(1)

    # Run enrichment steps
    print("\nStep 1/5: Medicare Part D data (CMS API)...")
    try:
        enrich_medicare_partd()
    except Exception as e:
        print(f"  Medicare enrichment failed: {e}")
        print("  Continuing with other enrichments...")

    print("\nStep 2/5: Census ACS demographics...")
    try:
        enrich_census()
    except Exception as e:
        print(f"  Census enrichment failed: {e}")
        print("  Continuing with other enrichments...")

    print("\nStep 3/5: HRSA HPSA designations...")
    try:
        enrich_hpsa()
    except Exception as e:
        print(f"  HPSA enrichment failed: {e}")
        print("  Continuing with other enrichments...")

    print("\nStep 4/5: Competition density calculation...")
    try:
        calculate_competition()
    except Exception as e:
        print(f"  Competition calculation failed: {e}")

    print("\nStep 5/5: Recalculating acquisition scores...")
    try:
        recalculate_scores()
    except Exception as e:
        print(f"  Score calculation failed: {e}")

    # Final summary
    conn = get_db()
    has_medicare = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE medicare_claims_count > 0").fetchone()[0]
    has_census = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE zip_population IS NOT NULL").fetchone()[0]
    has_hpsa = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE hpsa_designated = 1").fetchone()[0]
    has_scores = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    conn.close()

    print("\n" + "=" * 60)
    print("ENRICHMENT COMPLETE")
    print("=" * 60)
    print(f"  With Medicare data: {has_medicare:,}")
    print(f"  With Census data:   {has_census:,}")
    print(f"  In HPSA areas:      {has_hpsa:,}")
    print(f"  With Acq. Scores:   {has_scores:,}")
    print(f"\nRestart the Streamlit app to see updated data.")


if __name__ == "__main__":
    main()
