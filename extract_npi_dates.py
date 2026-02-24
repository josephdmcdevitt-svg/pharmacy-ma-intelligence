"""
Extract date fields from NPI CSV and update the pharmacy_intel.db database.

Reads only the columns we need (NPI + 4 date fields) from the 10GB CSV,
matches against NPIs already in our database, and updates them.
"""
import csv
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

APP_DIR = Path(__file__).parent
DB_PATH = APP_DIR / "pharmacy_intel.db"
CSV_PATH = APP_DIR / "data" / "npidata_pfile_20050523-20260208.csv"

# Columns we need from the CSV (0-indexed)
COL_NPI = 0
COL_ENUMERATION_DATE = 36        # "Provider Enumeration Date"
COL_LAST_UPDATE_DATE = 37        # "Last Update Date"
COL_DEACTIVATION_REASON = 38     # "NPI Deactivation Reason Code"
COL_DEACTIVATION_DATE = 39       # "NPI Deactivation Date"

NEEDED_COLS = {COL_NPI, COL_ENUMERATION_DATE, COL_LAST_UPDATE_DATE,
               COL_DEACTIVATION_REASON, COL_DEACTIVATION_DATE}
MAX_COL = max(NEEDED_COLS)


def add_columns_if_missing(conn):
    """Add the new date columns to the pharmacies table if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(pharmacies)").fetchall()}
    new_cols = [
        ("enumeration_date", "TEXT"),
        ("last_update_date", "TEXT"),
        ("npi_deactivation_date", "TEXT"),
        ("deactivation_reason", "TEXT"),
        ("years_in_operation", "REAL"),
    ]
    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE pharmacies ADD COLUMN {col_name} {col_type}")
            print(f"  Added column: {col_name}")
    conn.commit()


def parse_date(val):
    """Parse MM/DD/YYYY date string to YYYY-MM-DD, return None if invalid."""
    if not val or not val.strip():
        return None
    val = val.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def calc_years(enumeration_date_str):
    """Calculate years in operation from enumeration date string."""
    if not enumeration_date_str:
        return None
    try:
        d = datetime.strptime(enumeration_date_str, "%Y-%m-%d")
        delta = datetime.now() - d
        return round(delta.days / 365.25, 1)
    except ValueError:
        return None


def main():
    if not CSV_PATH.exists():
        print(f"ERROR: CSV not found at {CSV_PATH}")
        sys.exit(1)
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    # Step 1: Add columns
    print("Adding new columns to database...")
    add_columns_if_missing(conn)

    # Step 2: Load all NPIs from our database into a set for fast lookup
    print("Loading NPIs from database...")
    db_npis = {row[0] for row in conn.execute("SELECT npi FROM pharmacies").fetchall()}
    print(f"  {len(db_npis):,} pharmacies in database")

    # Step 3: Read CSV and collect date data for matching NPIs
    print(f"Scanning CSV: {CSV_PATH.name}")
    print("  (This reads a large file — may take a few minutes...)")

    updates = []
    rows_scanned = 0

    with open(CSV_PATH, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header

        for row in reader:
            rows_scanned += 1
            if rows_scanned % 1_000_000 == 0:
                print(f"  Scanned {rows_scanned:,} rows, {len(updates):,} matches...")

            if len(row) <= MAX_COL:
                continue

            npi = row[COL_NPI].strip()
            if npi not in db_npis:
                continue

            enum_date = parse_date(row[COL_ENUMERATION_DATE])
            last_update = parse_date(row[COL_LAST_UPDATE_DATE])
            deact_date = parse_date(row[COL_DEACTIVATION_DATE])
            deact_reason = row[COL_DEACTIVATION_REASON].strip() or None
            years = calc_years(enum_date)

            updates.append((enum_date, last_update, deact_date, deact_reason, years, npi))

    print(f"  Done scanning. {rows_scanned:,} total rows, {len(updates):,} matches.")

    # Step 4: Batch update database
    print("Updating database...")
    conn.executemany("""
        UPDATE pharmacies SET
            enumeration_date = ?,
            last_update_date = ?,
            npi_deactivation_date = ?,
            deactivation_reason = ?,
            years_in_operation = ?
        WHERE npi = ?
    """, updates)
    conn.commit()
    print(f"  Updated {len(updates):,} pharmacy records.")

    # Step 5: Recalculate acquisition scores with new retirement risk factor
    print("Recalculating acquisition scores with retirement risk factor...")
    recalc_scores(conn)

    # Step 6: Quick stats
    print("\n--- Summary ---")
    has_enum = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE enumeration_date IS NOT NULL").fetchone()[0]
    has_update = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE last_update_date IS NOT NULL").fetchone()[0]
    has_deact = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE npi_deactivation_date IS NOT NULL").fetchone()[0]
    long_tenured = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE years_in_operation >= 20").fetchone()[0]
    stale = conn.execute("""
        SELECT COUNT(*) FROM pharmacies
        WHERE last_update_date IS NOT NULL
          AND last_update_date < date('now', '-3 years')
    """).fetchone()[0]

    print(f"  Enumeration date populated: {has_enum:,}")
    print(f"  Last update date populated: {has_update:,}")
    print(f"  Deactivated NPIs: {has_deact:,}")
    print(f"  Long-tenured (20+ years): {long_tenured:,}")
    print(f"  Stale records (3+ years no update): {stale:,}")

    conn.close()
    print("\nDone!")


def recalc_scores(conn):
    """
    Recalculate acquisition_score using the updated 6-factor model:
      - Est. Rx Volume: 30%
      - Low Competition: 20%
      - Aging Population: 20%
      - Retirement Risk: 15%
      - Income Level: 8%
      - Pop Growth: 7%
    """
    pharmacies = conn.execute("""
        SELECT id, estimated_rx_volume, zip_pharmacies_per_10k,
               zip_pct_65_plus, zip_median_income, zip_pop_growth_pct,
               years_in_operation
        FROM pharmacies
    """).fetchall()

    updates = []
    for row in pharmacies:
        pid, rx_vol, comp, pct65, income, pop_growth, years_op = row

        # Rx volume score (0-100): normalize to reasonable pharmacy range
        if rx_vol and rx_vol > 0:
            rx_score = min(100, (rx_vol / 80000) * 100)
        else:
            rx_score = 20  # default for unknown

        # Competition score (0-100): fewer pharmacies per 10k = better
        if comp is not None:
            if comp <= 1:
                comp_score = 100
            elif comp <= 3:
                comp_score = 80
            elif comp <= 5:
                comp_score = 60
            elif comp <= 8:
                comp_score = 40
            elif comp <= 12:
                comp_score = 20
            else:
                comp_score = 10
        else:
            comp_score = 50

        # Aging population score (0-100)
        if pct65 is not None:
            age_score = min(100, (pct65 / 30) * 100)
        else:
            age_score = 50

        # Retirement risk score (0-100): longer operating = higher score
        if years_op is not None:
            if years_op >= 25:
                retire_score = 100
            elif years_op >= 20:
                retire_score = 80
            elif years_op >= 15:
                retire_score = 50
            elif years_op >= 10:
                retire_score = 30
            else:
                retire_score = 10
        else:
            retire_score = 30  # default unknown

        # Income score (0-100)
        if income and income > 0:
            income_score = min(100, (income / 100000) * 100)
        else:
            income_score = 50

        # Pop growth score (0-100)
        if pop_growth is not None:
            growth_score = min(100, max(0, 50 + pop_growth * 5))
        else:
            growth_score = 50

        # Weighted total
        score = (
            rx_score * 0.30 +
            comp_score * 0.20 +
            age_score * 0.20 +
            retire_score * 0.15 +
            income_score * 0.08 +
            growth_score * 0.07
        )

        updates.append((round(score, 2), pid))

    conn.executemany("UPDATE pharmacies SET acquisition_score = ? WHERE id = ?", updates)
    conn.commit()
    print(f"  Recalculated scores for {len(updates):,} pharmacies.")


if __name__ == "__main__":
    main()
