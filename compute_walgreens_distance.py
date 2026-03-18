"""
Walgreens Distance Computation Script

For every independent pharmacy in the database, computes the haversine distance
to the nearest Walgreens location and stores it in nearest_walgreens_miles.

Uses scipy.spatial.cKDTree on converted 3D unit-sphere coordinates so the
full 65K x N_walgreens comparison is done in milliseconds instead of hours.

Usage:
    cd "Claude random/M&A dash"
    python compute_walgreens_distance.py
"""
import sqlite3
import numpy as np
import sys
from pathlib import Path

try:
    from scipy.spatial import cKDTree
    USE_SCIPY = True
except ImportError:
    USE_SCIPY = False

DB_PATH = Path(__file__).parent / "pharmacy_intel.db"

EARTH_RADIUS_MILES = 3958.8


def haversine_miles(lat1, lon1, lat2, lon2):
    """Standard haversine formula — returns distance in miles."""
    R = EARTH_RADIUS_MILES
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def latlon_to_xyz(lats, lons):
    """
    Convert lat/lon arrays to 3D unit-sphere (x,y,z) coordinates.
    cKDTree Euclidean distance on unit sphere approximates great-circle distance
    well enough for nearest-neighbor queries; we then convert back to miles.
    """
    lats_r = np.radians(lats)
    lons_r = np.radians(lons)
    x = np.cos(lats_r) * np.cos(lons_r)
    y = np.cos(lats_r) * np.sin(lons_r)
    z = np.sin(lats_r)
    return np.column_stack([x, y, z])


def chord_to_miles(chord_dist):
    """
    Convert chord distance on unit sphere to great-circle miles.
    chord = 2 * sin(angle/2)  =>  angle = 2 * arcsin(chord/2)
    """
    # Clamp to valid arcsin domain
    chord_dist = np.minimum(chord_dist, 2.0)
    return EARTH_RADIUS_MILES * 2 * np.arcsin(chord_dist / 2)


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_column(conn):
    """Add nearest_walgreens_miles column if it doesn't exist yet."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(pharmacies)").fetchall()}
    if "nearest_walgreens_miles" not in existing:
        conn.execute("ALTER TABLE pharmacies ADD COLUMN nearest_walgreens_miles REAL")
        conn.commit()
        print("  Added nearest_walgreens_miles column to pharmacies table.")


def main():
    print("=" * 60)
    print("Walgreens Distance Computation")
    print("=" * 60)

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("Run the main app first to initialize the database.")
        sys.exit(1)

    conn = get_db()
    ensure_column(conn)

    # ── Load Walgreens locations ──────────────────────────────────────────────
    print("\nLoading Walgreens locations...")
    walgreens_rows = conn.execute("""
        SELECT latitude, longitude
        FROM pharmacies
        WHERE (organization_name LIKE '%WALGREEN%'
               OR chain_parent LIKE '%WALGREEN%')
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
    """).fetchall()

    if not walgreens_rows:
        print("No Walgreens locations found in the database.")
        print("Make sure pharmacy data is loaded. Exiting.")
        conn.close()
        sys.exit(1)

    wg_lats = np.array([r[0] for r in walgreens_rows], dtype=np.float64)
    wg_lons = np.array([r[1] for r in walgreens_rows], dtype=np.float64)
    print(f"  Found {len(wg_lats):,} Walgreens locations")

    # ── Load independent pharmacies that need distances ───────────────────────
    print("Loading independent pharmacy locations...")
    pharm_rows = conn.execute("""
        SELECT id, latitude, longitude
        FROM pharmacies
        WHERE is_independent = 1
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
    """).fetchall()

    if not pharm_rows:
        print("No independent pharmacies with lat/lon found. Exiting.")
        conn.close()
        sys.exit(1)

    pharm_ids = [r[0] for r in pharm_rows]
    pharm_lats = np.array([r[1] for r in pharm_rows], dtype=np.float64)
    pharm_lons = np.array([r[2] for r in pharm_rows], dtype=np.float64)
    print(f"  Found {len(pharm_ids):,} independent pharmacies with coordinates")

    # ── Compute nearest-neighbor distances ───────────────────────────────────
    print("\nComputing nearest Walgreens distances...")

    if USE_SCIPY:
        print("  Using scipy.spatial.cKDTree (fast)")
        # Build tree on Walgreens xyz coordinates
        wg_xyz = latlon_to_xyz(wg_lats, wg_lons)
        tree = cKDTree(wg_xyz)

        # Query all independent pharmacies at once
        pharm_xyz = latlon_to_xyz(pharm_lats, pharm_lons)
        chord_dists, _ = tree.query(pharm_xyz, k=1, workers=-1)
        distances_miles = chord_to_miles(chord_dists)
    else:
        print("  scipy not available — using numpy broadcast (slower)")
        wg_lats_r = np.radians(wg_lats)
        wg_lons_r = np.radians(wg_lons)
        distances_miles = np.empty(len(pharm_ids))

        BATCH = 500
        for i in range(0, len(pharm_ids), BATCH):
            b_lats = np.radians(pharm_lats[i:i+BATCH])[:, None]
            b_lons = np.radians(pharm_lons[i:i+BATCH])[:, None]
            dphi = wg_lats_r - b_lats
            dlambda = wg_lons_r - b_lons
            a = (np.sin(dphi / 2) ** 2
                 + np.cos(b_lats) * np.cos(wg_lats_r) * np.sin(dlambda / 2) ** 2)
            d = 2 * EARTH_RADIUS_MILES * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
            distances_miles[i:i+BATCH] = d.min(axis=1)
            if (i // BATCH) % 20 == 0:
                pct = min(100, (i + BATCH) / len(pharm_ids) * 100)
                print(f"    {pct:.0f}%...")

    print(f"  Done. Min={distances_miles.min():.1f} mi, "
          f"Max={distances_miles.max():.1f} mi, "
          f"Median={np.median(distances_miles):.1f} mi")

    # ── Write distances back to the DB ───────────────────────────────────────
    print("\nWriting distances to database...")
    batch_size = 2000
    total = len(pharm_ids)

    for i in range(0, total, batch_size):
        batch_ids = pharm_ids[i:i+batch_size]
        batch_dists = distances_miles[i:i+batch_size]
        updates = [
            (round(float(d), 2), pid)
            for pid, d in zip(batch_ids, batch_dists)
        ]
        conn.executemany(
            "UPDATE pharmacies SET nearest_walgreens_miles = ? WHERE id = ?",
            updates,
        )
        conn.commit()
        pct = min(100, (i + batch_size) / total * 100)
        print(f"  Wrote {min(i+batch_size, total):,} / {total:,} ({pct:.0f}%)")

    # ── Summary ──────────────────────────────────────────────────────────────
    count = conn.execute(
        "SELECT COUNT(*) FROM pharmacies WHERE nearest_walgreens_miles IS NOT NULL"
    ).fetchone()[0]
    farther_than_15 = conn.execute(
        "SELECT COUNT(*) FROM pharmacies WHERE nearest_walgreens_miles > 15 AND is_independent = 1"
    ).fetchone()[0]
    within_15 = conn.execute(
        "SELECT COUNT(*) FROM pharmacies WHERE nearest_walgreens_miles <= 15 AND is_independent = 1"
    ).fetchone()[0]

    conn.close()

    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"  Records with distance: {count:,}")
    print(f"  Within 15 miles of Walgreens: {within_15:,}")
    print(f"  More than 15 miles from Walgreens: {farther_than_15:,}")
    print(f"\nRestart the Streamlit app and re-run enrich_data.py to update scores.")


if __name__ == "__main__":
    main()
