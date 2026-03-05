"""
Pharmacy Acquisition Intelligence Platform

M&A intelligence dashboard for prescription file acquisitions.
Uses real CMS Medicare Part D data, NPPES registry, Census demographics,
and HRSA shortage area designations. No estimated or fabricated metrics.
"""
import json
import sys
import os
import shutil
import streamlit as st
import pandas as pd
import sqlite3
import re
import hashlib
import time
from datetime import datetime
from pathlib import Path

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# On Streamlit Cloud the repo is read-only, so copy DB to /tmp for writes
_SOURCE_DB = APP_DIR / "pharmacy_intel.db"
DB_PATH = Path("/tmp/pharmacy_intel.db")
if not DB_PATH.exists():
    if _SOURCE_DB.exists():
        shutil.copy2(str(_SOURCE_DB), str(DB_PATH))
    else:
        # Fallback: use source path directly (local dev)
        DB_PATH = _SOURCE_DB

st.set_page_config(
    page_title="Pharmacy Acquisition Intelligence",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════════
# CUSTOM CSS — Tables, Cards, Animations, Responsive, Filters
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
/* ── Global ─────────────────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

.main .block-container {
    padding-top: 1.5rem;
    max-width: 1400px;
}

h1, h2, h3, h4 { font-family: 'Inter', sans-serif; }

/* ── Metric Cards with animation ────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    animation: cardFadeIn 0.5s ease-out;
}
[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(0,0,0,0.1);
    border-color: #3b82f6;
}
[data-testid="stMetric"] label {
    color: #64748b !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #1e293b !important;
    font-weight: 700 !important;
    font-size: 1.6rem !important;
}

@keyframes cardFadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}

/* ── Dataframe / Table styling ──────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    animation: tableSlideIn 0.4s ease-out;
}
[data-testid="stDataFrame"] table {
    border-collapse: separate;
    border-spacing: 0;
}
[data-testid="stDataFrame"] thead th {
    background: #1e293b !important;
    color: #f8fafc !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 10px 12px !important;
    position: sticky;
    top: 0;
    z-index: 10;
    border-bottom: 2px solid #3b82f6 !important;
}
[data-testid="stDataFrame"] tbody tr:nth-child(even) {
    background-color: #f8fafc;
}
[data-testid="stDataFrame"] tbody tr:nth-child(odd) {
    background-color: #ffffff;
}
[data-testid="stDataFrame"] tbody tr:hover {
    background-color: #eff6ff !important;
    cursor: pointer;
    transition: background-color 0.15s ease;
}
[data-testid="stDataFrame"] tbody td {
    padding: 8px 12px !important;
    font-size: 0.85rem !important;
    border-bottom: 1px solid #f1f5f9 !important;
}

@keyframes tableSlideIn {
    from { opacity: 0; transform: translateY(15px); }
    to { opacity: 1; transform: translateY(0); }
}

/* ── Sidebar styling ────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
}
[data-testid="stSidebar"] * {
    color: #e2e8f0 !important;
}
[data-testid="stSidebar"] [data-testid="stMetric"] {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
}
[data-testid="stSidebar"] [data-testid="stMetric"] label {
    color: #94a3b8 !important;
}
[data-testid="stSidebar"] [data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
}

/* ── Filter panel styling ───────────────────────────────────────────────── */
.filter-panel {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 16px;
    animation: cardFadeIn 0.4s ease-out;
}

[data-testid="stSelectbox"] > div > div {
    border-radius: 8px !important;
    border: 1px solid #cbd5e1 !important;
    transition: all 0.2s ease;
}
[data-testid="stSelectbox"] > div > div:hover {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 2px rgba(59,130,246,0.1);
}
[data-testid="stSelectbox"] > div > div:focus-within {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15);
}

[data-testid="stTextInput"] > div > div > input {
    border-radius: 8px !important;
    border: 1px solid #cbd5e1 !important;
    transition: all 0.2s ease;
    padding: 8px 12px !important;
}
[data-testid="stTextInput"] > div > div > input:hover {
    border-color: #3b82f6 !important;
}
[data-testid="stTextInput"] > div > div > input:focus {
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.15) !important;
}

/* ── Toggle switch styling ──────────────────────────────────────────────── */
[data-testid="stToggle"] {
    padding: 4px 0;
}

/* ── Buttons ────────────────────────────────────────────────────────────── */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: all 0.2s ease !important;
    border: 1px solid #e2e8f0 !important;
}
.stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1) !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
    border: none !important;
}

/* ── Expanders ──────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    overflow: hidden;
    transition: all 0.2s ease;
}
[data-testid="stExpander"]:hover {
    border-color: #cbd5e1;
}

/* ── Tabs ───────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: #f1f5f9;
    border-radius: 10px;
    padding: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    font-weight: 500;
    transition: all 0.2s ease;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: white;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

/* ── Download buttons ───────────────────────────────────────────────────── */
[data-testid="stDownloadButton"] > button {
    background: linear-gradient(135deg, #10b981, #059669) !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
}

/* ── Dividers ───────────────────────────────────────────────────────────── */
hr {
    border: none;
    height: 1px;
    background: linear-gradient(to right, transparent, #e2e8f0, transparent);
    margin: 16px 0;
}

/* ── Score badges ───────────────────────────────────────────────────────── */
.score-high { color: #059669; font-weight: 700; }
.score-mid { color: #d97706; font-weight: 600; }
.score-low { color: #dc2626; font-weight: 500; }

/* ── Responsive ─────────────────────────────────────────────────────────── */
@media (max-width: 768px) {
    .main .block-container {
        padding-left: 1rem;
        padding-right: 1rem;
    }
    [data-testid="stMetric"] {
        padding: 10px 12px;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.2rem !important;
    }
    [data-testid="stDataFrame"] thead th {
        font-size: 0.7rem !important;
        padding: 6px 8px !important;
    }
    [data-testid="stDataFrame"] tbody td {
        font-size: 0.75rem !important;
        padding: 6px 8px !important;
    }
    h1 { font-size: 1.5rem !important; }
    h2 { font-size: 1.2rem !important; }
}

@media (max-width: 480px) {
    [data-testid="stMetric"] {
        padding: 8px 10px;
        border-radius: 8px;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1rem !important;
    }
    [data-testid="stMetric"] label {
        font-size: 0.65rem !important;
    }
}

/* ── Loading shimmer animation ──────────────────────────────────────────── */
@keyframes shimmer {
    0% { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}
.loading-shimmer {
    background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%);
    background-size: 200% 100%;
    animation: shimmer 1.5s infinite;
    border-radius: 8px;
    height: 20px;
}
</style>
""", unsafe_allow_html=True)

# ─── Authentication ──────────────────────────────────────────────────────────

def check_login():
    if st.session_state.get("authenticated"):
        return True
    st.markdown(
        "<div style='max-width:400px;margin:15vh auto;text-align:center'>"
        "<h1>💊 Pharmacy Intel</h1>"
        "<p style='color:gray'>Acquisition Intelligence Platform</p></div>",
        unsafe_allow_html=True,
    )
    with st.container():
        col_l, col_form, col_r = st.columns([1, 1, 1])
        with col_form:
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Sign In", use_container_width=True)
                if submitted:
                    if username == "admin" and password == "merger":
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
    return False

if not check_login():
    st.stop()

# ─── Database ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pharmacies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            npi TEXT UNIQUE NOT NULL, organization_name TEXT, dba_name TEXT,
            entity_type TEXT, address_line1 TEXT, address_line2 TEXT,
            city TEXT, state TEXT, zip TEXT, county TEXT, phone TEXT, fax TEXT,
            taxonomy_code TEXT, taxonomy_description TEXT,
            is_chain INTEGER DEFAULT 0, is_independent INTEGER DEFAULT 1,
            is_institutional INTEGER DEFAULT 0, chain_parent TEXT,
            authorized_official_name TEXT, authorized_official_title TEXT,
            authorized_official_phone TEXT, ownership_type TEXT,
            /* Real CMS Medicare Part D data */
            medicare_claims_count INTEGER,
            medicare_beneficiary_count INTEGER,
            medicare_total_cost REAL,
            medicare_brand_claims INTEGER,
            medicare_generic_claims INTEGER,
            medicare_opioid_claims INTEGER,
            medicare_antibiotic_claims INTEGER,
            medicare_avg_cost_per_claim REAL,
            /* Location */
            latitude REAL, longitude REAL,
            dedup_key TEXT, first_seen TEXT, last_refreshed TEXT,
            /* Census ACS demographics (real data) */
            zip_population INTEGER,
            zip_median_income INTEGER,
            zip_pct_65_plus REAL,
            zip_pop_growth_pct REAL,
            zip_median_age REAL,
            zip_pct_uninsured REAL,
            zip_pct_disabled REAL,
            zip_pct_poverty REAL,
            zip_pct_health_insurance REAL,
            zip_total_households INTEGER,
            zip_pct_owner_occupied REAL,
            /* HRSA designations */
            hpsa_designated INTEGER DEFAULT 0,
            hpsa_score INTEGER,
            medically_underserved INTEGER DEFAULT 0,
            /* Competition metrics (calculated from real NPI data) */
            zip_pharmacy_count INTEGER,
            zip_pharmacies_per_10k REAL,
            zip_chain_count INTEGER,
            zip_independent_count INTEGER,
            competition_score REAL,
            /* Composite scoring */
            market_demand_score REAL,
            acquisition_score REAL,
            /* Contact / Deal tracking */
            contact_email TEXT, contact_notes TEXT,
            deal_status TEXT DEFAULT 'Not Contacted',
            /* NPI registry dates (real data) */
            enumeration_date TEXT, last_update_date TEXT,
            npi_deactivation_date TEXT, deactivation_reason TEXT,
            years_in_operation REAL
        );
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
        CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT UNIQUE,
            last_updated TEXT,
            records_count INTEGER,
            description TEXT
        );
    """)
    # Add columns if they don't exist (for upgrades)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(pharmacies)").fetchall()}
    new_cols = {
        "medicare_brand_claims": "INTEGER",
        "medicare_generic_claims": "INTEGER",
        "medicare_opioid_claims": "INTEGER",
        "medicare_antibiotic_claims": "INTEGER",
        "medicare_avg_cost_per_claim": "REAL",
        "zip_median_age": "REAL",
        "zip_pct_uninsured": "REAL",
        "zip_pct_disabled": "REAL",
        "zip_pct_poverty": "REAL",
        "zip_pct_health_insurance": "REAL",
        "zip_total_households": "INTEGER",
        "zip_pct_owner_occupied": "REAL",
        "hpsa_designated": "INTEGER DEFAULT 0",
        "hpsa_score": "INTEGER",
        "medically_underserved": "INTEGER DEFAULT 0",
        "zip_chain_count": "INTEGER",
        "zip_independent_count": "INTEGER",
    }
    for col, dtype in new_cols.items():
        if col not in existing:
            try:
                conn.execute(f"ALTER TABLE pharmacies ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    conn.commit()
    conn.close()

try:
    init_db()
except Exception as e:
    st.error(f"Database initialization error: {e}")

# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    independent = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    chain = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_chain = 1").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM pharmacies WHERE state IS NOT NULL").fetchone()[0]
    avg_score = conn.execute("SELECT AVG(acquisition_score) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    total_medicare_claims = conn.execute("SELECT SUM(medicare_claims_count) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    total_medicare_cost = conn.execute("SELECT SUM(medicare_total_cost) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    hpsa_count = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE hpsa_designated = 1 AND is_independent = 1").fetchone()[0]
    deal_counts = {}
    for row in conn.execute("SELECT deal_status, COUNT(*) as c FROM pharmacies WHERE deal_status IS NOT NULL AND deal_status != 'Not Contacted' GROUP BY deal_status").fetchall():
        deal_counts[row["deal_status"]] = row["c"]
    conn.close()
    return {
        "total": total, "independent": independent, "chain": chain,
        "states": states, "avg_score": avg_score or 0,
        "total_medicare_claims": total_medicare_claims or 0,
        "total_medicare_cost": total_medicare_cost or 0,
        "hpsa_count": hpsa_count or 0,
        "deals": deal_counts,
    }

def fmt(val, prefix="", suffix=""):
    if val is None: return "—"
    if isinstance(val, float):
        if val >= 1_000_000:
            return f"{prefix}{val/1_000_000:.1f}M{suffix}"
        if val >= 1_000:
            return f"{prefix}{val/1_000:.0f}K{suffix}"
    return f"{prefix}{int(val):,}{suffix}"

def fmt_currency(val):
    if val is None: return "—"
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val/1_000:.0f}K"
    return f"${val:,.0f}"

def get_all_states():
    conn = get_db()
    rows = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM pharmacies WHERE state IS NOT NULL AND is_independent = 1 "
        "GROUP BY state ORDER BY state"
    ).fetchall()
    conn.close()
    return [(r["state"], r["cnt"]) for r in rows]

def search_pharmacies(search="", state="", city="", zip_code="", independent_only=False,
                      min_score=0, sort_by="acquisition_score", page=1, per_page=50):
    conn = get_db()
    conditions = []
    params = []
    if search:
        conditions.append("(organization_name LIKE ? OR dba_name LIKE ? OR city LIKE ? OR npi LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])
    if state:
        conditions.append("state = ?")
        params.append(state)
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")
    if zip_code:
        conditions.append("zip LIKE ?")
        params.append(f"{zip_code}%")
    if independent_only:
        conditions.append("is_independent = 1")
    if min_score > 0:
        conditions.append("acquisition_score >= ?")
        params.append(min_score)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * per_page
    order_map = {
        "acquisition_score": "acquisition_score DESC",
        "medicare_claims_count": "medicare_claims_count DESC",
        "medicare_total_cost": "medicare_total_cost DESC",
        "organization_name": "organization_name ASC",
        "competition_score": "competition_score DESC",
        "zip_pct_65_plus": "zip_pct_65_plus DESC",
        "zip_median_income": "zip_median_income DESC",
        "years_in_operation": "years_in_operation DESC",
        "zip_pct_uninsured": "zip_pct_uninsured DESC",
    }
    order = order_map.get(sort_by, "acquisition_score DESC")
    total = conn.execute(f"SELECT COUNT(*) FROM pharmacies {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM pharmacies {where} ORDER BY {order} NULLS LAST LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    return pd.DataFrame([dict(r) for r in rows]), total

def get_pharmacy_detail(pharmacy_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM pharmacies WHERE id = ?", (pharmacy_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def update_pharmacy_contact(pharmacy_id, email=None, notes=None, deal_status=None):
    conn = get_db()
    if email is not None:
        conn.execute("UPDATE pharmacies SET contact_email = ? WHERE id = ?", (email, pharmacy_id))
    if notes is not None:
        conn.execute("UPDATE pharmacies SET contact_notes = ? WHERE id = ?", (notes, pharmacy_id))
    if deal_status is not None:
        conn.execute("UPDATE pharmacies SET deal_status = ? WHERE id = ?", (deal_status, pharmacy_id))
    conn.commit()
    conn.close()


DEAL_STATUSES = ["Not Contacted", "Researching", "Contacted", "In Discussion",
                 "LOI Sent", "Under Contract", "Closed", "Passed"]

# ─── Sidebar ─────────────────────────────────────────────────────────────────

st.sidebar.markdown("## 💊 Pharmacy Intel")
st.sidebar.caption("Acquisition Intelligence Platform")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Top Targets", "Closing Signals", "Query Tools",
     "Tuck-in Finder", "Directory", "Deal Pipeline", "Market Map", "Data Sources"],
    label_visibility="collapsed",
)

st.sidebar.divider()
stats = get_stats()
if stats["total"] > 0:
    st.sidebar.metric("Independent Targets", f"{stats['independent']:,}")
    st.sidebar.metric("Avg Acq. Score", f"{stats['avg_score']:.1f}")
    if stats["hpsa_count"]:
        st.sidebar.metric("In Shortage Areas", f"{stats['hpsa_count']:,}")


# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

if page == "Dashboard":
    st.title("Dashboard")

    if stats["total"] == 0:
        st.info("No data loaded. Run the pipeline script first — see **Data Sources** page for details.")
    else:
        import plotly.express as px

        # Row 1: Key metrics
        conn_m = get_db()
        strong_buys = conn_m.execute(
            "SELECT COUNT(*) FROM pharmacies WHERE acquisition_score >= 70 AND is_independent = 1"
        ).fetchone()[0]
        avg_medicare_claims = conn_m.execute(
            "SELECT AVG(medicare_claims_count) FROM pharmacies WHERE is_independent = 1 AND medicare_claims_count > 0"
        ).fetchone()[0] or 0
        hpsa_targets = conn_m.execute(
            "SELECT COUNT(*) FROM pharmacies WHERE hpsa_designated = 1 AND is_independent = 1"
        ).fetchone()[0]
        conn_m.close()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Independent Targets", f"{stats['independent']:,}")
        c2.metric("Strong Buys (70+)", f"{strong_buys:,}")
        c3.metric("Avg Acq. Score", f"{stats['avg_score']:.1f}")
        c4.metric("Avg Medicare Claims", f"{avg_medicare_claims:,.0f}")
        c5.metric("In Shortage Areas", f"{hpsa_targets:,}")

        # Deal pipeline summary
        if stats["deals"]:
            st.divider()
            st.subheader("Deal Pipeline")
            deal_cols = st.columns(len(stats["deals"]))
            for i, (status, count) in enumerate(stats["deals"].items()):
                deal_cols[i].metric(status, count)

        st.divider()

        # Charts
        col_chart, col_pie = st.columns([2, 1])

        with col_chart:
            st.subheader("Independent Pharmacies by State (Top 15)")
            conn = get_db()
            top_states = conn.execute(
                "SELECT state, COUNT(*) as cnt FROM pharmacies WHERE state IS NOT NULL AND is_independent = 1 "
                "GROUP BY state ORDER BY cnt DESC LIMIT 15"
            ).fetchall()
            conn.close()
            if top_states:
                ts_df = pd.DataFrame([dict(r) for r in top_states])
                fig = px.bar(ts_df, x="state", y="cnt",
                             labels={"state": "State", "cnt": "Independent Pharmacies"},
                             color_discrete_sequence=["#3b82f6"])
                fig.update_layout(margin=dict(t=10, b=40, l=40, r=10), height=350, showlegend=False,
                                  plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
                fig.update_xaxes(gridcolor="#f1f5f9")
                fig.update_yaxes(gridcolor="#f1f5f9")
                st.plotly_chart(fig, use_container_width=True)

        with col_pie:
            st.subheader("Score Distribution")
            conn = get_db()
            score_dist = conn.execute("""
                SELECT
                    CASE
                        WHEN acquisition_score >= 70 THEN 'Strong Buy (70+)'
                        WHEN acquisition_score >= 55 THEN 'Good (55-70)'
                        WHEN acquisition_score >= 40 THEN 'Average (40-55)'
                        ELSE 'Below Avg (<40)'
                    END as bucket, COUNT(*) as cnt
                FROM pharmacies WHERE acquisition_score IS NOT NULL
                GROUP BY bucket
            """).fetchall()
            conn.close()
            if score_dist:
                sd_df = pd.DataFrame([dict(r) for r in score_dist])
                fig2 = px.pie(sd_df, values="cnt", names="bucket",
                              color="bucket", color_discrete_map={
                                  "Strong Buy (70+)": "#059669", "Good (55-70)": "#10b981",
                                  "Average (40-55)": "#f59e0b", "Below Avg (<40)": "#ef4444",
                              }, hole=0.4)
                fig2.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=350,
                                   paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig2, use_container_width=True)

        # Top 100 targets table
        st.subheader("Top 100 Acquisition Targets")
        st.caption("Ranked by acquisition score — click into **Top Targets** for full details and deal tracking.")
        conn = get_db()
        top = conn.execute("""
            SELECT npi, organization_name, city, state, phone,
                   authorized_official_name,
                   medicare_claims_count,
                   medicare_beneficiary_count,
                   medicare_total_cost,
                   zip_pharmacy_count,
                   zip_pct_65_plus,
                   ROUND(years_in_operation, 0) as years_open,
                   hpsa_designated,
                   ROUND(acquisition_score, 1) as score,
                   deal_status
            FROM pharmacies WHERE acquisition_score IS NOT NULL
            ORDER BY acquisition_score DESC LIMIT 100
        """).fetchall()
        conn.close()
        if top:
            top_df = pd.DataFrame([dict(r) for r in top])
            top_df.columns = ["NPI", "Name", "City", "State", "Phone", "Owner/Official",
                              "Medicare Claims", "Medicare Beneficiaries", "Medicare Cost",
                              "Pharmacies in ZIP", "% 65+", "Years Open",
                              "HPSA", "Score", "Deal Status"]
            top_df["Medicare Cost"] = top_df["Medicare Cost"].apply(
                lambda x: fmt_currency(x) if pd.notna(x) and x else "—")
            top_df["Medicare Claims"] = top_df["Medicare Claims"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
            top_df["Medicare Beneficiaries"] = top_df["Medicare Beneficiaries"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
            top_df["HPSA"] = top_df["HPSA"].map({1: "Yes", 0: "No", None: "—"})
            top_df["% 65+"] = top_df["% 65+"].apply(
                lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
            top_df["Years Open"] = top_df["Years Open"].apply(
                lambda x: f"{int(x)}" if pd.notna(x) and x else "—")
            st.dataframe(top_df, use_container_width=True, hide_index=True, height=600)


# ═══════════════════════════════════════════════════════════════════════════════
# TOP TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Top Targets":
    st.title("Acquisition Targets")
    st.caption("Independent pharmacies ranked by real data — Medicare claims, market demographics, competition density, NPI tenure.")

    # Filter panel
    st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        all_states = get_all_states()
        state_options = ["All States"] + [s for s, c in all_states]
        target_state = st.selectbox("State", state_options, key="target_state")
        target_state_filter = target_state if target_state != "All States" else ""
    with col2:
        min_score = st.slider("Min Score", 0, 100, 40)
    with col3:
        target_search = st.text_input("Search", placeholder="Name, city, NPI...", key="target_search")
    with col4:
        sort_options = {
            "Acquisition Score": "acquisition_score",
            "Medicare Claims": "medicare_claims_count",
            "Medicare Cost": "medicare_total_cost",
            "Low Competition": "competition_score",
            "Aging Population": "zip_pct_65_plus",
            "Years in Operation": "years_in_operation",
        }
        sort_label = st.selectbox("Sort by", list(sort_options.keys()))
        sort_by = sort_options[sort_label]

    # Toggle filters
    tcol1, tcol2, tcol3 = st.columns(3)
    with tcol1:
        hpsa_only = st.toggle("Shortage Areas Only", value=False, key="hpsa_filter")
    with tcol2:
        long_tenured_only = st.toggle("20+ Years Only", value=False, key="tenure_filter")
    with tcol3:
        has_medicare = st.toggle("Has Medicare Data", value=False, key="medicare_filter")
    st.markdown('</div>', unsafe_allow_html=True)

    if "target_page" not in st.session_state:
        st.session_state.target_page = 1

    # Build custom query with toggle filters
    conn = get_db()
    conditions = ["is_independent = 1"]
    params = []
    if target_state_filter:
        conditions.append("state = ?")
        params.append(target_state_filter)
    if target_search:
        conditions.append("(organization_name LIKE ? OR dba_name LIKE ? OR city LIKE ? OR npi LIKE ?)")
        like = f"%{target_search}%"
        params.extend([like, like, like, like])
    if min_score > 0:
        conditions.append("acquisition_score >= ?")
        params.append(min_score)
    if hpsa_only:
        conditions.append("hpsa_designated = 1")
    if long_tenured_only:
        conditions.append("years_in_operation >= 20")
    if has_medicare:
        conditions.append("medicare_claims_count IS NOT NULL AND medicare_claims_count > 0")

    where = "WHERE " + " AND ".join(conditions)
    order_map = {
        "acquisition_score": "acquisition_score DESC",
        "medicare_claims_count": "medicare_claims_count DESC",
        "medicare_total_cost": "medicare_total_cost DESC",
        "competition_score": "competition_score DESC",
        "zip_pct_65_plus": "zip_pct_65_plus DESC",
        "years_in_operation": "years_in_operation DESC",
    }
    order = order_map.get(sort_by, "acquisition_score DESC")
    per_page = 50
    offset = (st.session_state.target_page - 1) * per_page

    total = conn.execute(f"SELECT COUNT(*) FROM pharmacies {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM pharmacies {where} ORDER BY {order} NULLS LAST LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    total_pages = max(1, (total + per_page - 1) // per_page)
    conn.close()

    col_info, col_export = st.columns([3, 1])
    with col_info:
        st.caption(f"**{total:,}** targets — Page {st.session_state.target_page} of {total_pages}")
    with col_export:
        if not df.empty:
            export_cols = ["npi", "organization_name", "city", "state", "zip", "phone",
                           "authorized_official_name", "authorized_official_phone",
                           "medicare_claims_count", "medicare_beneficiary_count", "medicare_total_cost",
                           "acquisition_score", "years_in_operation", "hpsa_designated",
                           "zip_pct_65_plus", "zip_median_income", "deal_status", "contact_notes"]
            export_cols = [c for c in export_cols if c in df.columns]
            st.download_button("Export Outreach List", df[export_cols].to_csv(index=False),
                               file_name="acquisition_targets.csv", mime="text/csv")

    if not df.empty:
        display_cols = ["npi", "organization_name", "city", "state", "phone",
                        "medicare_claims_count", "medicare_beneficiary_count",
                        "medicare_total_cost", "acquisition_score",
                        "years_in_operation", "zip_pct_65_plus",
                        "zip_pharmacy_count", "hpsa_designated", "deal_status"]
        display_cols = [c for c in display_cols if c in df.columns]
        display_df = df[display_cols].copy()

        if "acquisition_score" in display_df.columns:
            display_df["acquisition_score"] = display_df["acquisition_score"].round(1)
        if "medicare_total_cost" in display_df.columns:
            display_df["medicare_total_cost"] = display_df["medicare_total_cost"].apply(
                lambda x: fmt_currency(x) if pd.notna(x) and x else "—")
        if "medicare_claims_count" in display_df.columns:
            display_df["medicare_claims_count"] = display_df["medicare_claims_count"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
        if "medicare_beneficiary_count" in display_df.columns:
            display_df["medicare_beneficiary_count"] = display_df["medicare_beneficiary_count"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
        if "zip_pct_65_plus" in display_df.columns:
            display_df["zip_pct_65_plus"] = display_df["zip_pct_65_plus"].apply(
                lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
        if "years_in_operation" in display_df.columns:
            display_df["years_in_operation"] = display_df["years_in_operation"].apply(
                lambda x: f"{int(x)}" if pd.notna(x) and x else "—")
        if "hpsa_designated" in display_df.columns:
            display_df["hpsa_designated"] = display_df["hpsa_designated"].map({1: "Yes", 0: "No", None: "—"})

        rename = {
            "npi": "NPI", "organization_name": "Name", "city": "City", "state": "ST",
            "phone": "Phone", "medicare_claims_count": "Medicare Claims",
            "medicare_beneficiary_count": "Beneficiaries",
            "medicare_total_cost": "Medicare Cost",
            "acquisition_score": "Score",
            "years_in_operation": "Years Open",
            "zip_pct_65_plus": "% 65+",
            "zip_pharmacy_count": "Pharmacies in ZIP",
            "hpsa_designated": "HPSA",
            "deal_status": "Status",
        }
        display_df = display_df.rename(columns=rename)

        event = st.dataframe(display_df, use_container_width=True, hide_index=True,
                             on_select="rerun", selection_mode="single-row")

        # Detail + contact edit on click
        if event and event.selection and event.selection.rows:
            selected_idx = event.selection.rows[0]
            pharmacy_id = int(df.iloc[selected_idx]["id"])
            detail = get_pharmacy_detail(pharmacy_id)
            if detail:
                st.divider()
                score = detail.get("acquisition_score")
                st.subheader(detail["organization_name"])

                # Key metrics row
                fc1, fc2, fc3, fc4, fc5, fc6 = st.columns(6)
                fc1.metric("NPI", detail.get("npi", "—"))
                fc2.metric("Medicare Claims", fmt(detail.get("medicare_claims_count")))
                fc3.metric("Beneficiaries", fmt(detail.get("medicare_beneficiary_count")))
                fc4.metric("Medicare Cost", fmt_currency(detail.get("medicare_total_cost")))
                fc5.metric("Acq. Score", f"{score:.1f}/100" if score else "—")
                fc6.metric("Pharmacies in ZIP", detail.get("zip_pharmacy_count", "—"))

                # Contact info
                st.markdown("---")
                st.markdown("#### Contact & Outreach")
                with st.form(f"contact_{pharmacy_id}"):
                    row1 = st.columns([1, 1, 1])
                    with row1[0]:
                        st.markdown(f"**Phone:** {detail.get('phone') or '—'}")
                        if detail.get("fax"):
                            st.markdown(f"**Fax:** {detail['fax']}")
                    with row1[1]:
                        if detail.get("authorized_official_name"):
                            st.markdown(f"**Owner:** {detail['authorized_official_name']}")
                        if detail.get("authorized_official_phone"):
                            st.markdown(f"**Direct Line:** {detail['authorized_official_phone']}")
                    with row1[2]:
                        current_status = detail.get("deal_status") or "Not Contacted"
                        status_idx = DEAL_STATUSES.index(current_status) if current_status in DEAL_STATUSES else 0
                        new_status = st.selectbox("Deal Status", DEAL_STATUSES, index=status_idx)

                    new_notes = st.text_area("Notes", value=detail.get("contact_notes") or "",
                                             height=80, placeholder="Add notes about this target...")
                    if st.form_submit_button("Save", use_container_width=True, type="primary"):
                        update_pharmacy_contact(pharmacy_id, notes=new_notes, deal_status=new_status)
                        st.success("Saved!")
                        st.rerun()

                # Detail panels
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Location & NPI**")
                    st.text(detail.get("address_line1") or "")
                    if detail.get("address_line2"):
                        st.text(detail["address_line2"])
                    st.text(f"{detail.get('city', '')}, {detail.get('state', '')} {detail.get('zip', '')}")
                    st.text(f"NPI: {detail['npi']}")
                    st.text(f"Taxonomy: {detail.get('taxonomy_description', 'N/A')}")
                    yrs = detail.get('years_in_operation')
                    st.text(f"Years Operating: {int(yrs) if yrs else 'N/A'}")
                    st.text(f"Enumeration Date: {detail.get('enumeration_date', 'N/A')}")

                with c2:
                    st.markdown("**Medicare Part D (CMS Data)**")
                    st.text(f"Total Claims: {fmt(detail.get('medicare_claims_count'))}")
                    st.text(f"Beneficiaries: {fmt(detail.get('medicare_beneficiary_count'))}")
                    st.text(f"Total Drug Cost: {fmt_currency(detail.get('medicare_total_cost'))}")
                    avg_cost = detail.get('medicare_avg_cost_per_claim')
                    st.text(f"Avg Cost/Claim: {fmt_currency(avg_cost) if avg_cost else '—'}")
                    st.text(f"Brand Claims: {fmt(detail.get('medicare_brand_claims'))}")
                    st.text(f"Generic Claims: {fmt(detail.get('medicare_generic_claims'))}")

                with c3:
                    st.markdown("**Market Demographics (Census)**")
                    st.text(f"ZIP Pop: {fmt(detail.get('zip_population'))}")
                    st.text(f"Median Age: {detail.get('zip_median_age', '—')}")
                    st.text(f"% 65+: {detail.get('zip_pct_65_plus', '—')}%")
                    st.text(f"Median Income: {fmt(detail.get('zip_median_income'), prefix='$')}")
                    st.text(f"% Uninsured: {detail.get('zip_pct_uninsured', '—')}%")
                    st.text(f"% Disabled: {detail.get('zip_pct_disabled', '—')}%")
                    st.text(f"% Poverty: {detail.get('zip_pct_poverty', '—')}%")
                    st.text(f"Pop Growth: {detail.get('zip_pop_growth_pct', '—')}%")
                    hpsa = "Yes" if detail.get("hpsa_designated") else "No"
                    st.text(f"HPSA Shortage Area: {hpsa}")

                # Ownership details
                with st.expander("Ownership & Registration Details"):
                    oc1, oc2 = st.columns(2)
                    with oc1:
                        st.text(f"Entity Type: {detail.get('entity_type', 'N/A')}")
                        st.text(f"Ownership Type: {detail.get('ownership_type', 'N/A')}")
                        st.text(f"Chain Status: {'Chain' if detail.get('is_chain') else 'Independent'}")
                        if detail.get("chain_parent"):
                            st.text(f"Chain Parent: {detail['chain_parent']}")
                    with oc2:
                        if detail.get("authorized_official_title"):
                            st.text(f"Title: {detail['authorized_official_title']}")
                        st.text(f"Last NPI Update: {detail.get('last_update_date', 'N/A')}")
                        if detail.get("npi_deactivation_date"):
                            st.error(f"DEACTIVATED: {detail['npi_deactivation_date']}")
                            if detail.get("deactivation_reason"):
                                st.text(f"Reason: {detail['deactivation_reason']}")

        # Pagination
        if total_pages > 1:
            col_prev, _, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button("Previous", disabled=st.session_state.target_page <= 1, key="tp"):
                    st.session_state.target_page -= 1
                    st.rerun()
            with col_next:
                if st.button("Next", disabled=st.session_state.target_page >= total_pages, key="tn"):
                    st.session_state.target_page += 1
                    st.rerun()
    else:
        st.info("No targets match filters.")

    with st.expander("Scoring Methodology"):
        st.markdown("""
        **Acquisition Score (0-100)** uses only real, verifiable data:

        | Factor | Weight | Source | Why it matters |
        |--------|--------|--------|----------------|
        | **Medicare Part D Claims** | 25% | CMS Public Use File | Real prescription volume — higher = more valuable |
        | **Competition Density** | 20% | NPPES Registry | Fewer nearby pharmacies = easier patient retention |
        | **Aging Population (65+)** | 15% | Census ACS | Seniors fill 2-3x more prescriptions |
        | **Retirement Risk** | 15% | NPI Enumeration Date | 25+ year pharmacies = owner likely near retirement |
        | **HPSA Designation** | 10% | HRSA | Shortage area = underserved patients, less competition |
        | **Income / Payer Mix** | 8% | Census ACS | Higher income area = better commercial payer mix |
        | **Pop Growth** | 7% | Census ACS | Growing market = appreciating value |

        All data comes from public federal sources. No estimates or fabricated metrics.
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# CLOSING SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Closing Signals":
    st.title("Closing Signals")
    st.caption("Pharmacies showing signs of closing or retirement — prime acquisition targets.")

    conn = get_db()
    has_dates = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE enumeration_date IS NOT NULL").fetchone()[0]

    if has_dates == 0:
        st.warning("Date fields not yet populated. Run `python extract_npi_dates.py` from the M&A dash folder first.")
        conn.close()
    else:
        long_tenured = conn.execute(
            "SELECT COUNT(*) FROM pharmacies WHERE years_in_operation >= 20 AND is_independent = 1"
        ).fetchone()[0]
        stale_records = conn.execute("""
            SELECT COUNT(*) FROM pharmacies
            WHERE is_independent = 1
              AND last_update_date IS NOT NULL
              AND last_update_date < date('now', '-3 years')
        """).fetchone()[0]
        recent_deactivated = conn.execute("""
            SELECT COUNT(*) FROM pharmacies
            WHERE npi_deactivation_date IS NOT NULL
              AND npi_deactivation_date >= date('now', '-12 months')
        """).fetchone()[0]

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Long-Tenured (20+ yrs)", f"{long_tenured:,}")
        mc2.metric("Stale Records (3+ yrs)", f"{stale_records:,}")
        mc3.metric("Deactivated (last 12 mo)", f"{recent_deactivated:,}")

        st.divider()

        # Filters with toggles
        st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            all_states = get_all_states()
            cs_state_options = ["All States"] + [s for s, c in all_states]
            cs_state = st.selectbox("State", cs_state_options, key="cs_state")
            cs_state_filter = cs_state if cs_state != "All States" else ""
        with fcol2:
            signal_filter = st.selectbox("Signal Type", [
                "All Signals", "Long-Tenured (20+ yrs)", "Stale Record (3+ yrs)", "Deactivated Nearby"
            ], key="cs_signal")
        with fcol3:
            cs_search = st.text_input("Search", placeholder="Name, city...", key="cs_search")
        st.markdown('</div>', unsafe_allow_html=True)

        conditions = ["is_independent = 1"]
        params = []

        if cs_state_filter:
            conditions.append("state = ?")
            params.append(cs_state_filter)
        if cs_search:
            conditions.append("(organization_name LIKE ? OR city LIKE ?)")
            params.extend([f"%{cs_search}%", f"%{cs_search}%"])

        if signal_filter == "Long-Tenured (20+ yrs)":
            conditions.append("years_in_operation >= 20")
        elif signal_filter == "Stale Record (3+ yrs)":
            conditions.append("last_update_date IS NOT NULL AND last_update_date < date('now', '-3 years')")
        elif signal_filter == "Deactivated Nearby":
            deact_zips = conn.execute("""
                SELECT DISTINCT zip FROM pharmacies
                WHERE npi_deactivation_date IS NOT NULL
                  AND npi_deactivation_date >= date('now', '-12 months')
                  AND zip IS NOT NULL
            """).fetchall()
            deact_zip_list = [r[0] for r in deact_zips]
            if deact_zip_list:
                placeholders = ",".join(["?"] * len(deact_zip_list))
                conditions.append(f"zip IN ({placeholders})")
                conditions.append("npi_deactivation_date IS NULL")
                params.extend(deact_zip_list)
            else:
                conditions.append("1=0")
        else:
            conditions.append("""(
                years_in_operation >= 20
                OR (last_update_date IS NOT NULL AND last_update_date < date('now', '-3 years'))
                OR zip IN (
                    SELECT DISTINCT zip FROM pharmacies
                    WHERE npi_deactivation_date IS NOT NULL
                      AND npi_deactivation_date >= date('now', '-12 months')
                      AND zip IS NOT NULL
                )
            )""")

        where = "WHERE " + " AND ".join(conditions)

        if "cs_page" not in st.session_state:
            st.session_state.cs_page = 1
        per_page = 50
        offset = (st.session_state.cs_page - 1) * per_page

        total = conn.execute(f"SELECT COUNT(*) FROM pharmacies {where}", params).fetchone()[0]
        total_pages = max(1, (total + per_page - 1) // per_page)

        rows = conn.execute(f"""
            SELECT id, npi, organization_name, city, state, phone,
                   medicare_claims_count, medicare_beneficiary_count,
                   years_in_operation, last_update_date,
                   npi_deactivation_date, ROUND(acquisition_score, 1) as score
            FROM pharmacies {where}
            ORDER BY acquisition_score DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()

        deact_zip_set = set()
        dz_rows = conn.execute("""
            SELECT DISTINCT zip FROM pharmacies
            WHERE npi_deactivation_date IS NOT NULL
              AND npi_deactivation_date >= date('now', '-12 months')
              AND zip IS NOT NULL
        """).fetchall()
        deact_zip_set = {r[0] for r in dz_rows}

        zip_lookup = {}
        if rows:
            ids = [r[0] for r in rows]
            placeholders = ",".join(["?"] * len(ids))
            zip_rows = conn.execute(
                f"SELECT id, zip FROM pharmacies WHERE id IN ({placeholders})", ids
            ).fetchall()
            zip_lookup = {r[0]: r[1] for r in zip_rows}

        conn.close()

        col_info, col_export = st.columns([3, 1])
        with col_info:
            st.caption(f"**{total:,}** pharmacies with closing signals — Page {st.session_state.cs_page} of {total_pages}")

        if rows:
            data = []
            for r in rows:
                pid = r[0]
                npi = r[1]
                name, city, state, phone = r[2], r[3], r[4], r[5]
                claims, beneficiaries = r[6], r[7]
                years_op, last_upd, deact, score = r[8], r[9], r[10], r[11]

                signals = []
                if years_op and years_op >= 20:
                    signals.append("Long-Tenured")
                if last_upd and last_upd < (datetime.now().replace(year=datetime.now().year - 3)).strftime("%Y-%m-%d"):
                    signals.append("Stale Record")
                pharmacy_zip = zip_lookup.get(pid, "")
                if pharmacy_zip in deact_zip_set and not deact:
                    signals.append("Deactivated Nearby")

                data.append({
                    "NPI": npi,
                    "Name": name,
                    "City": city,
                    "ST": state,
                    "Phone": phone or "—",
                    "Medicare Claims": f"{claims:,}" if claims else "—",
                    "Beneficiaries": f"{beneficiaries:,}" if beneficiaries else "—",
                    "Years Open": f"{years_op:.0f}" if years_op else "—",
                    "Last Updated": last_upd or "—",
                    "Signal": ", ".join(signals) if signals else "—",
                    "Score": score if score else "—",
                })

            display_df = pd.DataFrame(data)
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

            with col_export:
                st.download_button("Export Signals List",
                                   display_df.to_csv(index=False),
                                   file_name="closing_signals.csv", mime="text/csv")

            if total_pages > 1:
                col_prev, _, col_next = st.columns([1, 2, 1])
                with col_prev:
                    if st.button("Previous", disabled=st.session_state.cs_page <= 1, key="csp"):
                        st.session_state.cs_page -= 1
                        st.rerun()
                with col_next:
                    if st.button("Next", disabled=st.session_state.cs_page >= total_pages, key="csn"):
                        st.session_state.cs_page += 1
                        st.rerun()
        else:
            st.info("No pharmacies match the selected signal filter.")

        with st.expander("What are Closing Signals?"):
            st.markdown("""
            | Signal | What it means | Data Source |
            |--------|--------------|-------------|
            | **Long-Tenured** | Open 20+ years | NPI Enumeration Date |
            | **Stale Record** | NPI not updated in 3+ years | NPPES Last Update |
            | **Deactivated Nearby** | Another pharmacy in same ZIP recently closed | NPI Deactivation Records |

            These signals are derived from real NPI registration data — not estimates.
            """)


# ═══════════════════════════════════════════════════════════════════════════════
# QUERY TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Query Tools":
    st.title("Query Tools")
    st.caption("One-click analyses for acquisition strategy. Pick a query, get results + export.")

    query_choice = st.selectbox("Select a Query", [
        "Retirement Hotspots",
        "Underserved Markets",
        "HPSA Shortage Targets",
        "Cold Call Priority List",
        "Multi-Target Clusters",
        "Medicare Volume Leaders",
    ], key="qt_query")

    conn = get_db()

    # ── Retirement Hotspots
    if query_choice == "Retirement Hotspots":
        st.subheader("Retirement Hotspots")
        st.caption("Cities with the most long-tenured (20+ yr) independent pharmacies — cluster buying opportunities.")

        qt_state_options = ["All States"] + [s for s, c in get_all_states()]
        qt_state = st.selectbox("Filter by State", qt_state_options, key="qt_rh_state")

        state_cond = "AND state = ?" if qt_state != "All States" else ""
        state_params = [qt_state] if qt_state != "All States" else []

        hotspots = conn.execute(f"""
            SELECT city, state, COUNT(*) as long_tenured_count,
                   ROUND(AVG(years_in_operation), 0) as avg_years,
                   SUM(medicare_claims_count) as total_claims,
                   ROUND(AVG(acquisition_score), 1) as avg_score,
                   AVG(zip_pct_65_plus) as avg_65_plus
            FROM pharmacies
            WHERE is_independent = 1
              AND years_in_operation >= 20
              AND city IS NOT NULL
              {state_cond}
            GROUP BY city, state
            HAVING COUNT(*) >= 2
            ORDER BY long_tenured_count DESC, total_claims DESC
            LIMIT 100
        """, state_params).fetchall()

        if hotspots:
            data = []
            for r in hotspots:
                data.append({
                    "City": r[0], "ST": r[1],
                    "20+ Yr Pharmacies": r[2],
                    "Avg Years Open": int(r[3]) if r[3] else "—",
                    "Total Medicare Claims": f"{r[4]:,}" if r[4] else "—",
                    "Avg Score": r[5] if r[5] else "—",
                    "Avg % 65+": f"{r[6]:.1f}%" if r[6] else "—",
                })
            df = pd.DataFrame(data)

            mc1, mc2 = st.columns(2)
            mc1.metric("Hotspot Cities", len(data))
            total_targets = sum(r[2] for r in hotspots)
            mc2.metric("Total Targets", f"{total_targets:,}")

            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export Hotspots", df.to_csv(index=False),
                               file_name="retirement_hotspots.csv", mime="text/csv")
        else:
            st.info("No retirement hotspots found with current filters.")

    # ── Underserved Markets
    elif query_choice == "Underserved Markets":
        st.subheader("Underserved Markets")
        st.caption("ZIPs where pharmacies recently closed AND the population is aging/growing — displaced patients need a pharmacy.")

        underserved = conn.execute("""
            SELECT
                d.zip,
                d.deact_count,
                p.city, p.state,
                p.zip_population,
                p.zip_pct_65_plus,
                p.zip_pop_growth_pct,
                p.zip_pharmacy_count,
                p.zip_pct_uninsured,
                active.active_independents
            FROM (
                SELECT zip, COUNT(*) as deact_count
                FROM pharmacies
                WHERE npi_deactivation_date IS NOT NULL
                  AND npi_deactivation_date >= date('now', '-24 months')
                  AND zip IS NOT NULL
                GROUP BY zip
            ) d
            JOIN pharmacies p ON p.zip = d.zip AND p.id = (
                SELECT id FROM pharmacies WHERE zip = d.zip AND zip_population IS NOT NULL LIMIT 1
            )
            LEFT JOIN (
                SELECT zip, COUNT(*) as active_independents
                FROM pharmacies
                WHERE is_independent = 1
                  AND npi_deactivation_date IS NULL
                GROUP BY zip
            ) active ON active.zip = d.zip
            WHERE p.zip_pct_65_plus >= 15 OR p.zip_pop_growth_pct > 0
            ORDER BY d.deact_count DESC
            LIMIT 100
        """).fetchall()

        if underserved:
            data = []
            for r in underserved:
                data.append({
                    "ZIP": r[0], "City": r[2], "ST": r[3],
                    "Recently Closed": r[1],
                    "Active Independents": r[9] or 0,
                    "ZIP Population": f"{r[4]:,}" if r[4] else "—",
                    "% 65+": f"{r[5]:.1f}%" if r[5] else "—",
                    "Pop Growth %": r[6] if r[6] else "—",
                    "% Uninsured": f"{r[8]:.1f}%" if r[8] else "—",
                })
            df = pd.DataFrame(data)

            mc1, mc2 = st.columns(2)
            mc1.metric("Underserved ZIPs", len(data))
            mc2.metric("Total Closures (24 mo)", sum(r[1] for r in underserved))

            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export Underserved Markets", df.to_csv(index=False),
                               file_name="underserved_markets.csv", mime="text/csv")
        else:
            st.info("No underserved markets found.")

    # ── HPSA Shortage Targets
    elif query_choice == "HPSA Shortage Targets":
        st.subheader("HPSA Shortage Area Targets")
        st.caption("Independent pharmacies in federally-designated Health Professional Shortage Areas — high need, less competition.")

        qt_state_options = ["All States"] + [s for s, c in get_all_states()]
        qt_state = st.selectbox("Filter by State", qt_state_options, key="qt_hpsa_state")

        state_cond = "AND state = ?" if qt_state != "All States" else ""
        state_params = [qt_state] if qt_state != "All States" else []

        hpsa = conn.execute(f"""
            SELECT npi, organization_name, city, state, phone,
                   authorized_official_name,
                   medicare_claims_count,
                   medicare_beneficiary_count,
                   ROUND(acquisition_score, 1) as score,
                   ROUND(years_in_operation, 0) as years_open,
                   zip_pct_65_plus,
                   zip_pharmacy_count,
                   hpsa_score
            FROM pharmacies
            WHERE is_independent = 1
              AND hpsa_designated = 1
              {state_cond}
            ORDER BY acquisition_score DESC
            LIMIT 100
        """, state_params).fetchall()

        if hpsa:
            data = []
            for r in hpsa:
                data.append({
                    "NPI": r[0], "Name": r[1], "City": r[2], "ST": r[3],
                    "Phone": r[4] or "—",
                    "Owner": r[5] or "—",
                    "Medicare Claims": f"{r[6]:,}" if r[6] else "—",
                    "Beneficiaries": f"{r[7]:,}" if r[7] else "—",
                    "Score": r[8],
                    "Years Open": int(r[9]) if r[9] else "—",
                    "% 65+": f"{r[10]:.1f}%" if r[10] else "—",
                    "Pharmacies in ZIP": r[11] or "—",
                    "HPSA Score": r[12] or "—",
                })
            df = pd.DataFrame(data)

            mc1, mc2 = st.columns(2)
            mc1.metric("HPSA Targets", len(data))
            avg_score = sum(r[8] or 0 for r in hpsa) / len(hpsa)
            mc2.metric("Avg Score", f"{avg_score:.1f}")

            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export HPSA Targets", df.to_csv(index=False),
                               file_name="hpsa_targets.csv", mime="text/csv")
        else:
            st.info("No HPSA-designated pharmacies found. Run the data enrichment script to populate HPSA data.")

    # ── Cold Call Priority List
    elif query_choice == "Cold Call Priority List":
        st.subheader("Cold Call Priority List")
        st.caption("Top 100 most actionable targets: high score + closing signal + phone number. Ready to dial.")

        qt_state_options = ["All States"] + [s for s, c in get_all_states()]
        qt_state = st.selectbox("Filter by State", qt_state_options, key="qt_cc_state")

        state_cond = "AND state = ?" if qt_state != "All States" else ""
        state_params = [qt_state] if qt_state != "All States" else []

        calls = conn.execute(f"""
            SELECT npi, organization_name, city, state, phone,
                   authorized_official_name, authorized_official_phone,
                   medicare_claims_count,
                   ROUND(acquisition_score, 1) as score,
                   ROUND(years_in_operation, 0) as years_open,
                   last_update_date,
                   deal_status
            FROM pharmacies
            WHERE is_independent = 1
              AND phone IS NOT NULL AND phone != ''
              AND acquisition_score >= 40
              AND (years_in_operation >= 20
                   OR (last_update_date IS NOT NULL AND last_update_date < date('now', '-3 years')))
              AND (deal_status IS NULL OR deal_status = 'Not Contacted')
              {state_cond}
            ORDER BY acquisition_score DESC
            LIMIT 100
        """, state_params).fetchall()

        if calls:
            data = []
            for r in calls:
                signals = []
                if r[9] and r[9] >= 20:
                    signals.append("Long-Tenured")
                if r[10] and r[10] < (datetime.now().replace(year=datetime.now().year - 3)).strftime("%Y-%m-%d"):
                    signals.append("Stale Record")
                data.append({
                    "NPI": r[0], "Name": r[1], "City": r[2], "ST": r[3],
                    "Phone": r[4],
                    "Owner": r[5] or "—",
                    "Direct Line": r[6] or "—",
                    "Medicare Claims": f"{r[7]:,}" if r[7] else "—",
                    "Score": r[8],
                    "Signal": ", ".join(signals) or "—",
                })
            df = pd.DataFrame(data)

            st.metric("Ready to Call", len(data))
            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export Call List", df.to_csv(index=False),
                               file_name="cold_call_priority.csv", mime="text/csv")
        else:
            st.info("No un-contacted targets with closing signals found.")

    # ── Multi-Target Clusters
    elif query_choice == "Multi-Target Clusters":
        st.subheader("Multi-Target Clusters")
        st.caption("Cities with 3+ independent targets — acquire multiple files in one geography.")

        qt_state_options = ["All States"] + [s for s, c in get_all_states()]
        qt_state = st.selectbox("Filter by State", qt_state_options, key="qt_mf_state")
        min_targets = st.slider("Minimum targets in city", 3, 20, 3, key="qt_mf_min")

        state_cond = "AND state = ?" if qt_state != "All States" else ""
        state_params = [qt_state] if qt_state != "All States" else []

        clusters = conn.execute(f"""
            SELECT city, state,
                   COUNT(*) as target_count,
                   SUM(medicare_claims_count) as total_claims,
                   ROUND(AVG(acquisition_score), 1) as avg_score,
                   ROUND(AVG(years_in_operation), 0) as avg_years,
                   SUM(CASE WHEN years_in_operation >= 20 THEN 1 ELSE 0 END) as long_tenured,
                   MAX(zip_pct_65_plus) as max_65_plus,
                   SUM(CASE WHEN hpsa_designated = 1 THEN 1 ELSE 0 END) as hpsa_count
            FROM pharmacies
            WHERE is_independent = 1
              AND city IS NOT NULL
              {state_cond}
            GROUP BY city, state
            HAVING COUNT(*) >= ?
            ORDER BY target_count DESC, total_claims DESC
            LIMIT 100
        """, state_params + [min_targets]).fetchall()

        if clusters:
            data = []
            for r in clusters:
                data.append({
                    "City": r[0], "ST": r[1],
                    "Independents": r[2],
                    "Total Medicare Claims": f"{r[3]:,}" if r[3] else "—",
                    "Avg Score": r[4],
                    "Avg Years Open": int(r[5]) if r[5] else "—",
                    "Long-Tenured": r[6],
                    "Max % 65+": f"{r[7]:.1f}%" if r[7] else "—",
                    "In HPSA": r[8] or 0,
                })
            df = pd.DataFrame(data)

            mc1, mc2 = st.columns(2)
            mc1.metric("Cluster Cities", len(data))
            total_targets = sum(r[2] for r in clusters)
            mc2.metric("Total Targets", f"{total_targets:,}")

            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export Clusters", df.to_csv(index=False),
                               file_name="multi_target_clusters.csv", mime="text/csv")
        else:
            st.info("No clusters found. Try lowering minimum count or removing state filter.")

    # ── Medicare Volume Leaders
    elif query_choice == "Medicare Volume Leaders":
        st.subheader("Medicare Volume Leaders")
        st.caption("Independent pharmacies with the highest real Medicare Part D claim volumes — proven prescription traffic.")

        qt_state_options = ["All States"] + [s for s, c in get_all_states()]
        qt_state = st.selectbox("Filter by State", qt_state_options, key="qt_mv_state")

        state_cond = "AND state = ?" if qt_state != "All States" else ""
        state_params = [qt_state] if qt_state != "All States" else []

        leaders = conn.execute(f"""
            SELECT npi, organization_name, city, state, phone,
                   authorized_official_name,
                   medicare_claims_count,
                   medicare_beneficiary_count,
                   medicare_total_cost,
                   medicare_avg_cost_per_claim,
                   ROUND(acquisition_score, 1) as score,
                   ROUND(years_in_operation, 0) as years_open,
                   deal_status
            FROM pharmacies
            WHERE is_independent = 1
              AND medicare_claims_count IS NOT NULL AND medicare_claims_count > 0
              {state_cond}
            ORDER BY medicare_claims_count DESC
            LIMIT 100
        """, state_params).fetchall()

        if leaders:
            data = []
            for r in leaders:
                data.append({
                    "NPI": r[0], "Name": r[1], "City": r[2], "ST": r[3],
                    "Phone": r[4] or "—",
                    "Owner": r[5] or "—",
                    "Medicare Claims": f"{r[6]:,}",
                    "Beneficiaries": f"{r[7]:,}" if r[7] else "—",
                    "Total Drug Cost": fmt_currency(r[8]),
                    "Avg $/Claim": fmt_currency(r[9]),
                    "Score": r[10],
                    "Years Open": int(r[11]) if r[11] else "—",
                    "Status": r[12] or "Not Contacted",
                })
            df = pd.DataFrame(data)

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Leaders Found", len(data))
            total_claims = sum(r[6] for r in leaders)
            mc2.metric("Combined Claims", f"{total_claims:,}")
            total_cost = sum(r[8] or 0 for r in leaders)
            mc3.metric("Combined Drug Cost", fmt_currency(total_cost))

            st.dataframe(df, use_container_width=True, hide_index=True, height=500)
            st.download_button("Export Leaders", df.to_csv(index=False),
                               file_name="medicare_volume_leaders.csv", mime="text/csv")
        else:
            st.info("No Medicare data found. Run the CMS data enrichment to populate this.")

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TUCK-IN FINDER
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Tuck-in Finder":
    st.title("Tuck-in Finder")
    st.caption("Enter your store's ZIP code to find nearby independent pharmacies whose files you can absorb.")

    st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
    col1, col2 = st.columns([1, 2])
    with col1:
        my_zip = st.text_input("Your Store ZIP Code", placeholder="e.g. 10001", max_chars=5)
        radius_option = st.selectbox("Search Radius", ["Same ZIP", "Nearby ZIPs (+/-2)", "Nearby ZIPs (+/-5)"])
        min_score_tuckin = st.slider("Min Acq. Score", 0, 100, 0, key="tuckin_score")
    st.markdown('</div>', unsafe_allow_html=True)

    if my_zip and len(my_zip) == 5:
        conn = get_db()

        if radius_option == "Same ZIP":
            zip_condition = "zip = ?"
            zip_params = [my_zip]
        elif radius_option == "Nearby ZIPs (+/-2)":
            try:
                z = int(my_zip)
                zips = [str(z + i).zfill(5) for i in range(-2, 3)]
                zip_condition = f"zip IN ({','.join(['?'] * len(zips))})"
                zip_params = zips
            except ValueError:
                zip_condition = "zip = ?"
                zip_params = [my_zip]
        else:
            try:
                z = int(my_zip)
                zips = [str(z + i).zfill(5) for i in range(-5, 6)]
                zip_condition = f"zip IN ({','.join(['?'] * len(zips))})"
                zip_params = zips
            except ValueError:
                zip_condition = "zip = ?"
                zip_params = [my_zip]

        score_cond = "AND acquisition_score >= ?" if min_score_tuckin > 0 else ""
        score_params = [min_score_tuckin] if min_score_tuckin > 0 else []

        nearby = conn.execute(f"""
            SELECT * FROM pharmacies
            WHERE is_independent = 1 AND {zip_condition}
              {score_cond}
            ORDER BY acquisition_score DESC NULLS LAST
        """, zip_params + score_params).fetchall()
        conn.close()

        with col2:
            if nearby:
                st.success(f"Found **{len(nearby)}** independent pharmacies near ZIP {my_zip}")
                nearby_df = pd.DataFrame([dict(r) for r in nearby])

                total_claims = nearby_df["medicare_claims_count"].sum()
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric("Targets Found", len(nearby))
                mc2.metric("Total Medicare Claims", f"{total_claims:,.0f}" if total_claims else "—")
                avg_score_n = nearby_df["acquisition_score"].mean()
                mc3.metric("Avg Score", f"{avg_score_n:.1f}" if pd.notna(avg_score_n) else "—")

                display_cols = ["npi", "organization_name", "city", "state", "zip", "phone",
                                "medicare_claims_count", "medicare_beneficiary_count",
                                "acquisition_score",
                                "authorized_official_name", "deal_status"]
                display_cols = [c for c in display_cols if c in nearby_df.columns]
                disp = nearby_df[display_cols].copy()

                if "acquisition_score" in disp.columns:
                    disp["acquisition_score"] = disp["acquisition_score"].round(1)
                if "medicare_claims_count" in disp.columns:
                    disp["medicare_claims_count"] = disp["medicare_claims_count"].apply(
                        lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
                if "medicare_beneficiary_count" in disp.columns:
                    disp["medicare_beneficiary_count"] = disp["medicare_beneficiary_count"].apply(
                        lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")

                disp.columns = ["NPI", "Name", "City", "ST", "ZIP", "Phone",
                                "Medicare Claims", "Beneficiaries", "Score",
                                "Owner", "Status"][:len(disp.columns)]

                st.dataframe(disp, use_container_width=True, hide_index=True)

                export_cols_nearby = ["npi", "organization_name", "city", "state", "zip", "phone",
                                      "authorized_official_name", "medicare_claims_count",
                                      "acquisition_score", "deal_status"]
                export_cols_nearby = [c for c in export_cols_nearby if c in nearby_df.columns]
                st.download_button(
                    "Export Tuck-in List",
                    nearby_df[export_cols_nearby].to_csv(index=False),
                    file_name=f"tuckin_targets_ZIP_{my_zip}.csv", mime="text/csv",
                )
            else:
                st.warning(f"No independent pharmacies found near ZIP {my_zip} with the selected filters.")


# ═══════════════════════════════════════════════════════════════════════════════
# DIRECTORY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Directory":
    st.title("Full Directory")

    st.markdown('<div class="filter-panel">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        search = st.text_input("Search", placeholder="Name, city, NPI...")
    with col2:
        all_states = get_all_states()
        state_options = ["All States"] + [f"{s} ({c:,})" for s, c in all_states]
        state_sel = st.selectbox("State", state_options)
        state_filter = state_sel.split(" (")[0] if state_sel != "All States" else ""
    with col3:
        city = st.text_input("City")
    with col4:
        zip_code = st.text_input("ZIP")

    tcol1, tcol2, tcol3 = st.columns(3)
    with tcol1:
        independent_only = st.toggle("Independent Only", value=True, key="dir_indep")
    with tcol2:
        has_medicare_dir = st.toggle("Has Medicare Data", value=False, key="dir_medicare")
    with tcol3:
        sort_opts = {
            "Acq. Score": "acquisition_score",
            "Name": "organization_name",
            "Medicare Claims": "medicare_claims_count",
            "Years Operating": "years_in_operation",
        }
        sort_label = st.selectbox("Sort", list(sort_opts.keys()))
    st.markdown('</div>', unsafe_allow_html=True)

    if "dir_page" not in st.session_state:
        st.session_state.dir_page = 1

    # Custom query to support toggle filters
    conn = get_db()
    conditions = []
    params = []
    if search:
        conditions.append("(organization_name LIKE ? OR dba_name LIKE ? OR city LIKE ? OR npi LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])
    if state_filter:
        conditions.append("state = ?")
        params.append(state_filter)
    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")
    if zip_code:
        conditions.append("zip LIKE ?")
        params.append(f"{zip_code}%")
    if independent_only:
        conditions.append("is_independent = 1")
    if has_medicare_dir:
        conditions.append("medicare_claims_count IS NOT NULL AND medicare_claims_count > 0")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    order_map = {
        "acquisition_score": "acquisition_score DESC",
        "organization_name": "organization_name ASC",
        "medicare_claims_count": "medicare_claims_count DESC",
        "years_in_operation": "years_in_operation DESC",
    }
    order = order_map.get(sort_opts[sort_label], "acquisition_score DESC")
    per_page = 50
    offset_val = (st.session_state.dir_page - 1) * per_page

    total = conn.execute(f"SELECT COUNT(*) FROM pharmacies {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM pharmacies {where} ORDER BY {order} NULLS LAST LIMIT ? OFFSET ?",
        params + [per_page, offset_val],
    ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    total_pages = max(1, (total + per_page - 1) // per_page)
    conn.close()

    col_info, col_export = st.columns([3, 1])
    with col_info:
        st.caption(f"**{total:,}** results — Page {st.session_state.dir_page} of {total_pages}")
    with col_export:
        if not df.empty:
            st.download_button("Export CSV", df.to_csv(index=False),
                               file_name="pharmacies.csv", mime="text/csv")

    if not df.empty:
        cols = ["npi", "organization_name", "city", "state", "zip", "phone",
                "is_independent", "medicare_claims_count", "medicare_beneficiary_count",
                "acquisition_score", "years_in_operation", "deal_status"]
        cols = [c for c in cols if c in df.columns]
        disp = df[cols].copy()
        if "is_independent" in disp.columns:
            disp["is_independent"] = disp["is_independent"].map({1: "Independent", 0: "Chain"})
        if "acquisition_score" in disp.columns:
            disp["acquisition_score"] = disp["acquisition_score"].round(1)
        if "medicare_claims_count" in disp.columns:
            disp["medicare_claims_count"] = disp["medicare_claims_count"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
        if "medicare_beneficiary_count" in disp.columns:
            disp["medicare_beneficiary_count"] = disp["medicare_beneficiary_count"].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
        if "years_in_operation" in disp.columns:
            disp["years_in_operation"] = disp["years_in_operation"].apply(
                lambda x: f"{int(x)}" if pd.notna(x) and x else "—")
        disp.columns = ["NPI", "Name", "City", "ST", "ZIP", "Phone", "Type",
                        "Medicare Claims", "Beneficiaries", "Score",
                        "Years Open", "Status"][:len(disp.columns)]
        st.dataframe(disp, use_container_width=True, hide_index=True)

    if total_pages > 1:
        cp, _, cn = st.columns([1, 2, 1])
        with cp:
            if st.button("Previous", disabled=st.session_state.dir_page <= 1, key="dp"):
                st.session_state.dir_page -= 1
                st.rerun()
        with cn:
            if st.button("Next", disabled=st.session_state.dir_page >= total_pages, key="dn"):
                st.session_state.dir_page += 1
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# DEAL PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Deal Pipeline":
    st.title("Deal Pipeline")
    st.caption("Track your outreach and deal progress across all targets.")

    conn = get_db()
    pipeline = conn.execute("""
        SELECT deal_status, COUNT(*) as cnt,
               SUM(medicare_claims_count) as total_claims,
               SUM(medicare_total_cost) as total_cost
        FROM pharmacies
        WHERE deal_status IS NOT NULL AND deal_status != 'Not Contacted'
        GROUP BY deal_status
    """).fetchall()

    if not pipeline:
        st.info("No deals in the pipeline yet. Go to **Top Targets**, click a pharmacy, "
                "and update its Deal Status to start tracking.")
    else:
        pipeline_df = pd.DataFrame([dict(r) for r in pipeline])
        total_deals = pipeline_df["cnt"].sum()
        total_claims = pipeline_df["total_claims"].sum()
        total_cost = pipeline_df["total_cost"].sum()

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Active Deals", int(total_deals))
        mc2.metric("Pipeline Medicare Claims", f"{total_claims:,.0f}" if total_claims else "0")
        mc3.metric("Pipeline Drug Cost", fmt_currency(total_cost) if total_cost else "$0")

        st.subheader("By Status")
        for _, row in pipeline_df.iterrows():
            claims_str = f"{int(row['total_claims'] or 0):,} Medicare claims" if row['total_claims'] else "no Medicare data"
            st.markdown(f"**{row['deal_status']}** — {int(row['cnt'])} deals, {claims_str}")

        for _, row in pipeline_df.iterrows():
            status = row["deal_status"]
            deals = conn.execute("""
                SELECT id, npi, organization_name, city, state, phone,
                       medicare_claims_count, medicare_beneficiary_count,
                       contact_notes
                FROM pharmacies WHERE deal_status = ?
                ORDER BY acquisition_score DESC
            """, (status,)).fetchall()

            if deals:
                st.markdown(f"#### {status}")
                deals_df = pd.DataFrame([dict(r) for r in deals])
                if "medicare_claims_count" in deals_df.columns:
                    deals_df["medicare_claims_count"] = deals_df["medicare_claims_count"].apply(
                        lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
                if "medicare_beneficiary_count" in deals_df.columns:
                    deals_df["medicare_beneficiary_count"] = deals_df["medicare_beneficiary_count"].apply(
                        lambda x: f"{int(x):,}" if pd.notna(x) and x else "—")
                deals_df = deals_df.drop(columns=["id"], errors="ignore")
                deals_df.columns = ["NPI", "Name", "City", "ST", "Phone",
                                    "Medicare Claims", "Beneficiaries", "Notes"][:len(deals_df.columns)]
                st.dataframe(deals_df, use_container_width=True, hide_index=True)

    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET MAP
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Market Map":
    st.title("Market Map")
    import plotly.express as px

    if stats["total"] == 0:
        st.info("No data loaded.")
    else:
        map_metric = st.selectbox("Color by", [
            "Independent Pharmacies", "Avg Medicare Claims",
            "Avg % Population 65+", "Avg Median Income",
            "Avg Competition", "HPSA Shortage Count",
        ])

        conn = get_db()
        metric_map = {
            "Independent Pharmacies": ("COUNT(*)", "is_independent = 1", "Independents"),
            "Avg Medicare Claims": ("AVG(medicare_claims_count)", "is_independent = 1 AND medicare_claims_count > 0", "Avg Claims"),
            "Avg % Population 65+": ("AVG(zip_pct_65_plus)", "zip_pct_65_plus IS NOT NULL", "Avg % 65+"),
            "Avg Median Income": ("AVG(zip_median_income)", "zip_median_income IS NOT NULL AND zip_median_income > 0", "Avg Income ($)"),
            "Avg Competition": ("AVG(zip_pharmacies_per_10k)", "zip_pharmacies_per_10k IS NOT NULL", "Pharmacies/10K"),
            "HPSA Shortage Count": ("SUM(CASE WHEN hpsa_designated = 1 THEN 1 ELSE 0 END)", "is_independent = 1", "HPSA Count"),
        }
        agg, where_clause, label = metric_map[map_metric]
        state_data = conn.execute(f"""
            SELECT state, {agg} as val FROM pharmacies
            WHERE state IS NOT NULL AND {where_clause}
            GROUP BY state ORDER BY val DESC
        """).fetchall()
        conn.close()

        state_df = pd.DataFrame([dict(r) for r in state_data], columns=["state", "val"])
        if not state_df.empty:
            color_scale = ("Reds" if "Competition" in map_metric
                          else "Oranges" if "HPSA" in map_metric
                          else "Greens" if "Claims" in map_metric
                          else "Blues")
            fig = px.choropleth(state_df, locations="state", locationmode="USA-states",
                                color="val", scope="usa", color_continuous_scale=color_scale,
                                labels={"val": label, "state": "State"})
            fig.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=500,
                              plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

            state_df.columns = ["State", label]
            if "$" in label:
                state_df[label] = state_df[label].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
            elif "%" in label or "10K" in label:
                state_df[label] = state_df[label].round(1)
            else:
                state_df[label] = state_df[label].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
            st.dataframe(state_df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Data Sources":
    st.title("Data Sources & Enrichment")
    st.caption("All data comes from real, public federal sources. Run the enrichment script to pull additional data.")

    st.subheader("Data Sources Used")
    sources = [
        {
            "name": "NPPES (NPI Registry)",
            "url": "https://download.cms.gov/nppes/NPI_Files.html",
            "what": "NPI numbers, pharmacy names, addresses, phone numbers, taxonomy codes, authorized officials, enumeration dates, deactivation status",
            "update": "Monthly",
        },
        {
            "name": "CMS Medicare Part D Prescriber PUF",
            "url": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider",
            "what": "Actual prescription claim counts, beneficiary counts, total drug costs, brand vs generic splits — by NPI",
            "update": "Annual",
        },
        {
            "name": "U.S. Census ACS (5-Year Estimates)",
            "url": "https://data.census.gov",
            "what": "Population, median income, % age 65+, median age, % uninsured, % disabled, % poverty, household data — by ZIP/ZCTA",
            "update": "Annual",
        },
        {
            "name": "HRSA Health Professional Shortage Areas",
            "url": "https://data.hrsa.gov/topics/health-workforce/shortage-areas",
            "what": "HPSA designation and scores — identifies areas with insufficient healthcare providers",
            "update": "Quarterly",
        },
    ]

    for src in sources:
        with st.expander(f"**{src['name']}**"):
            st.markdown(f"**What it provides:** {src['what']}")
            st.markdown(f"**Update frequency:** {src['update']}")
            st.markdown(f"**Source:** {src['url']}")

    st.divider()
    st.subheader("Current Data Status")
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    has_medicare = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE medicare_claims_count IS NOT NULL AND medicare_claims_count > 0").fetchone()[0]
    has_census = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE zip_population IS NOT NULL").fetchone()[0]
    has_hpsa = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE hpsa_designated = 1").fetchone()[0]
    has_dates = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE enumeration_date IS NOT NULL").fetchone()[0]
    has_scores = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    conn.close()

    dc1, dc2, dc3 = st.columns(3)
    dc1.metric("Total Pharmacies", f"{total:,}")
    dc1.metric("With NPI Dates", f"{has_dates:,}")
    dc2.metric("With Medicare Data", f"{has_medicare:,}")
    dc2.metric("With Census Data", f"{has_census:,}")
    dc3.metric("With HPSA Data", f"{has_hpsa:,}")
    dc3.metric("With Acq. Scores", f"{has_scores:,}")

    st.divider()
    st.subheader("How to Enrich Data")
    st.markdown("""
    Run the enrichment script to pull additional data from public APIs:

    ```bash
    cd "Claude random/M&A dash"
    python enrich_data.py
    ```

    This will:
    1. **CMS Medicare Part D** — Match NPIs to real claim/cost data
    2. **Census ACS** — Pull demographics for each ZIP code
    3. **HRSA HPSA** — Check shortage area designations
    4. **Recalculate scores** — Update acquisition scores based on real data
    """)

    st.subheader("Scoring Formula")
    st.markdown("""
    | Factor | Weight | Source | Description |
    |--------|--------|--------|-------------|
    | Medicare Claims Volume | 25% | CMS Part D | Real Rx claim count from Medicare data |
    | Competition Density | 20% | NPPES | Pharmacies per 10K population in ZIP |
    | Aging Population | 15% | Census ACS | % population 65+ in ZIP |
    | Retirement Risk | 15% | NPPES | Years since NPI enumeration (25+ yr = highest) |
    | HPSA Designation | 10% | HRSA | Bonus for being in a shortage area |
    | Income / Payer Mix | 8% | Census ACS | Median household income in ZIP |
    | Population Growth | 7% | Census ACS | ZIP population growth rate |

    **All inputs are real, publicly-available federal data. No estimates.**
    """)
