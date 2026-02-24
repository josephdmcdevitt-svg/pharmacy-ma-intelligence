"""
Pharmacy Acquisition Intelligence Platform — Streamlit Edition

M&A intelligence dashboard for pharmacy acquisition targeting.
Enriched with CMS Medicare data and U.S. Census demographics.
"""
import streamlit as st
import pandas as pd
import sqlite3
import os
import re
import hashlib
import zipfile
import time
from datetime import datetime
from pathlib import Path

# ─── Configuration ───────────────────────────────────────────────────────────

APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
DB_PATH = APP_DIR / "pharmacy_intel.db"
DATA_DIR.mkdir(exist_ok=True)

st.set_page_config(
    page_title="Pharmacy M&A Intelligence",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
            medicare_claims_count INTEGER, medicare_beneficiary_count INTEGER,
            medicare_total_cost REAL, latitude REAL, longitude REAL,
            dedup_key TEXT, first_seen TEXT, last_refreshed TEXT,
            zip_population INTEGER, zip_median_income INTEGER,
            zip_pct_65_plus REAL, zip_pop_growth_pct REAL,
            zip_medicare_claims INTEGER, zip_medicare_cost REAL,
            zip_medicare_beneficiaries INTEGER, zip_pharmacy_count INTEGER,
            zip_pharmacies_per_10k REAL, competition_score REAL,
            market_demand_score REAL, acquisition_score REAL
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
    """)
    conn.close()


init_db()


# ─── Helper Queries ──────────────────────────────────────────────────────────

def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    independent = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    chain = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_chain = 1").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM pharmacies WHERE state IS NOT NULL").fetchone()[0]
    scored = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    avg_score = conn.execute("SELECT AVG(acquisition_score) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    conn.close()
    return {
        "total": total, "independent": independent, "chain": chain,
        "states": states, "scored": scored, "avg_score": avg_score or 0,
    }


def get_top_states(limit=15, independent_only=False):
    conn = get_db()
    where = "WHERE state IS NOT NULL" + (" AND is_independent = 1" if independent_only else "")
    rows = conn.execute(
        f"SELECT state, COUNT(*) as cnt FROM pharmacies {where} "
        f"GROUP BY state ORDER BY cnt DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return pd.DataFrame([dict(r) for r in rows], columns=["state", "cnt"])


def search_pharmacies(search="", state="", city="", zip_code="", independent_only=False,
                      min_score=0, sort_by="organization_name", page=1, per_page=50):
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
        "organization_name": "organization_name ASC",
        "acquisition_score": "acquisition_score DESC",
        "zip_medicare_claims": "zip_medicare_claims DESC",
        "zip_pct_65_plus": "zip_pct_65_plus DESC",
        "competition_score": "competition_score DESC",
        "zip_median_income": "zip_median_income DESC",
    }
    order = order_map.get(sort_by, "organization_name ASC")

    total = conn.execute(f"SELECT COUNT(*) FROM pharmacies {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM pharmacies {where} ORDER BY {order} LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()
    conn.close()
    df = pd.DataFrame([dict(r) for r in rows])
    return df, total


def get_pharmacy_detail(pharmacy_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM pharmacies WHERE id = ?", (pharmacy_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_states():
    conn = get_db()
    rows = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM pharmacies WHERE state IS NOT NULL "
        "GROUP BY state ORDER BY state"
    ).fetchall()
    conn.close()
    return [(r["state"], r["cnt"]) for r in rows]


def fmt(val, prefix="", suffix="", decimals=0):
    """Format a number nicely, return '—' if None."""
    if val is None:
        return "—"
    if decimals > 0:
        return f"{prefix}{val:,.{decimals}f}{suffix}"
    return f"{prefix}{int(val):,}{suffix}"


def score_color(score):
    """Return a color for a score value."""
    if score is None:
        return "gray"
    if score >= 70:
        return "green"
    if score >= 50:
        return "orange"
    return "red"


def score_label(score):
    if score is None:
        return "—"
    if score >= 70:
        return f"🟢 {score:.0f}"
    if score >= 50:
        return f"🟡 {score:.0f}"
    return f"🔴 {score:.0f}"


# ─── Sidebar Navigation ─────────────────────────────────────────────────────

st.sidebar.markdown("## 💊 Pharmacy Intel")
st.sidebar.caption("M&A Intelligence Platform")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Top Targets", "Directory", "Market Map", "Changes"],
    label_visibility="collapsed",
)

st.sidebar.divider()
stats = get_stats()
if stats["total"] > 0:
    st.sidebar.metric("Total Pharmacies", f"{stats['total']:,}")
    st.sidebar.metric("Independent", f"{stats['independent']:,}")
    st.sidebar.metric("Avg Acq. Score", f"{stats['avg_score']:.1f}" if stats['avg_score'] else "—")


# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

if page == "Dashboard":
    st.title("Dashboard")

    if stats["total"] == 0:
        st.info("No data loaded. Run the pipeline script first.")
    else:
        # Row 1: Key stats
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Pharmacies", f"{stats['total']:,}")
        c2.metric("Independent", f"{stats['independent']:,}")
        c3.metric("Chain", f"{stats['chain']:,}")
        c4.metric("States", stats["states"])
        c5.metric("Avg Acq. Score", f"{stats['avg_score']:.1f}")

        st.divider()

        # Row 2: Charts
        import plotly.express as px

        col_chart, col_pie = st.columns([2, 1])

        with col_chart:
            st.subheader("Independent Pharmacies by State (Top 15)")
            top_states = get_top_states(15, independent_only=True)
            if not top_states.empty:
                fig = px.bar(top_states, x="state", y="cnt",
                             labels={"state": "State", "cnt": "Independent Pharmacies"},
                             color_discrete_sequence=["#2563eb"])
                fig.update_layout(margin=dict(t=10, b=40, l=40, r=10), height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        with col_pie:
            st.subheader("Type Breakdown")
            breakdown = pd.DataFrame({
                "Type": ["Independent", "Chain"],
                "Count": [stats["independent"], stats["chain"]],
            })
            fig2 = px.pie(breakdown, values="Count", names="Type",
                          color_discrete_sequence=["#10b981", "#6b7280"], hole=0.4)
            fig2.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=350)
            st.plotly_chart(fig2, use_container_width=True)

        # Row 3: Score distribution + Market insights
        col_scores, col_market = st.columns(2)

        with col_scores:
            st.subheader("Acquisition Score Distribution")
            conn = get_db()
            score_dist = conn.execute("""
                SELECT
                    CASE
                        WHEN acquisition_score >= 70 THEN '70-100 (Strong Buy)'
                        WHEN acquisition_score >= 60 THEN '60-70 (Good)'
                        WHEN acquisition_score >= 50 THEN '50-60 (Average)'
                        WHEN acquisition_score >= 40 THEN '40-50 (Below Avg)'
                        ELSE '0-40 (Weak)'
                    END as bucket,
                    COUNT(*) as cnt
                FROM pharmacies WHERE acquisition_score IS NOT NULL
                GROUP BY bucket ORDER BY bucket DESC
            """).fetchall()
            conn.close()
            if score_dist:
                dist_df = pd.DataFrame([dict(r) for r in score_dist])
                fig3 = px.bar(dist_df, x="bucket", y="cnt",
                              labels={"bucket": "Score Range", "cnt": "Pharmacies"},
                              color="bucket",
                              color_discrete_map={
                                  "70-100 (Strong Buy)": "#10b981",
                                  "60-70 (Good)": "#34d399",
                                  "50-60 (Average)": "#fbbf24",
                                  "40-50 (Below Avg)": "#f97316",
                                  "0-40 (Weak)": "#ef4444",
                              })
                fig3.update_layout(margin=dict(t=10, b=40, l=40, r=10), height=300, showlegend=False)
                st.plotly_chart(fig3, use_container_width=True)

        with col_market:
            st.subheader("Market Insights")
            conn = get_db()
            insights = conn.execute("""
                SELECT
                    AVG(zip_pct_65_plus) as avg_aging,
                    AVG(zip_median_income) as avg_income,
                    AVG(zip_pop_growth_pct) as avg_growth,
                    AVG(zip_pharmacies_per_10k) as avg_competition,
                    AVG(zip_medicare_claims) as avg_medicare
                FROM pharmacies WHERE is_independent = 1 AND zip_population IS NOT NULL
            """).fetchone()
            conn.close()

            if insights:
                st.metric("Avg Population 65+", f"{insights['avg_aging']:.1f}%" if insights['avg_aging'] else "—")
                st.metric("Avg ZIP Median Income", fmt(insights['avg_income'], prefix="$"))
                st.metric("Avg ZIP Pop Growth", f"{insights['avg_growth']:.1f}%" if insights['avg_growth'] else "—")
                st.metric("Avg Pharmacies per 10K", f"{insights['avg_competition']:.1f}" if insights['avg_competition'] else "—")
                st.metric("Avg ZIP Medicare Claims", fmt(insights['avg_medicare']))

        # Row 4: Top chains + top targets preview
        col_chains, col_top = st.columns(2)

        with col_chains:
            st.subheader("Top Chain Parents")
            conn = get_db()
            chains = conn.execute(
                "SELECT chain_parent, COUNT(*) as cnt FROM pharmacies "
                "WHERE chain_parent IS NOT NULL GROUP BY chain_parent ORDER BY cnt DESC LIMIT 10"
            ).fetchall()
            conn.close()
            if chains:
                chain_df = pd.DataFrame([dict(r) for r in chains])
                chain_df.columns = ["Chain", "Locations"]
                st.dataframe(chain_df, use_container_width=True, hide_index=True)

        with col_top:
            st.subheader("Top 100 Acquisition Targets")
            conn = get_db()
            top_targets = conn.execute("""
                SELECT organization_name, city, state, phone,
                       ROUND(acquisition_score, 1) as score,
                       ROUND(competition_score, 0) as comp,
                       zip_pct_65_plus as aging
                FROM pharmacies
                WHERE acquisition_score IS NOT NULL
                ORDER BY acquisition_score DESC LIMIT 100
            """).fetchall()
            conn.close()
            if top_targets:
                target_df = pd.DataFrame([dict(r) for r in top_targets])
                target_df.columns = ["Name", "City", "State", "Phone", "Acq Score", "Competition", "% 65+"]
                st.dataframe(target_df, use_container_width=True, hide_index=True, height=400)


# ═══════════════════════════════════════════════════════════════════════════════
# TOP TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Top Targets":
    st.title("Top Acquisition Targets")
    st.caption("Independent pharmacies ranked by Acquisition Score — combining low competition, "
               "high Medicare demand, aging population, growth trends, and income levels.")

    # Filters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        all_states = get_all_states()
        state_options = ["All States"] + [s for s, c in all_states]
        target_state = st.selectbox("State", state_options, key="target_state")
        target_state_filter = target_state if target_state != "All States" else ""
    with col2:
        min_score = st.slider("Min Acquisition Score", 0, 100, 50)
    with col3:
        target_search = st.text_input("Search", placeholder="Name, city...", key="target_search")
    with col4:
        sort_options = {
            "Acquisition Score": "acquisition_score",
            "Competition (least)": "competition_score",
            "Medicare Demand": "zip_medicare_claims",
            "Aging Population": "zip_pct_65_plus",
            "Median Income": "zip_median_income",
        }
        sort_label = st.selectbox("Sort by", list(sort_options.keys()))
        sort_by = sort_options[sort_label]

    if "target_page" not in st.session_state:
        st.session_state.target_page = 1

    df, total = search_pharmacies(
        search=target_search, state=target_state_filter, independent_only=True,
        min_score=min_score, sort_by=sort_by,
        page=st.session_state.target_page, per_page=50,
    )
    total_pages = max(1, (total + 50 - 1) // 50)

    st.caption(f"**{total:,}** targets found — Page {st.session_state.target_page} of {total_pages}")

    if not df.empty:
        display_cols = ["organization_name", "city", "state", "zip", "phone",
                        "acquisition_score", "competition_score", "market_demand_score",
                        "zip_pct_65_plus", "zip_median_income", "zip_pop_growth_pct",
                        "zip_pharmacy_count"]
        display_cols = [c for c in display_cols if c in df.columns]
        display_df = df[display_cols].copy()

        # Format columns
        rename = {
            "organization_name": "Name", "city": "City", "state": "ST", "zip": "ZIP",
            "acquisition_score": "Acq Score", "competition_score": "Competition",
            "market_demand_score": "Medicare Demand", "zip_pct_65_plus": "% 65+",
            "zip_median_income": "Median Income", "zip_pop_growth_pct": "Pop Growth %",
            "zip_pharmacy_count": "Pharmacies in ZIP", "phone": "Phone",
        }
        display_df = display_df.rename(columns=rename)

        # Round scores
        for col in ["Acq Score", "Competition", "Medicare Demand"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].round(1)
        if "% 65+" in display_df.columns:
            display_df["% 65+"] = display_df["% 65+"].round(1)
        if "Pop Growth %" in display_df.columns:
            display_df["Pop Growth %"] = display_df["Pop Growth %"].round(1)
        if "Median Income" in display_df.columns:
            display_df["Median Income"] = display_df["Median Income"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—"
            )

        event = st.dataframe(
            display_df, use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row",
        )

        # Detail view on click
        if event and event.selection and event.selection.rows:
            selected_idx = event.selection.rows[0]
            selected_row = df.iloc[selected_idx]
            pharmacy_id = int(selected_row["id"])
            detail = get_pharmacy_detail(pharmacy_id)
            if detail:
                st.divider()

                # Score badge
                score = detail.get("acquisition_score")
                score_text = f"Acquisition Score: **{score:.1f}/100**" if score else "Not scored"
                st.subheader(f"{detail['organization_name']}")
                st.caption(score_text)

                # Score breakdown
                st.markdown("#### Score Breakdown")
                bc1, bc2, bc3, bc4, bc5 = st.columns(5)
                bc1.metric("Competition", fmt(detail.get("competition_score"), suffix="/100"),
                           help="Lower competition in ZIP = higher score")
                bc2.metric("Medicare Demand", fmt(detail.get("market_demand_score"), suffix="/100"),
                           help="Medicare prescription volume in area")
                bc3.metric("Aging Pop", f"{detail.get('zip_pct_65_plus', 0) or 0:.1f}%",
                           help="% of ZIP population age 65+")
                bc4.metric("Pop Growth", f"{detail.get('zip_pop_growth_pct', 0) or 0:.1f}%",
                           help="Population growth since 2019")
                bc5.metric("Median Income", fmt(detail.get("zip_median_income"), prefix="$"),
                           help="ZIP code median household income")

                # Details
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Location**")
                    st.text(detail.get("address_line1") or "")
                    if detail.get("address_line2"):
                        st.text(detail["address_line2"])
                    st.text(f"{detail.get('city', '')}, {detail.get('state', '')} {detail.get('zip', '')}")
                    if detail.get("phone"):
                        st.text(f"Phone: {detail['phone']}")

                with c2:
                    st.markdown("**Ownership Signals**")
                    if detail.get("authorized_official_name"):
                        st.text(f"Official: {detail['authorized_official_name']}")
                    if detail.get("authorized_official_title"):
                        st.text(f"Title: {detail['authorized_official_title']}")
                    st.text(f"Entity: {detail.get('ownership_type', 'N/A')}")
                    st.text(f"NPI: {detail['npi']}")

                with c3:
                    st.markdown("**Market Context**")
                    st.text(f"ZIP Population: {fmt(detail.get('zip_population'))}")
                    st.text(f"Pharmacies in ZIP: {detail.get('zip_pharmacy_count', '—')}")
                    st.text(f"Per 10K pop: {detail.get('zip_pharmacies_per_10k', '—')}")
                    st.text(f"ZIP Medicare Claims: {fmt(detail.get('zip_medicare_claims'))}")

        # Pagination
        if total_pages > 1:
            col_prev, _, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button("← Previous", disabled=st.session_state.target_page <= 1, key="tp"):
                    st.session_state.target_page -= 1
                    st.rerun()
            with col_next:
                if st.button("Next →", disabled=st.session_state.target_page >= total_pages, key="tn"):
                    st.session_state.target_page += 1
                    st.rerun()
    else:
        st.info("No targets found with the selected filters.")

    # Scoring methodology
    with st.expander("How is the Acquisition Score calculated?"):
        st.markdown("""
        The Acquisition Score (0-100) combines five factors weighted for M&A relevance:

        | Factor | Weight | What it measures |
        |--------|--------|-----------------|
        | **Competition** | 25% | Fewer pharmacies per capita in the ZIP = higher score |
        | **Medicare Demand** | 25% | Total Medicare Part D claims prescribed in the ZIP |
        | **Aging Population** | 20% | % of ZIP population age 65+ (more prescriptions) |
        | **Population Growth** | 15% | ZIP population change 2019-2024 (growing market) |
        | **Income Level** | 15% | ZIP median household income (better payer mix) |

        **Data sources:** NPI Registry (CMS), Medicare Part D Prescriber PUF (CMS 2023),
        American Community Survey 5-Year (Census Bureau 2024)
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# DIRECTORY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Directory":
    st.title("Pharmacy Directory")

    # Filters
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        search = st.text_input("Search", placeholder="Name, city, NPI...")
    with col2:
        all_states = get_all_states()
        state_options = ["All States"] + [f"{s} ({c:,})" for s, c in all_states]
        state_sel = st.selectbox("State", state_options)
        state_filter = state_sel.split(" (")[0] if state_sel != "All States" else ""
    with col3:
        city = st.text_input("City", placeholder="City name")
    with col4:
        zip_code = st.text_input("ZIP", placeholder="ZIP or prefix")

    fcol1, fcol2 = st.columns(2)
    with fcol1:
        independent_only = st.checkbox("Independent pharmacies only", value=False)
    with fcol2:
        dir_sort = st.selectbox("Sort by", ["Name", "Acquisition Score", "State"],
                                key="dir_sort")
        dir_sort_map = {"Name": "organization_name", "Acquisition Score": "acquisition_score",
                        "State": "organization_name"}
        dir_sort_by = dir_sort_map[dir_sort]

    if "dir_page" not in st.session_state:
        st.session_state.dir_page = 1

    per_page = 50
    df, total = search_pharmacies(
        search=search, state=state_filter, city=city, zip_code=zip_code,
        independent_only=independent_only, sort_by=dir_sort_by,
        page=st.session_state.dir_page, per_page=per_page,
    )
    total_pages = max(1, (total + per_page - 1) // per_page)

    col_info, col_export = st.columns([3, 1])
    with col_info:
        st.caption(f"**{total:,}** results — Page {st.session_state.dir_page} of {total_pages}")
    with col_export:
        if not df.empty:
            csv_data = df.to_csv(index=False)
            st.download_button("Export CSV", csv_data, file_name="pharmacies_export.csv", mime="text/csv")

    if not df.empty:
        display_cols = ["organization_name", "city", "state", "zip", "phone",
                        "is_independent", "chain_parent", "npi",
                        "acquisition_score", "zip_pct_65_plus", "zip_median_income"]
        display_cols = [c for c in display_cols if c in df.columns]
        display_df = df[display_cols].copy()
        display_df["is_independent"] = display_df["is_independent"].map({1: "Independent", 0: "Chain"})
        if "acquisition_score" in display_df.columns:
            display_df["acquisition_score"] = display_df["acquisition_score"].round(1)
        if "zip_pct_65_plus" in display_df.columns:
            display_df["zip_pct_65_plus"] = display_df["zip_pct_65_plus"].round(1)
        if "zip_median_income" in display_df.columns:
            display_df["zip_median_income"] = display_df["zip_median_income"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—"
            )
        display_df.columns = ["Name", "City", "ST", "ZIP", "Phone", "Type", "Chain",
                              "NPI", "Acq Score", "% 65+", "Income"][:len(display_df.columns)]

        event = st.dataframe(display_df, use_container_width=True, hide_index=True,
                             on_select="rerun", selection_mode="single-row")

        if event and event.selection and event.selection.rows:
            selected_idx = event.selection.rows[0]
            pharmacy_id = int(df.iloc[selected_idx]["id"])
            detail = get_pharmacy_detail(pharmacy_id)
            if detail:
                st.divider()
                st.subheader(detail["organization_name"])

                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Location**")
                    st.text(detail.get("address_line1") or "")
                    if detail.get("address_line2"):
                        st.text(detail["address_line2"])
                    st.text(f"{detail.get('city', '')}, {detail.get('state', '')} {detail.get('zip', '')}")
                    if detail.get("phone"):
                        st.text(f"Phone: {detail['phone']}")
                with c2:
                    st.markdown("**Ownership**")
                    if detail.get("authorized_official_name"):
                        st.text(f"Official: {detail['authorized_official_name']}")
                    if detail.get("authorized_official_title"):
                        st.text(f"Title: {detail['authorized_official_title']}")
                    st.text(f"Entity: {detail.get('ownership_type', 'N/A')}")
                    st.text(f"NPI: {detail['npi']}")
                with c3:
                    st.markdown("**Market Data**")
                    st.text(f"Acq Score: {detail.get('acquisition_score', '—')}")
                    st.text(f"ZIP Pop: {fmt(detail.get('zip_population'))}")
                    st.text(f"65+: {detail.get('zip_pct_65_plus', '—')}%")
                    st.text(f"Income: {fmt(detail.get('zip_median_income'), prefix='$')}")
                    st.text(f"Pharmacies in ZIP: {detail.get('zip_pharmacy_count', '—')}")
    else:
        st.info("No pharmacies found. Adjust filters.")

    if total_pages > 1:
        col_prev, _, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button("← Previous", disabled=st.session_state.dir_page <= 1, key="dp"):
                st.session_state.dir_page -= 1
                st.rerun()
        with col_next:
            if st.button("Next →", disabled=st.session_state.dir_page >= total_pages, key="dn"):
                st.session_state.dir_page += 1
                st.rerun()


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
            "Independent Pharmacies",
            "Avg Acquisition Score",
            "Avg % Population 65+",
            "Avg Median Income",
            "Avg Competition (Pharmacies/10K)",
        ])

        conn = get_db()

        metric_map = {
            "Independent Pharmacies": ("COUNT(*)", "is_independent = 1", "Independent Pharmacies"),
            "Avg Acquisition Score": ("AVG(acquisition_score)", "acquisition_score IS NOT NULL", "Avg Score"),
            "Avg % Population 65+": ("AVG(zip_pct_65_plus)", "zip_pct_65_plus IS NOT NULL", "Avg % 65+"),
            "Avg Median Income": ("AVG(zip_median_income)", "zip_median_income IS NOT NULL AND zip_median_income > 0", "Avg Income"),
            "Avg Competition (Pharmacies/10K)": ("AVG(zip_pharmacies_per_10k)", "zip_pharmacies_per_10k IS NOT NULL", "Pharmacies/10K"),
        }

        agg, where_clause, label = metric_map[map_metric]
        state_data = conn.execute(f"""
            SELECT state, {agg} as val
            FROM pharmacies WHERE state IS NOT NULL AND {where_clause}
            GROUP BY state ORDER BY val DESC
        """).fetchall()
        conn.close()

        state_df = pd.DataFrame([dict(r) for r in state_data], columns=["state", "val"])

        if not state_df.empty:
            # Choose color scale
            if "Competition" in map_metric:
                color_scale = "Reds"  # More = worse
            elif "Score" in map_metric:
                color_scale = "Greens"
            else:
                color_scale = "Blues"

            fig = px.choropleth(
                state_df, locations="state", locationmode="USA-states",
                color="val", scope="usa", color_continuous_scale=color_scale,
                labels={"val": label, "state": "State"},
            )
            fig.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=500,
                              geo=dict(bgcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig, use_container_width=True)

            # State table
            st.subheader("State Details")
            state_df.columns = ["State", label]
            if "Income" in label:
                state_df[label] = state_df[label].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
            elif "Score" in label or "65+" in label or "10K" in label:
                state_df[label] = state_df[label].round(1)
            else:
                state_df[label] = state_df[label].astype(int)
            st.dataframe(state_df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# CHANGES
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Changes":
    st.title("Change Tracking")

    conn = get_db()
    total_changes = conn.execute("SELECT COUNT(*) FROM pharmacy_changes").fetchone()[0]

    if total_changes == 0:
        st.info("No changes detected yet. Changes are tracked between pipeline runs — "
                "run the pipeline at least twice to see what changed.")
    else:
        filter_type = st.selectbox("Filter by type", ["All", "new", "updated", "deactivated"])
        where = ""
        params = []
        if filter_type != "All":
            where = "WHERE change_type = ?"
            params = [filter_type]

        changes = conn.execute(
            f"SELECT * FROM pharmacy_changes {where} ORDER BY detected_at DESC LIMIT 200", params
        ).fetchall()

        changes_df = pd.DataFrame([dict(r) for r in changes])
        if not changes_df.empty:
            display_cols = ["change_type", "npi", "organization_name", "field_changed",
                            "old_value", "new_value", "detected_at"]
            display_cols = [c for c in display_cols if c in changes_df.columns]
            changes_df = changes_df[display_cols]
            changes_df.columns = [c.replace("_", " ").title() for c in changes_df.columns]
            st.dataframe(changes_df, use_container_width=True, hide_index=True)

    conn.close()
