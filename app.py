"""
Pharmacy File Acquisition Intelligence Platform

M&A intelligence dashboard for prescription file acquisitions.
Enriched with CMS Medicare data and U.S. Census demographics.
"""
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
DB_PATH = APP_DIR / "pharmacy_intel.db"
DATA_DIR.mkdir(exist_ok=True)

st.set_page_config(
    page_title="Pharmacy M&A Intelligence",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Authentication ──────────────────────────────────────────────────────────

def check_login():
    if st.session_state.get("authenticated"):
        return True
    st.markdown(
        "<div style='max-width:400px;margin:15vh auto;text-align:center'>"
        "<h1>💊 Pharmacy Intel</h1>"
        "<p style='color:gray'>Prescription File Acquisition Platform</p></div>",
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
            medicare_claims_count INTEGER, medicare_beneficiary_count INTEGER,
            medicare_total_cost REAL, latitude REAL, longitude REAL,
            dedup_key TEXT, first_seen TEXT, last_refreshed TEXT,
            zip_population INTEGER, zip_median_income INTEGER,
            zip_pct_65_plus REAL, zip_pop_growth_pct REAL,
            zip_medicare_claims INTEGER, zip_medicare_cost REAL,
            zip_medicare_beneficiaries INTEGER, zip_pharmacy_count INTEGER,
            zip_pharmacies_per_10k REAL, competition_score REAL,
            market_demand_score REAL, acquisition_score REAL,
            estimated_rx_volume INTEGER, estimated_file_value INTEGER,
            contact_email TEXT, contact_notes TEXT,
            deal_status TEXT DEFAULT 'Not Contacted'
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

# ─── Helpers ─────────────────────────────────────────────────────────────────

def get_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    independent = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    chain = conn.execute("SELECT COUNT(*) FROM pharmacies WHERE is_chain = 1").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM pharmacies WHERE state IS NOT NULL").fetchone()[0]
    avg_score = conn.execute("SELECT AVG(acquisition_score) FROM pharmacies WHERE acquisition_score IS NOT NULL").fetchone()[0]
    total_rx = conn.execute("SELECT SUM(estimated_rx_volume) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    total_file_val = conn.execute("SELECT SUM(estimated_file_value) FROM pharmacies WHERE is_independent = 1").fetchone()[0]
    deal_counts = {}
    for row in conn.execute("SELECT deal_status, COUNT(*) as c FROM pharmacies WHERE deal_status IS NOT NULL AND deal_status != 'Not Contacted' GROUP BY deal_status").fetchall():
        deal_counts[row["deal_status"]] = row["c"]
    conn.close()
    return {
        "total": total, "independent": independent, "chain": chain,
        "states": states, "avg_score": avg_score or 0,
        "total_rx": total_rx or 0, "total_file_val": total_file_val or 0,
        "deals": deal_counts,
    }

def fmt(val, prefix="", suffix=""):
    if val is None: return "—"
    return f"{prefix}{int(val):,}{suffix}"

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
        "estimated_rx_volume": "estimated_rx_volume DESC",
        "estimated_file_value": "estimated_file_value DESC",
        "organization_name": "organization_name ASC",
        "competition_score": "competition_score DESC",
        "zip_pct_65_plus": "zip_pct_65_plus DESC",
        "zip_median_income": "zip_median_income DESC",
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


DEAL_STATUSES = ["Not Contacted", "Researching", "Contacted", "In Discussion", "LOI Sent", "Under Contract", "Closed", "Passed"]

# ─── Sidebar ─────────────────────────────────────────────────────────────────

st.sidebar.markdown("## 💊 Pharmacy Intel")
st.sidebar.caption("File Acquisition Platform")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Top Targets", "Tuck-in Finder", "Directory", "Deal Pipeline", "Market Map"],
    label_visibility="collapsed",
)

st.sidebar.divider()
stats = get_stats()
if stats["total"] > 0:
    st.sidebar.metric("Independent Pharmacies", f"{stats['independent']:,}")
    st.sidebar.metric("Est. Scripts/Year", f"{stats['total_rx']:,.0f}")


# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

if page == "Dashboard":
    st.title("Dashboard")

    if stats["total"] == 0:
        st.info("No data loaded. Run the pipeline script first.")
    else:
        import plotly.express as px

        # Row 1: Key metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Independent Targets", f"{stats['independent']:,}")
        c2.metric("Scripts/Year", f"{stats['total_rx']:,.0f}")
        c3.metric("Avg Acq. Score", f"{stats['avg_score']:.1f}")
        c4.metric("States Covered", stats["states"])

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
                             color_discrete_sequence=["#2563eb"])
                fig.update_layout(margin=dict(t=10, b=40, l=40, r=10), height=350, showlegend=False)
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
                                  "Strong Buy (70+)": "#10b981", "Good (55-70)": "#34d399",
                                  "Average (40-55)": "#fbbf24", "Below Avg (<40)": "#ef4444",
                              }, hole=0.4)
                fig2.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=350)
                st.plotly_chart(fig2, use_container_width=True)

        # Top 100 targets
        st.subheader("Top 100 File Acquisition Targets")
        conn = get_db()
        top = conn.execute("""
            SELECT organization_name, city, state, phone,
                   COALESCE(contact_email, '') as contact_email,
                   estimated_rx_volume,
                   CASE WHEN estimated_rx_volume IS NOT NULL THEN CAST(estimated_rx_volume / 12 AS INTEGER) ELSE NULL END as monthly_scripts,
                   estimated_file_value,
                   ROUND(acquisition_score, 1) as score,
                   zip_pct_65_plus, deal_status
            FROM pharmacies WHERE acquisition_score IS NOT NULL
            ORDER BY acquisition_score DESC LIMIT 100
        """).fetchall()
        conn.close()
        if top:
            top_df = pd.DataFrame([dict(r) for r in top])
            top_df.columns = ["Name", "City", "State", "Phone", "Email",
                              "Scripts/Yr", "Scripts/Mo",
                              "Est. File Value ($)",
                              "Acq Score", "% 65+", "Deal Status"]
            top_df["Est. File Value ($)"] = top_df["Est. File Value ($)"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—")
            top_df["Scripts/Yr"] = top_df["Scripts/Yr"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
            top_df["Scripts/Mo"] = top_df["Scripts/Mo"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
            if "% 65+" in top_df.columns:
                top_df["% 65+"] = top_df["% 65+"].round(1)
            st.dataframe(top_df, use_container_width=True, hide_index=True, height=600)


# ═══════════════════════════════════════════════════════════════════════════════
# TOP TARGETS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Top Targets":
    st.title("File Acquisition Targets")
    st.caption("Independent pharmacies ranked by file value — estimated Rx volume, competition, demographics.")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        all_states = get_all_states()
        state_options = ["All States"] + [s for s, c in all_states]
        target_state = st.selectbox("State", state_options, key="target_state")
        target_state_filter = target_state if target_state != "All States" else ""
    with col2:
        min_score = st.slider("Min Score", 0, 100, 40)
    with col3:
        target_search = st.text_input("Search", placeholder="Name, city...", key="target_search")
    with col4:
        sort_options = {
            "Acquisition Score": "acquisition_score",
            "Est. Rx Volume": "estimated_rx_volume",
            "Est. File Value": "estimated_file_value",
            "Low Competition": "competition_score",
            "Aging Population": "zip_pct_65_plus",
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

    col_info, col_export = st.columns([3, 1])
    with col_info:
        st.caption(f"**{total:,}** targets — Page {st.session_state.target_page} of {total_pages}")
    with col_export:
        if not df.empty:
            export_cols = ["organization_name", "city", "state", "zip", "phone", "contact_email",
                           "authorized_official_name", "authorized_official_phone",
                           "estimated_rx_volume", "estimated_file_value", "acquisition_score",
                           "zip_pct_65_plus", "zip_median_income", "deal_status", "contact_notes"]
            export_cols = [c for c in export_cols if c in df.columns]
            st.download_button("Export Outreach List", df[export_cols].to_csv(index=False),
                               file_name="file_acquisition_targets.csv", mime="text/csv")

    if not df.empty:
        display_cols = ["organization_name", "city", "state", "phone", "contact_email",
                        "estimated_rx_volume", "estimated_file_value", "acquisition_score",
                        "zip_pct_65_plus", "zip_pharmacy_count", "deal_status"]
        display_cols = [c for c in display_cols if c in df.columns]
        display_df = df[display_cols].copy()
        # Add monthly scripts column
        if "estimated_rx_volume" in display_df.columns:
            display_df.insert(
                display_df.columns.get_loc("estimated_rx_volume") + 1,
                "monthly_scripts",
                (display_df["estimated_rx_volume"] / 12).apply(lambda x: int(x) if pd.notna(x) else None),
            )
        if "contact_email" in display_df.columns:
            display_df["contact_email"] = display_df["contact_email"].fillna("")
        if "acquisition_score" in display_df.columns:
            display_df["acquisition_score"] = display_df["acquisition_score"].round(1)
        if "estimated_file_value" in display_df.columns:
            display_df["estimated_file_value"] = display_df["estimated_file_value"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—")
        if "estimated_rx_volume" in display_df.columns:
            display_df["estimated_rx_volume"] = display_df["estimated_rx_volume"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
        if "monthly_scripts" in display_df.columns:
            display_df["monthly_scripts"] = display_df["monthly_scripts"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
        if "zip_pct_65_plus" in display_df.columns:
            display_df["zip_pct_65_plus"] = display_df["zip_pct_65_plus"].round(1)

        rename = {"organization_name": "Name", "city": "City", "state": "ST", "phone": "Phone",
                  "contact_email": "Email", "estimated_rx_volume": "Scripts/Yr",
                  "monthly_scripts": "Scripts/Mo",
                  "estimated_file_value": "Est. File Value", "acquisition_score": "Score",
                  "zip_pct_65_plus": "% 65+", "zip_pharmacy_count": "Pharmacies in ZIP",
                  "deal_status": "Status"}
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

                # File value metrics
                fc1, fc2, fc3, fc4, fc5 = st.columns(5)
                fc1.metric("Scripts/Year", fmt(detail.get("estimated_rx_volume")))
                monthly = int(detail["estimated_rx_volume"] / 12) if detail.get("estimated_rx_volume") else None
                fc2.metric("Scripts/Month", fmt(monthly))
                fc3.metric("Est. File Value", fmt(detail.get("estimated_file_value"), prefix="$"))
                fc4.metric("Acq. Score", f"{score:.1f}/100" if score else "—")
                fc5.metric("Pharmacies in ZIP", detail.get("zip_pharmacy_count", "—"))

                # Contact info box (highlighted)
                st.markdown("---")
                st.markdown("#### Contact & Outreach")
                with st.form(f"contact_{pharmacy_id}"):
                    row1 = st.columns([1, 1, 1, 1])
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
                        new_email = st.text_input("Contact Email", value=detail.get("contact_email") or "",
                                                  placeholder="Enter email address")
                    with row1[3]:
                        current_status = detail.get("deal_status") or "Not Contacted"
                        status_idx = DEAL_STATUSES.index(current_status) if current_status in DEAL_STATUSES else 0
                        new_status = st.selectbox("Deal Status", DEAL_STATUSES, index=status_idx)

                    new_notes = st.text_area("Notes", value=detail.get("contact_notes") or "",
                                             height=80, placeholder="Add notes about this target...")
                    if st.form_submit_button("Save Contact Info", use_container_width=True, type="primary"):
                        update_pharmacy_contact(pharmacy_id, email=new_email, notes=new_notes, deal_status=new_status)
                        st.success("Saved!")
                        st.rerun()

                # Location, Ownership, Market details
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Location**")
                    st.text(detail.get("address_line1") or "")
                    if detail.get("address_line2"):
                        st.text(detail["address_line2"])
                    st.text(f"{detail.get('city', '')}, {detail.get('state', '')} {detail.get('zip', '')}")

                with c2:
                    st.markdown("**Ownership**")
                    if detail.get("authorized_official_title"):
                        st.text(f"Title: {detail['authorized_official_title']}")
                    st.text(f"Entity: {detail.get('ownership_type', 'N/A')}")
                    st.text(f"NPI: {detail['npi']}")

                with c3:
                    st.markdown("**Market**")
                    st.text(f"ZIP Pop: {fmt(detail.get('zip_population'))}")
                    st.text(f"65+: {detail.get('zip_pct_65_plus', '—')}%")
                    st.text(f"Income: {fmt(detail.get('zip_median_income'), prefix='$')}")
                    st.text(f"Pop Growth: {detail.get('zip_pop_growth_pct', '—')}%")
                    st.text(f"Medicare Claims (ZIP): {fmt(detail.get('zip_medicare_claims'))}")

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
        st.info("No targets match filters.")

    with st.expander("Scoring Methodology"):
        st.markdown("""
        **Acquisition Score (0-100)** is tuned for prescription file purchases:

        | Factor | Weight | Why it matters for file buys |
        |--------|--------|----------------------------|
        | **Est. Rx Volume** | 35% | More scripts = more valuable file |
        | **Low Competition** | 25% | Easier to retain patients at your store |
        | **Aging Population** | 20% | 65+ patients fill 2-3x more scripts, stickier |
        | **Income Level** | 10% | Better payer mix = higher value per script |
        | **Pop Growth** | 10% | Growing market = file value appreciates |

        **Est. File Value** = Est. Scripts/Year x $4/script (industry standard for file purchases)
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# TUCK-IN FINDER
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Tuck-in Finder":
    st.title("Tuck-in Finder")
    st.caption("Enter your store's ZIP code to find nearby independent pharmacies whose files you can absorb.")

    col1, col2 = st.columns([1, 2])
    with col1:
        my_zip = st.text_input("Your Store ZIP Code", placeholder="e.g. 10001", max_chars=5)
        radius_option = st.selectbox("Search Radius", ["Same ZIP", "Nearby ZIPs (±2)", "Nearby ZIPs (±5)"])
        min_rx = st.number_input("Min Est. Scripts/Year", value=5000, step=5000)

    if my_zip and len(my_zip) == 5:
        conn = get_db()

        if radius_option == "Same ZIP":
            zip_condition = "zip = ?"
            zip_params = [my_zip]
        elif radius_option == "Nearby ZIPs (±2)":
            try:
                z = int(my_zip)
                zips = [str(z + i).zfill(5) for i in range(-2, 3)]
                zip_condition = f"zip IN ({','.join(['?'] * len(zips))})"
                zip_params = zips
            except:
                zip_condition = "zip = ?"
                zip_params = [my_zip]
        else:
            try:
                z = int(my_zip)
                zips = [str(z + i).zfill(5) for i in range(-5, 6)]
                zip_condition = f"zip IN ({','.join(['?'] * len(zips))})"
                zip_params = zips
            except:
                zip_condition = "zip = ?"
                zip_params = [my_zip]

        nearby = conn.execute(f"""
            SELECT * FROM pharmacies
            WHERE is_independent = 1 AND {zip_condition}
              AND (estimated_rx_volume >= ? OR estimated_rx_volume IS NULL)
            ORDER BY estimated_rx_volume DESC
        """, zip_params + [min_rx]).fetchall()
        conn.close()

        with col2:
            if nearby:
                st.success(f"Found **{len(nearby)}** independent pharmacies near ZIP {my_zip}")
                nearby_df = pd.DataFrame([dict(r) for r in nearby])

                total_rx = nearby_df["estimated_rx_volume"].sum()
                total_val = nearby_df["estimated_file_value"].sum()
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric("Targets Found", len(nearby))
                mc2.metric("Total Est. Scripts", f"{total_rx:,.0f}")
                mc3.metric("Total Est. File Value", f"${total_val:,.0f}")

                display_cols = ["organization_name", "city", "state", "zip", "phone", "contact_email",
                                "estimated_rx_volume", "estimated_file_value", "acquisition_score",
                                "authorized_official_name", "deal_status"]
                display_cols = [c for c in display_cols if c in nearby_df.columns]
                disp = nearby_df[display_cols].copy()
                if "contact_email" in disp.columns:
                    disp["contact_email"] = disp["contact_email"].fillna("")
                # Add monthly scripts
                if "estimated_rx_volume" in disp.columns:
                    disp.insert(
                        disp.columns.get_loc("estimated_rx_volume") + 1,
                        "monthly_scripts",
                        (disp["estimated_rx_volume"] / 12).apply(lambda x: int(x) if pd.notna(x) else None),
                    )
                if "acquisition_score" in disp.columns:
                    disp["acquisition_score"] = disp["acquisition_score"].round(1)
                if "estimated_file_value" in disp.columns:
                    disp["estimated_file_value"] = disp["estimated_file_value"].apply(
                        lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—")
                if "estimated_rx_volume" in disp.columns:
                    disp["estimated_rx_volume"] = disp["estimated_rx_volume"].apply(
                        lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
                if "monthly_scripts" in disp.columns:
                    disp["monthly_scripts"] = disp["monthly_scripts"].apply(
                        lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
                disp.columns = ["Name", "City", "ST", "ZIP", "Phone", "Email",
                                "Scripts/Yr", "Scripts/Mo", "File Value", "Score",
                                "Owner", "Status"][:len(disp.columns)]

                st.dataframe(disp, use_container_width=True, hide_index=True)

                # Export
                st.download_button(
                    "Export Tuck-in List",
                    nearby_df[["organization_name", "city", "state", "zip", "phone",
                               "contact_email", "authorized_official_name",
                               "estimated_rx_volume", "estimated_file_value",
                               "acquisition_score", "deal_status"]
                    ].to_csv(index=False),
                    file_name=f"tuckin_targets_ZIP_{my_zip}.csv", mime="text/csv",
                )
            else:
                st.warning(f"No independent pharmacies found near ZIP {my_zip} with {min_rx:,}+ scripts.")


# ═══════════════════════════════════════════════════════════════════════════════
# DIRECTORY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Directory":
    st.title("Full Directory")

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

    fcol1, fcol2 = st.columns(2)
    with fcol1:
        independent_only = st.checkbox("Independent only", value=True)
    with fcol2:
        sort_opts = {"Acq. Score": "acquisition_score", "Name": "organization_name",
                     "Est. Scripts": "estimated_rx_volume", "File Value": "estimated_file_value"}
        sort_label = st.selectbox("Sort", list(sort_opts.keys()))

    if "dir_page" not in st.session_state:
        st.session_state.dir_page = 1

    df, total = search_pharmacies(
        search=search, state=state_filter, city=city, zip_code=zip_code,
        independent_only=independent_only, sort_by=sort_opts[sort_label],
        page=st.session_state.dir_page, per_page=50,
    )
    total_pages = max(1, (total + 50 - 1) // 50)

    col_info, col_export = st.columns([3, 1])
    with col_info:
        st.caption(f"**{total:,}** results — Page {st.session_state.dir_page} of {total_pages}")
    with col_export:
        if not df.empty:
            st.download_button("Export CSV", df.to_csv(index=False),
                               file_name="pharmacies.csv", mime="text/csv")

    if not df.empty:
        cols = ["organization_name", "city", "state", "zip", "phone", "contact_email",
                "is_independent", "estimated_rx_volume", "estimated_file_value",
                "acquisition_score", "deal_status"]
        cols = [c for c in cols if c in df.columns]
        disp = df[cols].copy()
        disp["is_independent"] = disp["is_independent"].map({1: "Independent", 0: "Chain"})
        if "contact_email" in disp.columns:
            disp["contact_email"] = disp["contact_email"].fillna("")
        # Add monthly scripts
        if "estimated_rx_volume" in disp.columns:
            disp.insert(
                disp.columns.get_loc("estimated_rx_volume") + 1,
                "monthly_scripts",
                (disp["estimated_rx_volume"] / 12).apply(lambda x: int(x) if pd.notna(x) else None),
            )
        if "acquisition_score" in disp.columns:
            disp["acquisition_score"] = disp["acquisition_score"].round(1)
        if "estimated_file_value" in disp.columns:
            disp["estimated_file_value"] = disp["estimated_file_value"].apply(
                lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—")
        if "estimated_rx_volume" in disp.columns:
            disp["estimated_rx_volume"] = disp["estimated_rx_volume"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
        if "monthly_scripts" in disp.columns:
            disp["monthly_scripts"] = disp["monthly_scripts"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
        disp.columns = ["Name", "City", "ST", "ZIP", "Phone", "Email", "Type",
                        "Scripts/Yr", "Scripts/Mo", "File Value", "Score", "Status"][:len(disp.columns)]
        st.dataframe(disp, use_container_width=True, hide_index=True)

    if total_pages > 1:
        cp, _, cn = st.columns([1, 2, 1])
        with cp:
            if st.button("← Prev", disabled=st.session_state.dir_page <= 1, key="dp"):
                st.session_state.dir_page -= 1
                st.rerun()
        with cn:
            if st.button("Next →", disabled=st.session_state.dir_page >= total_pages, key="dn"):
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
               SUM(estimated_rx_volume) as total_rx,
               SUM(estimated_file_value) as total_val
        FROM pharmacies
        WHERE deal_status IS NOT NULL AND deal_status != 'Not Contacted'
        GROUP BY deal_status
    """).fetchall()

    if not pipeline:
        st.info("No deals in the pipeline yet. Go to **Top Targets**, click a pharmacy, "
                "and update its Deal Status to start tracking.")
    else:
        # Summary metrics
        pipeline_df = pd.DataFrame([dict(r) for r in pipeline])
        total_deals = pipeline_df["cnt"].sum()
        total_rx = pipeline_df["total_rx"].sum()
        total_val = pipeline_df["total_val"].sum()

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Active Deals", int(total_deals))
        mc2.metric("Pipeline Scripts/Yr", f"{total_rx:,.0f}" if total_rx else "0")
        mc3.metric("Pipeline File Value", f"${total_val:,.0f}" if total_val else "$0")

        # Status breakdown
        st.subheader("By Status")
        for _, row in pipeline_df.iterrows():
            st.markdown(f"**{row['deal_status']}** — {int(row['cnt'])} deals, "
                        f"{int(row['total_rx'] or 0):,} scripts, "
                        f"${int(row['total_val'] or 0):,} est. file value")

        # Detail table for each status
        for _, row in pipeline_df.iterrows():
            status = row["deal_status"]
            deals = conn.execute("""
                SELECT id, organization_name, city, state, phone,
                       COALESCE(contact_email, '') as contact_email,
                       estimated_rx_volume,
                       CASE WHEN estimated_rx_volume IS NOT NULL THEN CAST(estimated_rx_volume / 12 AS INTEGER) ELSE NULL END as monthly_scripts,
                       estimated_file_value, contact_notes
                FROM pharmacies WHERE deal_status = ?
                ORDER BY estimated_file_value DESC
            """, (status,)).fetchall()

            if deals:
                st.markdown(f"#### {status}")
                deals_df = pd.DataFrame([dict(r) for r in deals])
                if "estimated_file_value" in deals_df.columns:
                    deals_df["estimated_file_value"] = deals_df["estimated_file_value"].apply(
                        lambda x: f"${x:,.0f}" if pd.notna(x) and x else "—")
                if "estimated_rx_volume" in deals_df.columns:
                    deals_df["estimated_rx_volume"] = deals_df["estimated_rx_volume"].apply(
                        lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
                if "monthly_scripts" in deals_df.columns:
                    deals_df["monthly_scripts"] = deals_df["monthly_scripts"].apply(
                        lambda x: f"{x:,.0f}" if pd.notna(x) and x else "—")
                deals_df = deals_df.drop(columns=["id"], errors="ignore")
                deals_df.columns = ["Name", "City", "ST", "Phone", "Email",
                                    "Scripts/Yr", "Scripts/Mo", "File Value", "Notes"][:len(deals_df.columns)]
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
            "Independent Pharmacies", "Avg File Value", "Avg Rx Volume",
            "Avg % Population 65+", "Avg Median Income", "Avg Competition",
        ])

        conn = get_db()
        metric_map = {
            "Independent Pharmacies": ("COUNT(*)", "is_independent = 1", "Independents"),
            "Avg File Value": ("AVG(estimated_file_value)", "is_independent = 1 AND estimated_file_value > 0", "Avg File Value ($)"),
            "Avg Rx Volume": ("AVG(estimated_rx_volume)", "is_independent = 1 AND estimated_rx_volume > 0", "Avg Scripts/Yr"),
            "Avg % Population 65+": ("AVG(zip_pct_65_plus)", "zip_pct_65_plus IS NOT NULL", "Avg % 65+"),
            "Avg Median Income": ("AVG(zip_median_income)", "zip_median_income IS NOT NULL AND zip_median_income > 0", "Avg Income ($)"),
            "Avg Competition": ("AVG(zip_pharmacies_per_10k)", "zip_pharmacies_per_10k IS NOT NULL", "Pharmacies/10K"),
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
            color_scale = "Reds" if "Competition" in map_metric else "Greens" if "File" in map_metric or "Rx" in map_metric else "Blues"
            fig = px.choropleth(state_df, locations="state", locationmode="USA-states",
                                color="val", scope="usa", color_continuous_scale=color_scale,
                                labels={"val": label, "state": "State"})
            fig.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=500)
            st.plotly_chart(fig, use_container_width=True)

            state_df.columns = ["State", label]
            if "$" in label:
                state_df[label] = state_df[label].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "—")
            elif "%" in label or "10K" in label:
                state_df[label] = state_df[label].round(1)
            else:
                state_df[label] = state_df[label].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
            st.dataframe(state_df, use_container_width=True, hide_index=True)
