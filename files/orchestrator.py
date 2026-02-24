"""
Pipeline Orchestrator

Runs the full data pipeline:
1. Download NPI data
2. Parse and normalize records
3. Chain/institutional filtering
4. Ownership signal extraction
5. Load to database
6. Enrich with CMS Medicare data
7. Geographic enrichment
8. Change detection
9. Update search vectors
"""
import logging
from datetime import datetime

from sqlalchemy import create_engine, text, select, func
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import Base
from app.models import Pharmacy, PipelineRun
from app.pipeline.sources.npi import download_nppes, parse_nppes
from app.pipeline.sources.cms import download_cms_partd, parse_cms_partd
from app.pipeline.sources.census import download_geographic_data
from app.pipeline.normalize import normalize_record, generate_dedup_key
from app.pipeline.chain_filter import classify_pharmacy, cluster_multi_location, extract_ownership_signals
from app.pipeline.change_detection import snapshot_current_state, detect_changes

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

settings = get_settings()


def run_pipeline():
    """Execute the full data pipeline."""
    engine = create_engine(settings.DATABASE_URL_SYNC, echo=False)
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        # Create pipeline run record
        pipeline_run = PipelineRun(started_at=datetime.utcnow(), status="running")
        db.add(pipeline_run)
        db.commit()

        try:
            # Step 0: Snapshot current state for change detection
            snapshot = snapshot_current_state(db)

            # Step 1: Download NPI data
            logger.info("=" * 60)
            logger.info("STAGE 1: Downloading NPPES data...")
            logger.info("=" * 60)
            csv_path = download_nppes(settings.DATA_DIR)

            # Step 2: Parse, normalize, classify, and load
            logger.info("=" * 60)
            logger.info("STAGE 2: Parsing and loading pharmacy records...")
            logger.info("=" * 60)

            records_processed = 0
            records_added = 0
            records_updated = 0
            new_npis = set()
            updated_npis = set()
            all_records = []

            for chunk in parse_nppes(csv_path):
                for record in chunk:
                    records_processed += 1

                    # Normalize
                    record = normalize_record(record)

                    # Classify chain/independent
                    record = classify_pharmacy(record)

                    # Extract ownership signals
                    record = extract_ownership_signals(record)

                    all_records.append(record)

                # Batch upsert
                for record in all_records:
                    npi = record["npi"]
                    existing = db.execute(
                        select(Pharmacy).where(Pharmacy.npi == npi)
                    ).scalar_one_or_none()

                    if existing:
                        # Update existing record
                        for key, value in record.items():
                            if key != "npi" and value is not None:
                                setattr(existing, key, value)
                        existing.last_refreshed = datetime.utcnow()
                        records_updated += 1
                        updated_npis.add(npi)
                    else:
                        # Insert new record
                        pharmacy = Pharmacy(
                            **record,
                            first_seen=datetime.utcnow(),
                            last_refreshed=datetime.utcnow(),
                        )
                        db.add(pharmacy)
                        records_added += 1
                        new_npis.add(npi)

                db.commit()
                all_records = []
                logger.info(f"  Loaded batch. Total: {records_processed:,} processed, {records_added:,} new, {records_updated:,} updated")

            # Step 3: Multi-location clustering
            logger.info("=" * 60)
            logger.info("STAGE 3: Multi-location clustering...")
            logger.info("=" * 60)
            _run_multi_location_clustering(db)

            # Step 4: CMS Medicare enrichment
            logger.info("=" * 60)
            logger.info("STAGE 4: CMS Medicare Part D enrichment...")
            logger.info("=" * 60)
            _enrich_medicare(db)

            # Step 5: Geographic enrichment
            logger.info("=" * 60)
            logger.info("STAGE 5: Geographic enrichment...")
            logger.info("=" * 60)
            _enrich_geography(db)

            # Step 6: Change detection
            logger.info("=" * 60)
            logger.info("STAGE 6: Change detection...")
            logger.info("=" * 60)
            changes_detected = detect_changes(db, snapshot, updated_npis, new_npis)

            # Step 7: Update search vectors
            logger.info("=" * 60)
            logger.info("STAGE 7: Updating search vectors...")
            logger.info("=" * 60)
            _update_search_vectors(db)

            # Complete pipeline run
            pipeline_run.completed_at = datetime.utcnow()
            pipeline_run.status = "completed"
            pipeline_run.records_processed = records_processed
            pipeline_run.records_added = records_added
            pipeline_run.records_updated = records_updated
            pipeline_run.changes_detected = changes_detected
            db.commit()

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info(f"  Records processed: {records_processed:,}")
            logger.info(f"  New records: {records_added:,}")
            logger.info(f"  Updated records: {records_updated:,}")
            logger.info(f"  Changes detected: {changes_detected}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            pipeline_run.completed_at = datetime.utcnow()
            pipeline_run.status = "failed"
            pipeline_run.error_log = str(e)
            db.commit()
            raise


def _run_multi_location_clustering(db: Session):
    """Flag multi-location operators that slipped through keyword filtering."""
    result = db.execute(
        text("""
            UPDATE pharmacies SET is_chain = true, is_independent = false, chain_parent = 'Multi-Location Operator'
            WHERE is_independent = true AND organization_name IN (
                SELECT organization_name FROM pharmacies
                WHERE is_independent = true
                GROUP BY organization_name
                HAVING COUNT(*) >= 10
            )
        """)
    )
    db.commit()
    logger.info(f"Multi-location clustering updated {result.rowcount} records")


def _enrich_medicare(db: Session):
    """Join CMS Part D data to pharmacy records."""
    try:
        csv_path = download_cms_partd(settings.DATA_DIR)
        if not csv_path:
            logger.info("No CMS data available, skipping Medicare enrichment")
            return

        cms_data = parse_cms_partd(csv_path)
        if not cms_data:
            return

        updated = 0
        for npi, metrics in cms_data.items():
            result = db.execute(
                text("""
                    UPDATE pharmacies
                    SET medicare_claims_count = :claims,
                        medicare_beneficiary_count = :benes,
                        medicare_total_cost = :cost
                    WHERE npi = :npi
                """),
                {
                    "npi": npi,
                    "claims": metrics.get("medicare_claims_count"),
                    "benes": metrics.get("medicare_beneficiary_count"),
                    "cost": metrics.get("medicare_total_cost"),
                },
            )
            if result.rowcount > 0:
                updated += 1

            if updated % 10000 == 0 and updated > 0:
                db.commit()

        db.commit()
        logger.info(f"Medicare enrichment: updated {updated:,} pharmacies")

    except Exception as e:
        logger.warning(f"Medicare enrichment failed (non-fatal): {e}")


def _enrich_geography(db: Session):
    """Add geographic context data."""
    try:
        county_data = download_geographic_data(settings.DATA_DIR)
        # Geographic enrichment would map ZIP to county and add RUCC codes
        # For now, this is a placeholder that will be enhanced with ZIP-to-FIPS crosswalk
        logger.info(f"Geographic data loaded: {len(county_data)} counties available")
    except Exception as e:
        logger.warning(f"Geographic enrichment failed (non-fatal): {e}")


def _update_search_vectors(db: Session):
    """Build full-text search vectors for all pharmacies."""
    db.execute(
        text("""
            UPDATE pharmacies SET search_vector =
                to_tsvector('english',
                    coalesce(organization_name, '') || ' ' ||
                    coalesce(dba_name, '') || ' ' ||
                    coalesce(city, '') || ' ' ||
                    coalesce(state, '') || ' ' ||
                    coalesce(zip, '') || ' ' ||
                    coalesce(county, '') || ' ' ||
                    coalesce(npi, '')
                )
        """)
    )
    db.commit()
    logger.info("Search vectors updated")


if __name__ == "__main__":
    run_pipeline()
