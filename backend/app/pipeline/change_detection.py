"""
Change detection — snapshots current state and detects changes after pipeline run.
"""
import logging
from datetime import datetime
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.models import Pharmacy, PharmacyChange

logger = logging.getLogger(__name__)

TRACKED_FIELDS = [
    "organization_name", "dba_name", "address_line1", "city", "state", "zip",
    "phone", "is_chain", "is_independent", "chain_parent", "authorized_official_name",
]


def snapshot_current_state(db: Session) -> dict:
    """Take a snapshot of current pharmacy data for change comparison."""
    result = db.execute(select(Pharmacy))
    pharmacies = result.scalars().all()

    snapshot = {}
    for p in pharmacies:
        snapshot[p.npi] = {field: getattr(p, field) for field in TRACKED_FIELDS}

    logger.info(f"Snapshot captured: {len(snapshot)} existing pharmacies")
    return snapshot


def detect_changes(db: Session, snapshot: dict, updated_npis: set, new_npis: set) -> int:
    """Compare current state against snapshot and record changes."""
    changes_count = 0
    now = datetime.utcnow()

    # New pharmacies
    for npi in new_npis:
        result = db.execute(select(Pharmacy).where(Pharmacy.npi == npi))
        pharmacy = result.scalar_one_or_none()
        if pharmacy:
            change = PharmacyChange(
                npi=npi,
                organization_name=pharmacy.organization_name,
                change_type="new",
                field_changed="all",
                new_value=f"New pharmacy: {pharmacy.organization_name}",
                detected_at=now,
            )
            db.add(change)
            changes_count += 1

    # Updated pharmacies
    for npi in updated_npis:
        if npi not in snapshot:
            continue

        result = db.execute(select(Pharmacy).where(Pharmacy.npi == npi))
        pharmacy = result.scalar_one_or_none()
        if not pharmacy:
            continue

        old_data = snapshot[npi]
        for field in TRACKED_FIELDS:
            old_val = str(old_data.get(field) or "")
            new_val = str(getattr(pharmacy, field) or "")
            if old_val != new_val:
                change = PharmacyChange(
                    npi=npi,
                    organization_name=pharmacy.organization_name,
                    change_type="updated",
                    field_changed=field,
                    old_value=old_val,
                    new_value=new_val,
                    detected_at=now,
                )
                db.add(change)
                changes_count += 1

    db.commit()
    logger.info(f"Change detection: {changes_count} changes recorded")
    return changes_count
