from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import PharmacyChange
from app.auth.utils import get_current_user

router = APIRouter(prefix="/api/changes", tags=["changes"])


@router.get("")
async def list_changes(
    change_type: str = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    query = select(PharmacyChange)
    count_query = select(func.count(PharmacyChange.id))

    if change_type:
        query = query.where(PharmacyChange.change_type == change_type)
        count_query = count_query.where(PharmacyChange.change_type == change_type)

    total = (await db.execute(count_query)).scalar() or 0
    total_pages = max(1, (total + per_page - 1) // per_page)

    query = query.order_by(PharmacyChange.detected_at.desc()).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(query)
    changes = result.scalars().all()

    return {
        "data": [
            {
                "id": c.id,
                "npi": c.npi,
                "organization_name": c.organization_name,
                "change_type": c.change_type,
                "field_changed": c.field_changed,
                "old_value": c.old_value,
                "new_value": c.new_value,
                "detected_at": c.detected_at.isoformat() if c.detected_at else None,
            }
            for c in changes
        ],
        "total": total,
        "page": page,
        "total_pages": total_pages,
    }
