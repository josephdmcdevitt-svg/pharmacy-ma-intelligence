from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, text, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Pharmacy
from app.auth.utils import get_current_user

router = APIRouter(prefix="/api/pharmacies", tags=["pharmacies"])


@router.get("")
async def list_pharmacies(
    search: str = Query(None),
    state: str = Query(None),
    city: str = Query(None),
    zip: str = Query(None),
    is_independent: bool = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    query = select(Pharmacy)
    count_query = select(func.count(Pharmacy.id))

    if search:
        like = f"%{search}%"
        condition = or_(
            Pharmacy.organization_name.ilike(like),
            Pharmacy.dba_name.ilike(like),
            Pharmacy.city.ilike(like),
            Pharmacy.npi.ilike(like),
        )
        query = query.where(condition)
        count_query = count_query.where(condition)

    if state:
        query = query.where(Pharmacy.state == state.upper())
        count_query = count_query.where(Pharmacy.state == state.upper())

    if city:
        query = query.where(Pharmacy.city.ilike(f"%{city}%"))
        count_query = count_query.where(Pharmacy.city.ilike(f"%{city}%"))

    if zip:
        query = query.where(Pharmacy.zip.startswith(zip))
        count_query = count_query.where(Pharmacy.zip.startswith(zip))

    if is_independent is not None:
        query = query.where(Pharmacy.is_independent == is_independent)
        count_query = count_query.where(Pharmacy.is_independent == is_independent)

    total = (await db.execute(count_query)).scalar() or 0
    total_pages = max(1, (total + per_page - 1) // per_page)

    query = query.order_by(Pharmacy.organization_name).offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(query)
    pharmacies = result.scalars().all()

    return {
        "data": [
            {
                "id": p.id,
                "npi": p.npi,
                "organization_name": p.organization_name,
                "dba_name": p.dba_name,
                "city": p.city,
                "state": p.state,
                "zip": p.zip,
                "phone": p.phone,
                "is_independent": p.is_independent,
                "is_chain": p.is_chain,
                "chain_parent": p.chain_parent,
                "medicare_claims_count": p.medicare_claims_count,
            }
            for p in pharmacies
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


@router.get("/states")
async def list_states(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    result = await db.execute(
        select(Pharmacy.state, func.count(Pharmacy.id).label("count"))
        .where(Pharmacy.state.isnot(None))
        .group_by(Pharmacy.state)
        .order_by(Pharmacy.state)
    )
    return [{"state": row.state, "count": row.count} for row in result.all()]


@router.get("/{pharmacy_id}")
async def get_pharmacy(pharmacy_id: int, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    result = await db.execute(select(Pharmacy).where(Pharmacy.id == pharmacy_id))
    pharmacy = result.scalar_one_or_none()
    if not pharmacy:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Pharmacy not found")

    return {
        "id": pharmacy.id,
        "npi": pharmacy.npi,
        "organization_name": pharmacy.organization_name,
        "dba_name": pharmacy.dba_name,
        "entity_type": pharmacy.entity_type,
        "address_line1": pharmacy.address_line1,
        "address_line2": pharmacy.address_line2,
        "city": pharmacy.city,
        "state": pharmacy.state,
        "zip": pharmacy.zip,
        "county": pharmacy.county,
        "phone": pharmacy.phone,
        "fax": pharmacy.fax,
        "taxonomy_code": pharmacy.taxonomy_code,
        "taxonomy_description": pharmacy.taxonomy_description,
        "is_chain": pharmacy.is_chain,
        "is_independent": pharmacy.is_independent,
        "is_institutional": pharmacy.is_institutional,
        "chain_parent": pharmacy.chain_parent,
        "authorized_official_name": pharmacy.authorized_official_name,
        "authorized_official_title": pharmacy.authorized_official_title,
        "authorized_official_phone": pharmacy.authorized_official_phone,
        "ownership_type": pharmacy.ownership_type,
        "medicare_claims_count": pharmacy.medicare_claims_count,
        "medicare_beneficiary_count": pharmacy.medicare_beneficiary_count,
        "medicare_total_cost": pharmacy.medicare_total_cost,
        "latitude": pharmacy.latitude,
        "longitude": pharmacy.longitude,
        "first_seen": pharmacy.first_seen.isoformat() if pharmacy.first_seen else None,
        "last_refreshed": pharmacy.last_refreshed.isoformat() if pharmacy.last_refreshed else None,
    }
