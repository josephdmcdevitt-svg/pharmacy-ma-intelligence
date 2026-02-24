import io
import csv
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Pharmacy
from app.auth.utils import get_current_user

router = APIRouter(prefix="/api/exports", tags=["exports"])


@router.get("/csv")
async def export_csv(
    search: str = Query(None),
    state: str = Query(None),
    is_independent: bool = Query(None),
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    query = select(Pharmacy)

    if search:
        like = f"%{search}%"
        query = query.where(
            or_(
                Pharmacy.organization_name.ilike(like),
                Pharmacy.city.ilike(like),
                Pharmacy.npi.ilike(like),
            )
        )

    if state:
        query = query.where(Pharmacy.state == state.upper())

    if is_independent is not None:
        query = query.where(Pharmacy.is_independent == is_independent)

    query = query.order_by(Pharmacy.organization_name).limit(10000)
    result = await db.execute(query)
    pharmacies = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "NPI", "Organization Name", "DBA Name", "Address", "City", "State", "ZIP",
        "Phone", "Type", "Chain Parent", "Medicare Claims", "Medicare Cost",
    ])
    for p in pharmacies:
        writer.writerow([
            p.npi, p.organization_name, p.dba_name, p.address_line1,
            p.city, p.state, p.zip, p.phone,
            "Independent" if p.is_independent else "Chain",
            p.chain_parent or "",
            p.medicare_claims_count or "",
            p.medicare_total_cost or "",
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=pharmacies_export.csv"},
    )
