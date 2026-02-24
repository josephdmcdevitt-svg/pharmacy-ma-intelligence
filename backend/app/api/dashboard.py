from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Pharmacy, PharmacyChange, PipelineRun
from app.auth.utils import get_current_user

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def dashboard_stats(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    total = (await db.execute(select(func.count(Pharmacy.id)))).scalar() or 0
    independent = (await db.execute(
        select(func.count(Pharmacy.id)).where(Pharmacy.is_independent == True)
    )).scalar() or 0
    chain = (await db.execute(
        select(func.count(Pharmacy.id)).where(Pharmacy.is_chain == True)
    )).scalar() or 0
    states_covered = (await db.execute(
        select(func.count(func.distinct(Pharmacy.state)))
    )).scalar() or 0
    recent_changes = (await db.execute(
        select(func.count(PharmacyChange.id))
    )).scalar() or 0

    # Top states
    top_states_result = await db.execute(
        select(Pharmacy.state, func.count(Pharmacy.id).label("count"))
        .where(Pharmacy.state.isnot(None))
        .group_by(Pharmacy.state)
        .order_by(func.count(Pharmacy.id).desc())
        .limit(10)
    )
    top_states = [{"state": row.state, "count": row.count} for row in top_states_result.all()]

    # Recent pipeline runs
    runs_result = await db.execute(
        select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(5)
    )
    runs = runs_result.scalars().all()

    return {
        "total_pharmacies": total,
        "independent_count": independent,
        "chain_count": chain,
        "states_covered": states_covered,
        "recent_changes": recent_changes,
        "top_states": top_states,
        "recent_runs": [
            {
                "id": r.id,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "records_processed": r.records_processed,
            }
            for r in runs
        ],
    }
