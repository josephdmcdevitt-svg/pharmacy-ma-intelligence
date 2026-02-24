import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.database import engine, Base, sync_engine, AsyncSessionLocal
from app.models import User
from app.auth.utils import hash_password
from app.auth.router import router as auth_router
from app.api.pharmacies import router as pharmacies_router
from app.api.changes import router as changes_router
from app.api.exports import router as exports_router
from app.api.dashboard import router as dashboard_router
from app.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Seed admin user if not exists
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
        result = await session.execute(select(User).where(User.email == settings.ADMIN_EMAIL))
        if not result.scalar_one_or_none():
            admin = User(
                email=settings.ADMIN_EMAIL,
                password_hash=hash_password(settings.ADMIN_PASSWORD),
                name="Admin",
                is_active=True,
            )
            session.add(admin)
            await session.commit()
            logger.info(f"Admin user created: {settings.ADMIN_EMAIL}")

    yield

    await engine.dispose()


app = FastAPI(
    title="Pharmacy Acquisition Intelligence",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(pharmacies_router)
app.include_router(changes_router)
app.include_router(exports_router)
app.include_router(dashboard_router)


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "pharmacy-intel"}


@app.post("/api/pipeline/trigger")
async def trigger_pipeline():
    """Manually trigger a pipeline run (runs in background)."""
    import threading
    from app.pipeline.orchestrator import run_pipeline

    def _run():
        try:
            run_pipeline()
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return {"status": "started", "message": "Pipeline running in background"}


@app.get("/api/pipeline/status")
async def pipeline_status():
    from app.models import PipelineRun
    from sqlalchemy import select
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(1)
        )
        run = result.scalar_one_or_none()
        if run:
            return {
                "status": run.status,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "records_processed": run.records_processed,
            }
        return {"status": "never_run"}
