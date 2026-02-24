from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, Float, DateTime, Text, Index
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Pharmacy(Base):
    __tablename__ = "pharmacies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    npi = Column(String(10), unique=True, nullable=False, index=True)
    organization_name = Column(String(500), index=True)
    dba_name = Column(String(500))
    entity_type = Column(String(50))

    # Address
    address_line1 = Column(String(500))
    address_line2 = Column(String(500))
    city = Column(String(255), index=True)
    state = Column(String(2), index=True)
    zip = Column(String(10), index=True)
    county = Column(String(255))
    phone = Column(String(20))
    fax = Column(String(20))

    # Classification
    taxonomy_code = Column(String(20))
    taxonomy_description = Column(String(500))
    is_chain = Column(Boolean, default=False, index=True)
    is_independent = Column(Boolean, default=True, index=True)
    is_institutional = Column(Boolean, default=False)
    chain_parent = Column(String(255))

    # Ownership signals
    authorized_official_name = Column(String(500))
    authorized_official_title = Column(String(255))
    authorized_official_phone = Column(String(20))
    ownership_type = Column(String(100))

    # Medicare data
    medicare_claims_count = Column(Integer)
    medicare_beneficiary_count = Column(Integer)
    medicare_total_cost = Column(Float)

    # Geographic
    latitude = Column(Float)
    longitude = Column(Float)
    rucc_code = Column(String(5))
    fips_code = Column(String(10))

    # Dedup / tracking
    dedup_key = Column(String(255), index=True)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_refreshed = Column(DateTime, default=datetime.utcnow)

    # Full-text search
    search_vector = Column(TSVECTOR)

    __table_args__ = (
        Index("ix_pharmacies_search", "search_vector", postgresql_using="gin"),
    )


class PharmacyChange(Base):
    __tablename__ = "pharmacy_changes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    npi = Column(String(10), index=True)
    organization_name = Column(String(500))
    change_type = Column(String(50))  # new, updated, deactivated
    field_changed = Column(String(100))
    old_value = Column(Text)
    new_value = Column(Text)
    detected_at = Column(DateTime, default=datetime.utcnow)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    status = Column(String(50), default="pending")
    records_processed = Column(Integer, default=0)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    changes_detected = Column(Integer, default=0)
    error_log = Column(Text)
