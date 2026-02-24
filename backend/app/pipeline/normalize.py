"""
Record normalization — standardizes addresses, phone numbers, and names.
"""
import re
import hashlib


def normalize_record(record: dict) -> dict:
    """Normalize a pharmacy record."""
    # Normalize organization name
    if record.get("organization_name"):
        record["organization_name"] = _normalize_name(record["organization_name"])

    if record.get("dba_name"):
        record["dba_name"] = _normalize_name(record["dba_name"])

    # Normalize phone
    if record.get("phone"):
        record["phone"] = _normalize_phone(record["phone"])
    if record.get("fax"):
        record["fax"] = _normalize_phone(record["fax"])

    # Normalize address
    if record.get("address_line1"):
        record["address_line1"] = _normalize_address(record["address_line1"])

    # Normalize ZIP to 5 digits
    if record.get("zip"):
        record["zip"] = record["zip"][:5]

    # Normalize state to uppercase
    if record.get("state"):
        record["state"] = record["state"].upper().strip()

    # Generate dedup key
    record["dedup_key"] = generate_dedup_key(record)

    return record


def _normalize_name(name: str) -> str:
    """Normalize business names."""
    name = name.strip().upper()
    # Common abbreviation standardization
    replacements = {
        " LLC": " LLC",
        " INC": " INC",
        " CORP": " CORP",
        " PHARMACY": " PHARMACY",
        " PHARM ": " PHARMACY ",
        " RX ": " PHARMACY ",
        " DRUG ": " DRUG ",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    # Remove extra whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _normalize_phone(phone: str) -> str:
    """Normalize phone numbers to (XXX) XXX-XXXX format."""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone


def _normalize_address(address: str) -> str:
    """Normalize street addresses."""
    address = address.strip().upper()
    replacements = {
        " STREET": " ST",
        " AVENUE": " AVE",
        " BOULEVARD": " BLVD",
        " DRIVE": " DR",
        " ROAD": " RD",
        " SUITE": " STE",
        " HIGHWAY": " HWY",
    }
    for old, new in replacements.items():
        address = address.replace(old, new)
    return re.sub(r"\s+", " ", address).strip()


def generate_dedup_key(record: dict) -> str:
    """Generate a deduplication key from name + address."""
    parts = [
        (record.get("organization_name") or "").upper().strip(),
        (record.get("address_line1") or "").upper().strip(),
        (record.get("zip") or "").strip()[:5],
    ]
    raw = "|".join(parts)
    return hashlib.md5(raw.encode()).hexdigest()
