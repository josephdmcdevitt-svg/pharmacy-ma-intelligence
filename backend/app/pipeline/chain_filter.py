"""
Chain vs. independent pharmacy classification.
"""
import re

# Known chain pharmacy patterns
CHAIN_PATTERNS = [
    r"\bCVS\b", r"\bWALGREEN", r"\bWALMART\b", r"\bRITE\s*AID\b",
    r"\bKROGER\b", r"\bCOSTCO\b", r"\bSAM'?S\s+CLUB\b", r"\bTARGET\b",
    r"\bPUBLIX\b", r"\bH[\-\s]?E[\-\s]?B\b", r"\bALBERTSON", r"\bSAFEWAY\b",
    r"\bMEIJER\b", r"\bWINN[\-\s]?DIXIE\b", r"\bGIANT\b", r"\bSHOPRITE\b",
    r"\bWEGMAN", r"\bHY[\-\s]?VEE\b", r"\bFRED\s+MEYER\b", r"\bHARRIS\s+TEETER\b",
    r"\bOMNICARE\b", r"\bPHARMERICA\b", r"\bKINDRED\b", r"\bBRIGHTSPRING\b",
    r"\bCARDINAL\s+HEALTH\b", r"\bMCKESSON\b", r"\bAMERISOURCE\b",
    r"\bEXPRESS\s+SCRIPTS\b", r"\bOPTUM\s+RX\b", r"\bCIGNA\b",
    r"\bAMAZON\s+PHARMACY\b", r"\bCAPSULE\b", r"\bALTO\s+PHARMACY\b",
    r"\bGENOA\b", r"\bPHARMHOUSE\b",
]

# Institutional pharmacy indicators
INSTITUTIONAL_PATTERNS = [
    r"\bHOSPITAL\b", r"\bMEDICAL\s+CENTER\b", r"\bNURSING\b",
    r"\bLONG[\-\s]?TERM\s+CARE\b", r"\bLTC\b", r"\bSKILLED\s+NURSING\b",
    r"\bREHAB\b", r"\bASS?ISTED\s+LIVING\b", r"\bINFUSION\b",
    r"\bCORRECTIONAL\b", r"\bPRISON\b", r"\bVETERANS?\b", r"\bVA\s+\b",
]

CHAIN_MAP = {
    "CVS": r"\bCVS\b",
    "WALGREENS": r"\bWALGREEN",
    "WALMART": r"\bWALMART\b",
    "RITE AID": r"\bRITE\s*AID\b",
    "KROGER": r"\bKROGER\b",
    "COSTCO": r"\bCOSTCO\b",
    "SAM'S CLUB": r"\bSAM'?S\s+CLUB\b",
    "TARGET": r"\bTARGET\b",
    "PUBLIX": r"\bPUBLIX\b",
    "H-E-B": r"\bH[\-\s]?E[\-\s]?B\b",
    "ALBERTSONS": r"\bALBERTSON",
    "SAFEWAY": r"\bSAFEWAY\b",
    "MEIJER": r"\bMEIJER\b",
    "WINN-DIXIE": r"\bWINN[\-\s]?DIXIE\b",
    "OMNICARE": r"\bOMNICARE\b",
    "PHARMERICA": r"\bPHARMERICA\b",
    "GENOA": r"\bGENOA\b",
    "EXPRESS SCRIPTS": r"\bEXPRESS\s+SCRIPTS\b",
    "OPTUM RX": r"\bOPTUM\s+RX\b",
    "AMAZON PHARMACY": r"\bAMAZON\s+PHARMACY\b",
}


def classify_pharmacy(record: dict) -> dict:
    """Classify a pharmacy as chain, independent, or institutional."""
    name = (record.get("organization_name") or "").upper()
    dba = (record.get("dba_name") or "").upper()
    combined = f"{name} {dba}"

    # Check for chain
    record["is_chain"] = False
    record["is_independent"] = True
    record["is_institutional"] = False
    record["chain_parent"] = None

    for parent, pattern in CHAIN_MAP.items():
        if re.search(pattern, combined):
            record["is_chain"] = True
            record["is_independent"] = False
            record["chain_parent"] = parent
            break

    if not record["is_chain"]:
        for pattern in CHAIN_PATTERNS:
            if re.search(pattern, combined):
                record["is_chain"] = True
                record["is_independent"] = False
                break

    # Check for institutional
    for pattern in INSTITUTIONAL_PATTERNS:
        if re.search(pattern, combined):
            record["is_institutional"] = True
            break

    return record


def cluster_multi_location(records: list) -> list:
    """Flag operators with 10+ locations as chains."""
    from collections import Counter
    name_counts = Counter(r.get("organization_name", "") for r in records)
    multi = {name for name, count in name_counts.items() if count >= 10}

    for record in records:
        if record.get("organization_name") in multi and record.get("is_independent"):
            record["is_chain"] = True
            record["is_independent"] = False
            record["chain_parent"] = "Multi-Location Operator"

    return records


def extract_ownership_signals(record: dict) -> dict:
    """Extract ownership type from available signals."""
    name = (record.get("organization_name") or "").upper()

    if "LLC" in name:
        record["ownership_type"] = "LLC"
    elif "INC" in name or "INCORPORATED" in name:
        record["ownership_type"] = "Corporation"
    elif "LLP" in name or "PARTNERSHIP" in name:
        record["ownership_type"] = "Partnership"
    elif "PC" in name or "PLLC" in name:
        record["ownership_type"] = "Professional Corporation"
    else:
        record["ownership_type"] = "Unknown"

    return record
