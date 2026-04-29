from typing import Optional


def extract_trial(study: dict) -> dict:
    """Return a flat dict for the trials table from a raw ClinicalTrials.gov v2 study."""
    ps = study.get("protocolSection", {})

    id_mod     = ps.get("identificationModule", {})
    status_mod = ps.get("statusModule", {})
    desc_mod   = ps.get("descriptionModule", {})
    design_mod = ps.get("designModule", {})
    sponsor_mod = ps.get("sponsorCollaboratorsModule", {})

    phases = design_mod.get("phases", [])
    start  = status_mod.get("startDateStruct", {}).get("date")

    return {
        "nct_id":         id_mod.get("nctId"),
        "title":          id_mod.get("briefTitle"),
        "brief_summary":  desc_mod.get("briefSummary"),
        "overall_status": status_mod.get("overallStatus"),
        "phase":          phases[0] if phases else None,
        "start_date":     start,
        "sponsor":        sponsor_mod.get("leadSponsor", {}).get("name"),
    }


def extract_conditions(study: dict) -> list[dict]:
    ps    = study.get("protocolSection", {})
    conds = ps.get("conditionsModule", {}).get("conditions", [])
    return [{"condition_name": c} for c in conds]


def extract_interventions(study: dict) -> list[dict]:
    ps  = study.get("protocolSection", {})
    ivs = ps.get("armsInterventionsModule", {}).get("interventions", [])
    return [
        {
            "name":        iv.get("name"),
            "type":        iv.get("type"),
            "description": iv.get("description"),
        }
        for iv in ivs
    ]


def extract_eligibility(study: dict) -> Optional[dict]:
    ps       = study.get("protocolSection", {})
    elig_mod = ps.get("eligibilityModule", {})
    if not elig_mod:
        return None
    healthy = elig_mod.get("healthyVolunteers")
    return {
        "criteria_text":     elig_mod.get("eligibilityCriteria"),
        "min_age":           elig_mod.get("minimumAge"),
        "max_age":           elig_mod.get("maximumAge"),
        "gender":            elig_mod.get("sex"),
        "healthy_volunteers": healthy if isinstance(healthy, bool) else None,
    }
