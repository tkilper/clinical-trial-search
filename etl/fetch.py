import logging
import time
from collections.abc import Iterator

import requests

log = logging.getLogger(__name__)

_BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
_PAGE_SIZE = 1000


def fetch_trials(max_trials: int = 10_000) -> Iterator[dict]:
    """Yield raw study dicts from ClinicalTrials.gov v2 API up to max_trials."""
    fetched = 0
    page_token: str | None = None

    while fetched < max_trials:
        params: dict = {
            "format": "json",
            "pageSize": min(_PAGE_SIZE, max_trials - fetched),
        }
        if page_token:
            params["pageToken"] = page_token

        data = _get_with_retry(params)
        studies: list[dict] = data.get("studies", [])

        if not studies:
            break

        yield from studies
        fetched += len(studies)
        log.info("Fetched %d / %d trials", fetched, max_trials)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        time.sleep(0.3)  # polite rate limit


def _get_with_retry(params: dict, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            resp = requests.get(_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            log.warning("Request failed (attempt %d): %s — retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")
