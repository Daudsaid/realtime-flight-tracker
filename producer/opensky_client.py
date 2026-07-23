import time
import logging
import requests

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"

logger = logging.getLogger("opensky_client")


class OpenSkyClient:
    def __init__(self, client_id=None, client_secret=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._token_expiry = 0
        self.session = requests.Session()

    @property
    def authenticated(self):
        return bool(self.client_id and self.client_secret)

    def _refresh_token(self):
        resp = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expiry = time.time() + payload.get("expires_in", 1800) - 30
        logger.info("Refreshed OpenSky OAuth2 token")

    def _auth_headers(self):
        if not self.authenticated:
            return {}
        if self._token is None or time.time() >= self._token_expiry:
            self._refresh_token()
        return {"Authorization": f"Bearer {self._token}"}
    def get_states(self, bbox=None, max_retries=5):
        params = {}
        if bbox:
            lamin, lomin, lamax, lomax = bbox
            params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}

        backoff = 5
        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(
                    STATES_URL, params=params, headers=self._auth_headers(), timeout=15
                )
                if resp.status_code == 429:
                    logger.warning(
                        "Rate limited (429). Backing off %ss (attempt %s/%s)",
                        backoff, attempt, max_retries,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data.get("states") or []
            except requests.RequestException as exc:
                logger.warning("OpenSky request failed (attempt %s/%s): %s", attempt, max_retries, exc)
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)

        logger.error("Giving up after %s failed attempts", max_retries)
        return []