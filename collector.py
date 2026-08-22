import json
import logging
import time
from datetime import datetime, timedelta, timezone

import requests

from config import GRAPHQL_ENDPOINT, get_api_token, get_zone_id

log = logging.getLogger(__name__)


class CloudflareCollector:
    """Pulls traffic data from Cloudflare's GraphQL Analytics API (not
    Logpush, not the legacy REST Analytics API).

    Data-source scope (investigated 2026-07-04, see README for detail):
    `collect_hourly_traffic` (httpRequests1hGroups) reports on ALL
    HTTP traffic that reached the Cloudflare edge for this zone, including
    requests blocked or challenged by the WAF, bot management, IP/ASN
    blocks, and firewall rules — not just traffic that passed through to
    the origin. The `threats` field is the count of requests within that
    total that Cloudflare's security products actually mitigated (blocked
    or challenged); Log/Skip/Allow actions are not counted as threats.
    `collect_firewall_events` (firewallEventsAdaptive) supplies the
    action-level detail (block, challenge, js_challenge, allow, log) for
    that same edge traffic. In short: request counts and threat ratios
    computed downstream represent the FULL attempted-traffic picture, not a
    pre-filtered "already allowed through" subset.
    """

    def __init__(self, require_zone: bool = True):
        self.token = get_api_token()
        self.zone_id = get_zone_id() if require_zone else None
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _query(self, graphql: str, variables: dict | None = None, _retries: int = 2) -> dict:
        """POST a GraphQL query, with diagnostics and limited retry.

        401/403 are NOT retried — they're auth/permission failures that won't
        resolve by trying again from the same process, and a tight retry loop
        just burns time before falling back to cached data. Anything else
        (timeouts, connection resets, 429, 5xx) gets a couple of short
        backoff retries since those are plausibly transient.

        On any HTTP error, the response body is logged in full: Cloudflare's
        error payload (code + message) is far more actionable than the bare
        "401 Unauthorized" `requests` gives you by default — it's the
        difference between "token revoked", "token doesn't have this scope",
        and "request blocked by IP allowlist on the token", which otherwise
        all look identical from the outside.
        """
        payload = {"query": graphql}
        if variables:
            payload["variables"] = variables

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.post(
                    GRAPHQL_ENDPOINT, json=payload, headers=self.headers, timeout=30
                )
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt <= _retries:
                    wait = 2 ** attempt
                    log.warning("Request error (%s), retrying in %ds (attempt %d/%d)...", e, wait, attempt, _retries)
                    time.sleep(wait)
                    continue
                raise

            if not resp.ok:
                try:
                    body = json.dumps(resp.json(), indent=2)
                except ValueError:
                    body = resp.text[:500]
                log.error("Cloudflare GraphQL request failed: HTTP %d\n%s", resp.status_code, body)

                if resp.status_code in (401, 403):
                    # Not retryable from here. Most common causes: token
                    # revoked/expired, missing "Zone > Analytics/Firewall
                    # Services > Read" scope, or the token has a Client IP
                    # Address Filtering allowlist that doesn't include this
                    # runner's (dynamic) egress IP.
                    resp.raise_for_status()
                if attempt <= _retries:
                    wait = 2 ** attempt
                    log.warning("Retrying in %ds (attempt %d/%d)...", wait, attempt, _retries)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

            data = resp.json()
            if data.get("errors"):
                log.error("GraphQL errors: %s", json.dumps(data["errors"], indent=2))
            return data

    def collect_hourly_traffic(
        self, hours_back: int = 25
    ) -> list[dict]:
        """Pull aggregated HTTP request data grouped by hour, country, method, status."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=hours_back)

        query = """
        query HourlyTraffic($zoneTag: string!, $start: Time!, $end: Time!) {
          viewer {
            zones(filter: { zoneTag: $zoneTag }) {
              httpRequests1hGroups(
                filter: { datetime_gt: $start, datetime_lt: $end }
                limit: 10000
                orderBy: [datetime_ASC]
              ) {
                dimensions {
                  datetime
                }
                sum {
                  requests
                  bytes
                  threats
                  countryMap {
                    clientCountryName
                    requests
                    threats
                    bytes
                  }
                  responseStatusMap {
                    edgeResponseStatus
                    requests
                  }
                }
                uniq {
                  uniques
                }
              }
            }
          }
        }
        """
        variables = {
            "zoneTag": self.zone_id,
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        data = self._query(query, variables)
        collected_at = now.isoformat()
        rows = []

        try:
            groups = data["data"]["viewer"]["zones"][0]["httpRequests1hGroups"]
        except (KeyError, IndexError, TypeError):
            log.warning("No hourly traffic data returned")
            return rows

        for group in groups:
            bucket = group["dimensions"]["datetime"]
            total_requests = group["sum"]["requests"]
            total_bytes = group["sum"]["bytes"]
            total_threats = group["sum"]["threats"]
            unique_visitors = group["uniq"]["uniques"]

            for cm in group["sum"].get("countryMap", []):
                rows.append({
                    "collected_at": collected_at,
                    "bucket": bucket,
                    "country": cm["clientCountryName"],
                    "http_method": "",
                    "status_code": 0,
                    "content_type": "",
                    "request_count": cm["requests"],
                    "bytes_total": cm["bytes"],
                    "threats": cm["threats"],
                    "unique_visitors": 0,
                })

            if not group["sum"].get("countryMap"):
                rows.append({
                    "collected_at": collected_at,
                    "bucket": bucket,
                    "country": "Unknown",
                    "http_method": "",
                    "status_code": 0,
                    "content_type": "",
                    "request_count": total_requests,
                    "bytes_total": total_bytes,
                    "threats": total_threats,
                    "unique_visitors": unique_visitors,
                })

        log.info("Collected %d hourly traffic rows", len(rows))
        return rows

    def collect_firewall_events(self, hours_back: int = 24) -> list[dict]:
        """Pull recent firewall/security events (sampled on free plan, 24h max)."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=min(hours_back, 24))

        query = """
        query FirewallEvents($zoneTag: string!, $start: Time!, $end: Time!) {
          viewer {
            zones(filter: { zoneTag: $zoneTag }) {
              firewallEventsAdaptive(
                filter: { datetime_gt: $start, datetime_lt: $end }
                limit: 10000
                orderBy: [datetime_ASC]
              ) {
                action
                clientIP
                clientCountryName
                clientRequestHTTPHost
                clientRequestHTTPMethodName
                clientRequestPath
                datetime
                userAgent
                ruleId
                source
                rayName
              }
            }
          }
        }
        """
        variables = {
            "zoneTag": self.zone_id,
            "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        data = self._query(query, variables)
        collected_at = now.isoformat()
        rows = []

        try:
            events = data["data"]["viewer"]["zones"][0]["firewallEventsAdaptive"]
        except (KeyError, IndexError, TypeError):
            log.warning("No firewall events returned")
            return rows

        for evt in events:
            rows.append({
                "collected_at": collected_at,
                "event_datetime": evt.get("datetime", ""),
                "action": evt.get("action", ""),
                "client_ip": evt.get("clientIP", ""),
                "country": evt.get("clientCountryName", ""),
                "host": evt.get("clientRequestHTTPHost", ""),
                "http_method": evt.get("clientRequestHTTPMethodName", ""),
                "request_path": evt.get("clientRequestPath", ""),
                "user_agent": evt.get("userAgent", ""),
                "rule_id": evt.get("ruleId", ""),
                "source": evt.get("source", ""),
                "ray_name": evt.get("rayName", ""),
            })

        log.info("Collected %d firewall events", len(rows))
        return rows

    def verify_token(self) -> bool:
        """Quick check that the API token works by making a minimal GraphQL query."""
        try:
            query = """query { viewer { zones(filter: { zoneTag: "%s" }) { zoneTag } } }"""
            if self.zone_id:
                query = query % self.zone_id
            else:
                query = "query { viewer { user { email } } }"
            resp = requests.post(
                GRAPHQL_ENDPOINT,
                json={"query": query},
                headers=self.headers,
                timeout=10,
            )
            data = resp.json()
            if data.get("errors"):
                log.error("Token verification failed: %s", data["errors"])
                return False
            log.info("API token verified successfully")
            return True
        except Exception as e:
            log.error("Token verification error: %s", e)
            return False
