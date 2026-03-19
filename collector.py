import json
import logging
from datetime import datetime, timedelta, timezone

import requests

from config import GRAPHQL_ENDPOINT, get_api_token, get_zone_id

log = logging.getLogger(__name__)


class CloudflareCollector:
    def __init__(self, require_zone: bool = True):
        self.token = get_api_token()
        self.zone_id = get_zone_id() if require_zone else None
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _query(self, graphql: str, variables: dict | None = None) -> dict:
        payload = {"query": graphql}
        if variables:
            payload["variables"] = variables
        resp = requests.post(
            GRAPHQL_ENDPOINT, json=payload, headers=self.headers, timeout=30
        )
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
                    "http_method": None,
                    "status_code": None,
                    "content_type": None,
                    "request_count": cm["requests"],
                    "bytes_total": cm["bytes"],
                    "threats": cm["threats"],
                    "unique_visitors": 0,
                })

            if not group["sum"].get("countryMap"):
                rows.append({
                    "collected_at": collected_at,
                    "bucket": bucket,
                    "country": None,
                    "http_method": None,
                    "status_code": None,
                    "content_type": None,
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

    def collect_adaptive_requests(self, hours_back: int = 23) -> list[dict]:
        """Pull detailed request groups for path-level analysis (max 24h on free plan)."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=min(hours_back, 23))

        query = """
        query AdaptiveRequests($zoneTag: string!, $start: Time!, $end: Time!) {
          viewer {
            zones(filter: { zoneTag: $zoneTag }) {
              httpRequestsAdaptiveGroups(
                filter: { datetime_gt: $start, datetime_lt: $end }
                limit: 10000
                orderBy: [count_DESC]
              ) {
                count
                dimensions {
                  clientCountryName
                  clientRequestHTTPMethodName
                  edgeResponseStatus
                  clientRequestPath
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
            groups = data["data"]["viewer"]["zones"][0]["httpRequestsAdaptiveGroups"]
        except (KeyError, IndexError, TypeError):
            log.warning("No adaptive request data returned")
            return rows

        bucket = start.strftime("%Y-%m-%dT%H:00:00Z")
        for g in groups:
            dims = g["dimensions"]
            rows.append({
                "collected_at": collected_at,
                "bucket": bucket,
                "country": dims.get("clientCountryName", ""),
                "http_method": dims.get("clientRequestHTTPMethodName", ""),
                "status_code": dims.get("edgeResponseStatus"),
                "path": dims.get("clientRequestPath", ""),
                "request_count": g["count"],
            })

        log.info("Collected %d adaptive request groups", len(rows))
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
