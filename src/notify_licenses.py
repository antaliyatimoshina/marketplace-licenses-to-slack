#!/usr/bin/env python3
import os
import sys
import json
import datetime as dt
from dateutil import tz
import requests

# Required env vars (set as GitHub Secrets in Actions):
# MP_USER           -> your Atlassian account email
# MP_API_TOKEN      -> your Atlassian API token
# VENDOR_ID         -> numeric vendor id
# SLACK_WEBHOOK     -> Slack Incoming Webhook URL
#
# Optional:
# APPS              -> comma-separated app names to include (defaults to all)

def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and not v:
        print(f"Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v

MP_USER       = env("MP_USER", required=True)
MP_API_TOKEN  = env("MP_API_TOKEN", required=True)
VENDOR_ID     = env("VENDOR_ID", required=True)
SLACK_WEBHOOK = env("SLACK_WEBHOOK", required=True)
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# How far back (in days) the "wide fetch" looks when inferring trial→paid
# conversions. Must comfortably exceed Atlassian's max trial length, since
# the commercial license inherits the trial's maintenanceStartDate. 90 is a
# safe default; can still be overridden via env.
CONVERSION_LOOKBACK_DAYS = int(os.getenv("CONVERSION_LOOKBACK_DAYS", "90"))

# Atlassian's pricing model gives 1-10 users for free, so renewals at that
# tier don't generate revenue and aren't worth posting. Only renewals at or
# above this user count are kept in the License renewals section. Override
# via env if Atlassian changes the free-tier limit.
PAID_RENEWAL_MIN_USERS = int(os.getenv("PAID_RENEWAL_MIN_USERS", "11"))

def _iso10(s):
    return (s or "")[:10] if isinstance(s, str) else None

def infer_conversions_from_licenses(lic_items, target: dt.date):
    """
    Heuristic conversion finder for day=target:
      - license is COMMERCIAL/PAID (not evaluation)
      - had a trial (latestEvaluationStartDate present)
      - license row was updated on the target date (lastUpdated == target)
    Returns a filtered subset of lic_items (raw dicts).
    """
    want = []
    tgt = target.isoformat()
    for lic in lic_items or []:
        lt = (lic.get("licenseType") or lic.get("tier") or "").upper()
        if lt not in ("COMMERCIAL", "PAID"):
            continue
        if not _iso10(lic.get("latestEvaluationStartDate")):
            continue
        if _iso10(lic.get("lastUpdated")) != tgt:
            continue
        want.append(lic)
    return want

def build_entitlement_enrichment(*license_lists):
    """
    From licenses payloads, build a dict:
      { entitlementNumber -> {
          "customer": ...,
          "contactName": ...,
          "contactEmail": ...,
          "maintenanceEndDate": "YYYY-MM-DD" | None,
          "users": int | None,
        }
      }
    Uses contactDetails (technical/billing) and company when available.
    maintenanceEndDate is the date the license stops being valid — used to
    annotate uninstall/unsubscribe rows with "ends … (N days left)".
    users is parsed from the license `tier` string (e.g., "11 Users" → 11)
    and used as a fallback for rows whose source feed doesn't carry the
    user count (the feedback feed typically doesn't).
    """
    import re

    out = {}
    for lst in license_lists:
        for lic in (lst or []):
            ent = lic.get("appEntitlementNumber") or lic.get("hostEntitlementNumber")
            if not ent:
                continue
            cd = lic.get("contactDetails") or {}
            comp = cd.get("company") or lic.get("customer") or lic.get("cloudSiteHostname") or "—"

            # prefer technical contact, then billing
            t = cd.get("technicalContact") or {}
            b = cd.get("billingContact") or {}
            name  = t.get("name")  or b.get("name")
            email = t.get("email") or b.get("email")

            # Pull the license end date if available; field name varies.
            maint_end = (
                lic.get("maintenanceEndDate")
                or lic.get("latestMaintenanceEndDate")
            )
            if isinstance(maint_end, str):
                maint_end = maint_end[:10]

            # Extract user count: parse from the tier string first, then
            # fall back to direct numeric fields if present.
            users = None
            tier = lic.get("tier")
            if isinstance(tier, str):
                m = re.search(r"(\d+)\s*Users?", tier, re.I)
                if m:
                    users = int(m.group(1))
            if users is None:
                for k in ("users", "seats", "quantity"):
                    v = lic.get(k)
                    if isinstance(v, int):
                        users = v
                        break
                    if isinstance(v, str) and v.isdigit():
                        users = int(v)
                        break

            out[ent] = {
                "customer": comp,
                "contactName": name,
                "contactEmail": email,
                "maintenanceEndDate": maint_end,
                "users": users,
            }
    return out

def build_app_name_map(*payload_lists):
    """
    Build {addonKey -> addonName} from any Marketplace payload lists
    (licenses, feedback/uninstalls, etc.).
    """
    m = {}
    for plist in payload_lists:
        for it in (plist or []):
            app = it.get("app") or {}
            key = it.get("addonKey") or app.get("key")
            name = it.get("addonName") or app.get("name")
            if key and name:
                m[key] = name
    return m

def slack_post(payload: dict):
    """Post to Slack unless DRY_RUN=1, in which case just log."""
    if DRY_RUN:
        print("[DRY_RUN] Would post to Slack:\n" + payload.get("text","")[:2000])
        return
    r = requests.post(SLACK_WEBHOOK, json=payload, timeout=30)
    r.raise_for_status()


APPS_FILTER   = set([a.strip() for a in os.getenv("APPS","").split(",") if a.strip()])

# Date window (UTC)
today_utc = dt.datetime.utcnow().date()

def fetch_transactions(vendor_id: str, start: dt.date, end: dt.date):
    """
    Transactions async export:
      1) POST initiate
      2) poll status
      3) download JSON
    Returns a list of transaction dicts (or []).
    """
    import time, urllib.parse

    base = "https://marketplace.atlassian.com"
    # Try v2 then v4 (tenants differ)
    init_urls = [
        f"{base}/rest/2/vendors/{vendor_id}/reporting/transactions/async/export",
        f"{base}/rest/4/vendors/{vendor_id}/reporting/transactions/async/export",
    ]
    qparams = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "accept": "json",
        # UI often adds this; harmless if ignored:
        "include": "zeroTransactions",
    }
    headers = {"Accept": "application/json"}

    status_url = None
    last_err = None

    # 1) Initiate
    for init in init_urls:
        try:
            r = requests.post(init, params=qparams, headers=headers,
                              auth=(MP_USER, MP_API_TOKEN), timeout=60)
            if r.status_code == 404:
                last_err = f"404 on {r.url}"
                continue
            r.raise_for_status()
            data = r.json() if r.content else {}
            export_id = (
                data.get("exportId")
                or data.get("id")
                or (data.get("links") or {}).get("self", "").split("/")[-1]
            )
            status_url = (
                data.get("statusUrl")
                or (data.get("links") or {}).get("status")
                or (f"{init}/{urllib.parse.quote(str(export_id))}/status" if export_id else None)
            )
            if status_url:
                break
            last_err = f"unexpected initiate response on {init}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e} on {init}"
            continue

    if not status_url:
        print(f"[WARN] transactions initiate failed: {last_err}")
        return []

    # 2) Poll status (up to ~60s)
    deadline = time.time() + 60
    download_url = None
    while time.time() < deadline:
        rs = requests.get(status_url, headers=headers, auth=(MP_USER, MP_API_TOKEN), timeout=60)
        if rs.status_code == 404:
            time.sleep(2)
            continue
        rs.raise_for_status()
        sdata = rs.json() if rs.content else {}
        state = (sdata.get("state") or sdata.get("status") or "").lower()
        download_url = sdata.get("downloadUrl") or sdata.get("resultUrl")
        if state in ("completed", "complete", "done") and download_url:
            break
        if state in ("failed", "error"):
            print(f"[WARN] transactions export failed: {sdata}")
            return []
        time.sleep(2)

    if not download_url:
        print("[WARN] transactions export timed out without downloadUrl")
        return []

    # 3) Download JSON
    rd = requests.get(download_url, headers=headers, auth=(MP_USER, MP_API_TOKEN), timeout=120)
    rd.raise_for_status()
    try:
        payload = rd.json()
    except Exception:
        print("[WARN] transactions export is not JSON; first 200 chars:")
        print(rd.text[:200])
        return []

    # Normalize list
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("transactions"), list):
        return payload["transactions"]
    return []

def debug_dump_transactions(items, prefix="[TX]"):
    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    print(f"{prefix} total: {len(items)}")
    for i, t in enumerate(items[:50], 1):
        when = first(t.get("transactionDate"), t.get("date"), t.get("created"))
        if isinstance(when, str): when = when[:19]
        ent  = first(t.get("appEntitlementNumber"), t.get("entitlementNumber"))
        typ  = (first(t.get("transactionType"), t.get("eventType"), t.get("type")) or "").upper()
        lic  = (first(t.get("licenseType"), t.get("license")) or "").title()
        app  = first(t.get("addonName"), (t.get("app") or {}).get("name"), "Unknown app")
        cust = first((t.get("contactDetails") or {}).get("company"), t.get("customer"), t.get("accountName"), "—")
        users= first(t.get("users"), t.get("quantity"), t.get("seats"))
        amt  = first(t.get("amount"), t.get("price")); cur = first(t.get("currency"), t.get("currencyCode"))
        amt_s = f" · {amt} {cur}" if amt and cur else ""
        users_s = f" · {users} users" if users else ""
        print(f"{prefix} {i:02d} • {when} • {app} • {typ}/{lic}{users_s} • {cust} • {ent}{amt_s}")


def fetch_cloud_conversions(vendor_id: str, start: dt.date, end: dt.date):
    """
    Transactions for a date window (UTC).
    Tries a comprehensive set of endpoint shapes (paths under /reporting/ and
    /reporting/sales/, sync GET on /export and bare resource, plus GET on
    /async/export which on some tenants returns synchronously). Logs the
    outcome of every attempt so a failure is debuggable without re-running.
    """
    base = "https://marketplace.atlassian.com"
    # Endpoint paths to try, in priority order.
    # The /sales/transactions/ variants exist because Atlassian's Reports UI
    # groups transactions under "Sales" — some tenants only respond on that
    # path, others only on the bare /transactions path.
    paths = [
        # sync export endpoints (most likely to work)
        "/reporting/transactions/export",
        "/reporting/sales/transactions/export",
        # bare resource endpoints
        "/reporting/transactions",
        "/reporting/sales/transactions",
        # async/export accessed via GET (POST returns 405 on this tenant,
        # which strongly hints GET is the intended method on some flavours)
        "/reporting/transactions/async/export",
        "/reporting/sales/transactions/async/export",
    ]
    # Try v2 first (matches what licenses uses on this tenant), then v4.
    versions = ["2", "4"]
    endpoints = [
        f"{base}/rest/{v}/vendors/{vendor_id}{p}"
        for v in versions
        for p in paths
    ]
    # try with and without the include=zeroTransactions switch the UI uses
    param_variants = [
        {"startDate": start.isoformat(), "endDate": end.isoformat(), "accept": "json", "include": "zeroTransactions"},
        {"startDate": start.isoformat(), "endDate": end.isoformat(), "accept": "json"},
    ]
    headers = {"Accept": "application/json"}
    attempts = []  # list of (status, url, note) — printed on failure

    for url in endpoints:
        for params in param_variants:
            try:
                r = requests.get(url, params=params, headers=headers,
                                 auth=(MP_USER, MP_API_TOKEN), timeout=60)
                status = r.status_code
                # Hard-skip statuses
                if status in (404, 405):
                    attempts.append((status, r.url, "skipped"))
                    continue
                if status == 204 or not r.content:
                    attempts.append((status, r.url, "no content"))
                    continue
                if status >= 400:
                    body = (r.text or "")[:120].replace("\n", " ")
                    attempts.append((status, r.url, f"error body={body!r}"))
                    continue

                # Some endpoints return CSV when accept=json is ignored. Detect.
                ct = (r.headers.get("Content-Type") or "").lower()
                if "json" not in ct and not r.text.lstrip().startswith(("[", "{")):
                    attempts.append((status, r.url, f"non-JSON content-type={ct}"))
                    continue

                data = r.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for k in ("transactions", "items", "data", "results", "values"):
                        items = data.get(k)
                        if isinstance(items, list):
                            return items
                    attempts.append((status, r.url, f"unexpected JSON keys={list(data.keys())}"))
                    continue
                attempts.append((status, r.url, f"unexpected type={type(data).__name__}"))
            except Exception as e:
                attempts.append(("EXC", url, f"{type(e).__name__}: {e}"))
                continue

    print("[WARN] fetch_transactions failed on all attempts. Attempt log:")
    # Deduplicate identical (status, path) entries so the log isn't 12 lines of "404"
    seen = set()
    for status, url, note in attempts:
        # strip query string for compactness
        path_only = url.split("?", 1)[0]
        key = (status, path_only)
        if key in seen:
            continue
        seen.add(key)
        print(f"  {status:>5}  {path_only}  {note}")
    return []

def debug_dump_conversions(items, prefix="[CONV]"):
    """
    Print compact lines for cloud conversions so you can see what the API returns.
    """
    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    print(f"{prefix} total: {len(items)}")
    for i, c in enumerate(items[:100], 1):
        when = first(c.get("conversionDate"), c.get("date"))
        if isinstance(when, str):
            when = when[:19]
        ent  = first(c.get("appEntitlementNumber"), c.get("entitlementNumber"))
        app  = first(c.get("addonName"), (c.get("app") or {}).get("name"), "Unknown app")
        key  = first(c.get("addonKey"), (c.get("app") or {}).get("key"))
        cust = first((c.get("contactDetails") or {}).get("company"),
                     c.get("customer"), c.get("accountName"),
                     c.get("cloudSiteHostname"), "—")
        users = first(c.get("users"), c.get("seats"), c.get("quantity"))
        users_s = f" · {users} users" if users else ""
        print(f"{prefix} {i:02d} • {when} • {app} • {cust} • {ent}{users_s} • key={key}")


def _extract_license_id(lic: dict):
    """Prefer the visible E-… entitlement; fall back to other ids/composite."""
    def _first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None
    return _first(
        lic.get("appEntitlementNumber"),
        lic.get("hostEntitlementNumber"),
        lic.get("appEntitlementId"),
        lic.get("hostEntitlementId"),
        (f"{lic.get('addonKey')}::{lic.get('cloudId')}"
         if lic.get("addonKey") and lic.get("cloudId") else None),
    )

def _parse_date(s: str | None):
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None

def day_window_utc():
    """
    Returns (start_date, end_date) as the same YYYY-MM-DD date in UTC.
    If env DAY=YYYY-MM-DD is set, uses that date; else defaults to yesterday (UTC).
    """
    d = os.getenv("DAY")
    if d:
        s = e = dt.date.fromisoformat(d)
    else:
        e = dt.datetime.utcnow().date() - dt.timedelta(days=1)
        s = e
    return s, e

# Pick the reporting day (yesterday by default, or DAY=YYYY-MM-DD for backfill)
start_date, end_date = day_window_utc()

def fetch_licenses(vendor_id: str, start: dt.date, end: dt.date, dateType: str = "start"):
    """
    Fetch licenses via the EXPORT endpoint (JSON) for a UTC date window.
    Robust to payload being either a list or an object wrapper.

    `dateType` controls which date field the API filters on:
      - "start"        — license maintenance/evaluation start date (default;
                         what conversion detection needs)
      - "lastUpdated"  — license record's lastUpdated timestamp (catches
                         recently-touched licenses regardless of when they
                         originally started — used for enriching feedback rows)
    """
    base = "https://marketplace.atlassian.com"
    url = f"{base}/rest/2/vendors/{vendor_id}/reporting/licenses/export"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dateType": dateType,
        "accept": "json",           # export API returns JSON when accept=json
        "withDataInsights": "true", # include evaluation/customer fields
    }
    auth = (MP_USER, MP_API_TOKEN)
    headers = {"Accept": "application/json"}

    r = requests.get(url, params=params, auth=auth, headers=headers, timeout=120)
    r.raise_for_status()
    payload = r.json()

    def extract_items(p):
        # If the API returns a bare array
        if isinstance(p, list):
            return p
        # If it returns an object wrapper
        if isinstance(p, dict):
            for key in ("licenses", "items", "data", "results", "values"):
                v = p.get(key)
                if isinstance(v, list):
                    return v
            # nested containers some responses use
            for key in ("content", "page", "paging", "_embedded"):
                v = p.get(key)
                if isinstance(v, dict):
                    for k2 in ("licenses", "items", "data", "results", "values"):
                        v2 = v.get(k2)
                        if isinstance(v2, list):
                            return v2
            # single-record fallback
            if any(k in p for k in ("licenseId", "appName", "customer", "evaluationStartDate")):
                return [p]
        return []

    return extract_items(payload)

def pick_new_evaluations(items, date_from: dt.date, date_to: dt.date):
    """
    Map ANY new licenses (trial/paid) to rows for Slack and include:
      - app      : pretty name
      - appKey   : canonical key for grouping with uninstall rows
      - customer : company/site/email-domain fallback
      - contactName/contactEmail
      - licenseType : e.g., EVALUATION/COMMERCIAL (uppercased)
      - users    : parsed from 'tier' when present
      - licenseId: visible entitlement number if available
    """
    import re

    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    def domain(email):
        return email.split("@", 1)[1] if isinstance(email, str) and "@" in email else None

    rows = []
    for lic in (items or []):
        # Names/keys
        app_name = first(
            lic.get("addonName"),
            (lic.get("app") or {}).get("name"),
            lic.get("appName"),
            "Unknown app",
        )
        # compute the key inline (no temporary variable)
        app_key_expr = first(
            lic.get("addonKey"),
            (lic.get("app") or {}).get("key"),
            app_name,  # last-resort fallback to keep grouping stable
        )

        # Contact/customer
        cd   = lic.get("contactDetails") or {}
        tech = cd.get("technicalContact") or {}
        bill = cd.get("billingContact") or {}
        site = lic.get("cloudSiteHostname")

        customer = first(
            cd.get("company"),
            site,
            domain(tech.get("email")),
            domain(bill.get("email")),
            tech.get("name"),
            bill.get("name"),
            "Unknown customer",
        )
        contact_name  = first(tech.get("name"),  bill.get("name"))
        contact_email = first(tech.get("email"), bill.get("email"))

        # Type & users
        license_id = _extract_license_id(lic)
        license_type = (lic.get("licenseType") or lic.get("tier") or "LICENSE").upper()

        # Evaluation insights: potential number of users for trials
        trial_user_count = None
        raw_eval_size = lic.get("evaluationOpportunitySize")
        if isinstance(raw_eval_size, str):
            if raw_eval_size.isdigit():
                trial_user_count = int(raw_eval_size)
        elif isinstance(raw_eval_size, (int, float)):
            try:
                trial_user_count = int(raw_eval_size)
            except (TypeError, ValueError):
                trial_user_count = None

        users = None
        if isinstance(lic.get("tier"), str):
            m = re.search(r"(\d+)\s*Users?", lic["tier"], re.I)
            if m:
                users = int(m.group(1))


        # Best visible ID
        license_id = first(
            lic.get("appEntitlementNumber"),
            lic.get("hostEntitlementNumber"),
            lic.get("appEntitlementId"),
            lic.get("hostEntitlementId"),
            (f"{lic.get('addonKey')}::{lic.get('cloudId')}"
             if lic.get("addonKey") and lic.get("cloudId") else None),
        )

        start_dt = _parse_date(
            lic.get("maintenanceStartDate")
            or lic.get("latestMaintenanceStartDate")
            or lic.get("evaluationStartDate")
        )
        trial_dt = _parse_date(lic.get("latestEvaluationStartDate"))
        
        is_paid = license_type not in ("EVALUATION", "EVAL", "TRIAL")
        is_conversion = (
            is_paid and trial_dt and start_dt and
            (start_dt - trial_dt).days >= 0 and
            (start_dt - trial_dt).days <= CONVERSION_LOOKBACK_DAYS
        )

        rows.append({
            "app": app_name,
            "appKey": app_key_expr,
            "customer": customer,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
            "licenseId": license_id,
            "isConversion": bool(is_conversion),
            "trialStarted": trial_dt.isoformat() if trial_dt else None,
            "trial_user_count": trial_user_count if license_type in ("EVALUATION", "EVAL", "TRIAL") else None,

        })

    return rows

def fetch_uninstalls(vendor_id: str, start: dt.date, end: dt.date):
    """
    Fetch churn feedback (uninstall/unsubscribe/disable) for a UTC date window.
    Uses Feedback Details EXPORT with accept=json for richer fields.
    """
    base = "https://marketplace.atlassian.com"
    url = f"{base}/rest/2/vendors/{vendor_id}/reporting/feedback/details/export"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "accept": "json",
        # churn actions to include:
        "type": ["uninstall", "unsubscribe", "disable"],
    }
    auth = (MP_USER, MP_API_TOKEN)
    headers = {"Accept": "application/json"}
    r = requests.get(url, params=params, auth=auth, headers=headers, timeout=120)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("feedback", []) or data.get("items", []) or []
    return []

def pick_renewals(tx_items, day: dt.date, name_map=None, ent_map=None):
    """
    From a transactions payload, pick rows where saleType == 'Renewal'
    on the given UTC day. Returns rows with the same shape as license rows
    so they can flow through the existing Slack rendering.

    Atlassian Marketplace transactions typically expose:
      - purchaseDetails.saleType ('New' | 'Renewal' | 'Upgrade' | 'Refund')
      - purchaseDetails.licenseType, tier, maintenanceStartDate
      - customerDetails.company / technicalContact / billingContact
      - appEntitlementNumber, addonKey, addonName, transactionDate/saleDate
    """
    import re

    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    target = day.isoformat()
    rows = []
    for tx in (tx_items or []):
        pd = tx.get("purchaseDetails") or {}

        # saleType lives under purchaseDetails on most tenants, but be defensive
        sale = (
            pd.get("saleType")
            or tx.get("saleType")
            or tx.get("transactionType")
            or ""
        ).strip().lower()
        if sale != "renewal":
            continue

        # Filter to the target date — try a few common date fields
        when = first(
            tx.get("saleDate"),
            pd.get("saleDate"),
            tx.get("transactionDate"),
            pd.get("maintenanceStartDate"),
            tx.get("date"),
        )
        if isinstance(when, str):
            when = when[:10]
        if when and when != target:
            continue

        # App identifiers
        app = tx.get("app") or {}
        app_name = first(tx.get("addonName"), app.get("name"), "Unknown app")
        app_key = first(tx.get("addonKey"), app.get("key"), app_name)

        # Customer / contact (transactions sometimes use customerDetails, sometimes contactDetails)
        cd = tx.get("customerDetails") or tx.get("contactDetails") or {}
        tech = cd.get("technicalContact") or {}
        bill = cd.get("billingContact") or {}
        company = first(
            cd.get("company"),
            tx.get("customer"),
            tx.get("accountName"),
            tx.get("cloudSiteHostname"),
            "—",
        )
        contact_name = first(tech.get("name"), bill.get("name"))
        contact_email = first(tech.get("email"), bill.get("email"))

        # Atlassian's transaction payload sometimes fills `company` and the
        # contact `name` with the contact email when no real values are set
        # (rendering as "email · email (email)"). Treat those as missing so
        # the ent_map license enrichment below can supply a real company name
        # and contact name from the matching license record.
        if (isinstance(company, str) and contact_email and
                company.strip().lower() == contact_email.strip().lower()):
            company = "—"
        if (isinstance(contact_name, str) and contact_email and
                contact_name.strip().lower() == contact_email.strip().lower()):
            contact_name = None

        # Entitlement id — fallback to license enrichment map for missing fields
        ent_id = first(
            tx.get("appEntitlementNumber"),
            tx.get("entitlementNumber"),
            pd.get("appEntitlementNumber"),
        )
        if ent_map and ent_id:
            info = ent_map.get(ent_id) or {}
            if company in (None, "", "—", "Unknown"):
                company = info.get("customer") or company
            if not contact_name:
                contact_name = info.get("contactName")
            if not contact_email:
                contact_email = info.get("contactEmail")

        # Users — parse from tier string when available
        users = first(
            tx.get("users"),
            tx.get("quantity"),
            tx.get("seats"),
            pd.get("users"),
        )
        if users is None:
            tier = pd.get("tier") or tx.get("tier") or ""
            if isinstance(tier, str):
                m = re.search(r"(\d+)\s*Users?", tier, re.I)
                if m:
                    users = int(m.group(1))

        # Filter to paid renewals only. Atlassian's free tier covers 1-10
        # users; below the threshold we drop the row. We also drop rows
        # where user count is unknown — without it we can't tell paid from
        # free, and erring on the side of fewer false positives is safer
        # than spamming the channel with free-tier "renewals".
        try:
            users_int = int(users) if users is not None else None
        except (TypeError, ValueError):
            users_int = None
        if users_int is None or users_int < PAID_RENEWAL_MIN_USERS:
            continue

        license_type = (pd.get("licenseType") or tx.get("licenseType") or "COMMERCIAL").upper()

        rows.append({
            "app": app_name or (name_map or {}).get(app_key) or app_key or "Unknown app",
            "appKey": app_key,
            "customer": company,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
            "licenseId": ent_id,
            "isRenewal": True,
        })
    return rows

def pick_uninstalls(items, name_map=None, ent_map=None):
    """
    Map Feedback/Uninstall/Unsubscribe rows to the common row shape,
    enriching missing customer/contact from licenses using entitlement id.
    """
    out = []
    for f in (items or []):
        app = f.get("app") or {}
        app_name = f.get("addonName") or app.get("name")
        app_key  = f.get("addonKey")  or app.get("key")

        # raw fields from feedback payload
        cust   = (f.get("contactDetails") or {}).get("company") or f.get("customer") or "Unknown"
        name   = f.get("contactName")
        email  = f.get("contactEmail")
        ftype  = (f.get("feedbackType") or "").upper()  # UNSUBSCRIBE / UNINSTALL / DISABLE
        ent_id = f.get("appEntitlementNumber") or f.get("entitlementNumber")
        users  = f.get("users") or f.get("seats") or f.get("quantity")

        # License end date — try the feedback payload first, fall back to
        # ent_map. Used to annotate the row with "ends … (N days left)".
        maint_end = (
            f.get("maintenanceEndDate")
            or f.get("latestMaintenanceEndDate")
        )
        if isinstance(maint_end, str):
            maint_end = maint_end[:10]

        # enrichment from licenses by entitlement number
        if ent_map and ent_id:
            info = ent_map.get(ent_id)
            if info:
                if cust in ("Unknown", "—") or not cust:
                    cust = info.get("customer") or cust
                if not name:
                    name = info.get("contactName")
                if not email:
                    email = info.get("contactEmail")
                if not maint_end:
                    maint_end = info.get("maintenanceEndDate")
                if not users:
                    users = info.get("users")

        # human labels (optional)
        ACTION_LABELS = {
            "UNSUBSCRIBE": "UNSUBSCRIBE",
            "UNINSTALL": "UNINSTALL",
            "DISABLE": "DISABLE",
        }
        label = ACTION_LABELS.get(ftype, ftype or "UNSUBSCRIBE")

        out.append({
            "app": app_name or (name_map or {}).get(app_key) or app_key or "Unknown app",
            "appKey": app_key or app_name,
            "customer": cust or "—",
            "contactName": name,
            "contactEmail": email,
            "licenseType": label,
            "users": users,
            "licenseId": ent_id,
            "maintenanceEndDate": maint_end,
        })
    return out

def post_combined_to_slack(webhook, licenses_rows, uninstall_rows, start: dt.date, end: dt.date, renewal_rows=None):
    """
    One message per appKey:
      {Pretty App Name} Marketplace Events (YYYY-MM-DD, UTC)

    💰 Conversions (trial → paid)
    ✈️ New licenses
    🔄 License renewals
    ➖ Uninstalls / Unsubscribes
    """
    # Group by canonical key
    groups = {}
    for r in (licenses_rows or []):
        k = r.get("appKey") or r.get("app") or "unknown"
        g = groups.setdefault(k, {"names": set(), "lic": [], "renew": [], "un": []})
        if r.get("app"):
            g["names"].add(r["app"])
        g["lic"].append(r)

    for r in (renewal_rows or []):
        k = r.get("appKey") or r.get("app") or "unknown"
        g = groups.setdefault(k, {"names": set(), "lic": [], "renew": [], "un": []})
        if r.get("app"):
            g["names"].add(r["app"])
        g["renew"].append(r)

    for r in (uninstall_rows or []):
        k = r.get("appKey") or r.get("app") or "unknown"
        g = groups.setdefault(k, {"names": set(), "lic": [], "renew": [], "un": []})
        if r.get("app"):
            g["names"].add(r["app"])
        g["un"].append(r)

    if not groups:
        slack_post({"text": f"ℹ️ No new licenses, renewals or uninstalls for {start.isoformat()} (UTC)."})
        print("Nothing to post.")
        return

    def prettiest_name(names: set[str]) -> str:
        if not names:
            return "Unknown app"
        # prefer human-looking names (with spaces/colon)
        return sorted(names, key=lambda s: (":" not in s and " " not in s, len(s)))[0]

    date_label = start.isoformat()
    parts: list[str] = []
    
    for k in sorted(groups.keys()):
        g = groups[k]
        app_title = prettiest_name(g["names"])
    
        section_chunks: list[str] = []
    
        # app-scoped rows
        lic_rows = g["lic"]
        renew_rows = g["renew"]
        un_rows  = g["un"]
    
        # 1) split licenses into conversions vs non-conversions
        paid_conversions = [e for e in lic_rows if e.get("isConversion")]
        new_nonconversion = [e for e in lic_rows if not e.get("isConversion")]

        # 2) suppress "false" uninstalls — if an entitlement also appears in
        # licenses or renewals for the same day, the unsubscribe event is
        # almost always API/state churn (e.g. trial entitlement winding down
        # as the new commercial one starts, or convert+cancel same day),
        # not a real reinstall. Only show truly churning entitlements.
        active_ids = {
            e["licenseId"] for e in (lic_rows + renew_rows) if e.get("licenseId")
        }
        un_rows = [e for e in un_rows if e.get("licenseId") not in active_ids]
    
        # Conversions
        if paid_conversions:
            lines = []
            for e in paid_conversions:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}" if e.get("licenseId") else ""
                trial_part = f" (trial started {e['trialStarted']})" if e.get("trialStarted") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}{trial_part}")
            section_chunks.append(":moneybag: Conversions (trial → paid)\n" + "\n".join(lines))
    
        # New licenses (non-conversions)
        if new_nonconversion:
            lines = []
            for e in new_nonconversion:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                trial_users = e.get("trial_user_count")
                if trial_users and e.get("licenseType") in ("EVALUATION", "EVAL", "TRIAL"):
                    # For trials: show "10 users" based on evaluationOpportunitySize
                    users_part = f" · {trial_users} users"
                else:
                    # For paid licenses: keep existing users count from tier
                    users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}" if e.get("licenseId") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}")
            section_chunks.append(":airplane: New licenses\n" + "\n".join(lines))

        # License renewals (sourced from transactions where saleType == "Renewal")
        if renew_rows:
            lines = []
            for e in renew_rows:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}" if e.get("licenseId") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}")
            section_chunks.append(":arrows_counterclockwise: Paid renewals\n" + "\n".join(lines))

        # Uninstalls / Unsubscribes — only true churn (same-day "reinstalls"
        # already filtered out above). Each row is annotated with the license
        # end date and days remaining so the team can prioritise outreach:
        # short windows ("ends 2026-05-07 (7 days left)") are more urgent
        # than long ones ("ends 2026-05-21 (22 days left)").
        if un_rows:
            lines = []
            for e in un_rows:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part = f" · {e['licenseId']}" if e.get("licenseId") else ""

                # Compute "ends YYYY-MM-DD (N days left)" annotation.
                end_part = ""
                end_str = e.get("maintenanceEndDate")
                if end_str:
                    try:
                        end_dt = dt.date.fromisoformat(end_str[:10])
                        delta = (end_dt - start).days
                        if delta > 0:
                            end_part = f" · ends {end_str} ({delta} days left)"
                        elif delta == 0:
                            end_part = f" · ends today ({end_str})"
                        else:
                            end_part = f" · ended {end_str} ({-delta} days ago)"
                    except (ValueError, TypeError):
                        # Fall back to the raw string if parsing fails
                        end_part = f" · ends {end_str}"

                lines.append(
                    f"• {e['customer']} · {contact} · {e['licenseType']}"
                    f"{users_part}{id_part}{end_part}"
                )
            section_chunks.append(":heavy_minus_sign: Uninstalls / Unsubscribes\n" + "\n".join(lines))

        # Skip apps whose only events were filtered out (e.g. "false" uninstalls
        # that turned out to be conversions/renewals on the same day).
        if not section_chunks:
            continue

        parts.append(
            f"{app_title} Marketplace Events ({date_label}, UTC)\n\n"
            + "\n\n".join(section_chunks)
        )

    if not parts:
        slack_post({"text": f"ℹ️ No new licenses, renewals or uninstalls for {start.isoformat()} (UTC)."})
        print("All events filtered as same-day churn; posted 'no changes'.")
        return

    text = "\n\n".join(parts)
    slack_post({"text": text})
    print("Posted combined message (merged by appKey).")

def main():
    start_date, end_date = day_window_utc()
    print(f"[INFO] Daily window (UTC): {start_date}")

    # 2a) Wide fetch for conversion inference (uses lastUpdated on the target date)
    wide_start = start_date - dt.timedelta(days=CONVERSION_LOOKBACK_DAYS)
    lic_items_wide = fetch_licenses(VENDOR_ID, wide_start, end_date)   # dateType=start

    # Note: Atlassian's licenses/export endpoint on this tenant only accepts
    # dateType=start (other values like last_updated, lastUpdated, updated all
    # return 400). That means the wide fetch above is the sole source of
    # license records for the enrichment map, and any license whose start
    # predates `wide_start` won't be enrichable. CONVERSION_LOOKBACK_DAYS=90
    # is wide enough to catch ordinary cases — if an unsubscribe ever lands
    # on a license older than that, the row will degrade to "Unknown · —"
    # rather than failing. If/when that becomes a problem, options are to
    # widen the lookback further or look up specific entitlements via a
    # different endpoint (e.g. /reporting/licenses/details/{entitlementId}).
    ent_map = build_entitlement_enrichment(lic_items_wide)

    inferred_raw = infer_conversions_from_licenses(lic_items_wide, start_date)
    conv_rows = pick_new_evaluations(inferred_raw, start_date, end_date)  # reuse your mapper
    # mark as conversions + carry trial start date if present
    # build a quick index by licenseId so we can annotate trialStarted:
    raw_by_ent = {}
    for lic in inferred_raw:
        ent = lic.get("appEntitlementNumber") or lic.get("hostEntitlementNumber")
        if ent:
            raw_by_ent[ent] = lic
    for r in conv_rows:
        r["isConversion"] = True
        ent = r.get("licenseId")
        trial_dt = _iso10(raw_by_ent.get(ent, {}).get("latestEvaluationStartDate")) if ent else None
        if trial_dt:
            r["trialStarted"] = trial_dt

    # 2b) Normal single-day license rows (new starts etc.)
    lic_items = fetch_licenses(VENDOR_ID, start_date, end_date)
    lic_rows  = pick_new_evaluations(lic_items, start_date, end_date)

    # 2c) Uninstalls (your existing path)
    un_items = fetch_uninstalls(VENDOR_ID, start_date, end_date)
    name_map = build_app_name_map(lic_items, un_items)
    un_rows   = pick_uninstalls(un_items, name_map=name_map, ent_map=ent_map)

    # 2c-bis) Renewals — sourced from transactions, where saleType == "Renewal".
    # fetch_cloud_conversions tries multiple GET endpoint shapes; the path that
    # works on this tenant is /reporting/sales/transactions/export.
    tx_items = fetch_cloud_conversions(VENDOR_ID, start_date, end_date)
    renew_rows = pick_renewals(tx_items, start_date, name_map=name_map, ent_map=ent_map)

    # 2d) Merge conversions + de-dupe by licenseId so they don’t also appear under New licenses
    seen_ids = {r.get("licenseId") for r in conv_rows if r.get("licenseId")}
    lic_rows_filtered = [r for r in lic_rows if r.get("licenseId") not in seen_ids]
    lic_rows_final = conv_rows + lic_rows_filtered

    # If a renewal entitlement also surfaced in the licenses feed (rare but possible
    # on continuous renewals where maintenanceStartDate ticks forward), prefer the
    # renewal section and drop it from "New licenses" to avoid double-counting.
    renew_ids = {r.get("licenseId") for r in renew_rows if r.get("licenseId")}
    if renew_ids:
        lic_rows_final = [r for r in lic_rows_final if r.get("licenseId") not in renew_ids]

    # Cross-feed conflict resolution.
    # An entitlement that appears in BOTH the licenses/conversions feed AND the
    # uninstalls feed for the same UTC day is almost always `lastUpdated` drift:
    # the unsubscribe event re-touched the license record, which then matches
    # the conversion heuristic (COMMERCIAL + has trial + lastUpdated == target)
    # even though the real conversion happened days/weeks earlier.
    #
    # The fix is one-sided: drop the false conversion from the licenses feed,
    # but KEEP the unsubscribe row in the uninstalls feed. The unsubscribe is
    # a real, actionable churn signal — for an active long-running customer,
    # this is their 30-day notice that they've turned off auto-renewal. The
    # only thing that's noise is the duplicated "conversion" appearance.
    un_ids = {r.get("licenseId") for r in un_rows if r.get("licenseId")}
    lic_ids = {r.get("licenseId") for r in lic_rows_final if r.get("licenseId")}
    conflicts = lic_ids & un_ids
    if conflicts:
        lic_rows_final = [r for r in lic_rows_final if r.get("licenseId") not in conflicts]
        # Note: deliberately NOT removing from un_rows. The unsubscribe is real.

    print(
        f"[INFO] Licenses mapped: {len(lic_rows_final)} | "
        f"Conversions inferred: {len(conv_rows)} | "
        f"Renewals: {len(renew_rows)} | "
        f"Uninstalls mapped: {len(un_rows)}"
    )

    if not lic_rows_final and not renew_rows and not un_rows:
        slack_post({"text": f"ℹ️ No new licenses, renewals or uninstalls for {start_date} (UTC)."})
        print("[INFO] No items; posted 'no changes' message to Slack.")
        return

    post_combined_to_slack(SLACK_WEBHOOK, lic_rows_final, un_rows, start_date, end_date, renewal_rows=renew_rows)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc()  # log to GitHub Actions logs
        raise
