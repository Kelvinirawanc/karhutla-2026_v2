"""
KARHUTLA INDONESIA 2026 — ADAPTIVE GOVERNMENT SCRAPER

Purpose
-------
Discover current official government pages by topic/keyword instead of relying
on one permanent article URL. The scraper is designed for daily GitHub Actions
runs where article slugs, publication dates and reporting pages may change.

Flow
----
official domains
    -> sitemap / robots / homepage / internal search / search-engine discovery
    -> candidate pages
    -> relevance + recency scoring
    -> source validation
    -> adaptive metric extraction
    -> carry-forward only when a field is not found today
    -> JSON outputs

Important
---------
- Fixed URLs are optional safety references only; they are NOT used as the
  primary source-selection mechanism.
- Values carried from the previous JSON are explicitly labelled as carried
  forward, so a daily refresh does not pretend stale values are fresh.
- The six priority provinces are configurable and can be expanded.
"""
from __future__ import annotations

import argparse
import json
import re
import time
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from bs4 import XMLParsedAsHTMLWarning
except ImportError:
    XMLParsedAsHTMLWarning = Warning


# ---------------------------------------------------------------------------
# Paths / runtime configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.name.lower() in {"scripts", "scraper", "src"} else SCRIPT_DIR

DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_JSON = DATA_DIR / "karhutla_2026.json"
PROVINCE_OUTPUT = DATA_DIR / "province_metrics.json"
REGISTRY_OUTPUT = DATA_DIR / "source_registry.json"

TIMEOUT = 25
CRAWL_DELAY = 0.20
MAX_SEARCH_RESULTS = 8
MAX_SITEMAP_URLS = 350
MAX_CANDIDATES_PER_SOURCE = 80
MAX_PAGES_PER_RUN = 420
MAX_TEXT_CHARS = 12000
RELEVANCE_THRESHOLD = 18

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/154.0 Safari/537.36 KarhutlaDashboard/2.0"
    ),
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# Government source discovery configuration
# ---------------------------------------------------------------------------

DEFAULT_KEYWORDS = [
    "karhutla",
    "kebakaran hutan dan lahan",
    "kebakaran lahan",
    "hotspot",
    "titik panas",
    "firespot",
    "fire spot",
    "luas lahan terbakar",
    "luas terbakar",
    "luas dipadamkan",
    "pemadaman",
    "pendinginan",
    "water bombing",
    "operasi darat",
    "operasi udara",
    "operasi modifikasi cuaca",
    "omc",
    "kabut asap",
]

DEFAULT_QUERIES = [
    "karhutla 2026",
    "karhutla hotspot 2026",
    "kebakaran hutan dan lahan 2026",
    "karhutla situasi terkini",
    "karhutla enam provinsi prioritas",
    "titik panas karhutla",
    "luas lahan terbakar karhutla",
]

SOURCE_CONFIG = [
    {
        "key": "bnpb",
        "name": "BNPB",
        "domains": ["bnpb.go.id", "www.bnpb.go.id", "ppid.bnpb.go.id"],
        "keywords": DEFAULT_KEYWORDS + [
            "enam provinsi prioritas",
            "six priority provinces",
            "personel gabungan",
            "firespot",
            "satgas darat",
            "satgas udara",
        ],
        "queries": DEFAULT_QUERIES + [
            "karhutla bnpb hari ini",
            "perkembangan situasi karhutla",
            "penanganan karhutla provinsi",
        ],
    },
    {
        "key": "bmkg",
        "name": "BMKG",
        "domains": ["bmkg.go.id", "www.bmkg.go.id"],
        "keywords": DEFAULT_KEYWORDS + [
            "prakiraan cuaca",
            "hari tanpa hujan",
            "el niño",
            "risiko karhutla",
            "hotspot dengan tingkat kepercayaan tinggi",
            "operasi modifikasi cuaca",
        ],
        "queries": [
            "karhutla hotspot terbaru",
            "karhutla hotspot indonesia",
            "prakiraan karhutla",
            "operasi modifikasi cuaca karhutla",
            "el nino karhutla",
        ],
    },
    {
        "key": "kemenhut",
        "name": "Kementerian Kehutanan",
        "domains": ["kemenhut.go.id", "www.kemenhut.go.id"],
        "keywords": DEFAULT_KEYWORDS + [
            "manggala agni",
            "pengendalian karhutla",
            "pemadaman karhutla",
            "kawasan hutan",
        ],
        "queries": [
            "karhutla kementerian kehutanan",
            "pengendalian karhutla",
            "manggala agni karhutla",
            "pemadaman karhutla",
        ],
    },
    {
        "key": "gis_bnpb",
        "name": "BNPB GIS",
        "domains": ["gis.bnpb.go.id"],
        "keywords": DEFAULT_KEYWORDS + [
            "karhutla2026",
            "dashboard karhutla",
            "hotspot",
            "fire spot",
            "luas terbakar",
        ],
        "queries": [
            "karhutla dashboard",
            "karhutla 2026 dashboard",
            "hotspot karhutla gis bnpb",
        ],
    },
]

# Fixed references are only safety links for source registry/history.
# They are NOT used to define what is "latest".
SAFETY_REFERENCES = {
    "bnpb": "https://bnpb.go.id/berita/perkembangan-situasi-terkini-penanganan-karhutla-di-6-provinsi-prioritas",
    "bmkg": "https://www.bmkg.go.id/berita/dampak-el-nino-masih-perlu-diwaspadai-bmkg-perkuat-dukungan-pengendalian-karhutla",
}

# Six-province dashboard scope from the user's existing project.
DEFAULT_PRIORITY_PROVINCES = [
    "Riau",
    "Jambi",
    "Sumatera Selatan",
    "Kalimantan Barat",
    "Kalimantan Tengah",
    "Kalimantan Selatan",
]

PROVINCE_COORDS = {
    "Riau": (0.30, 101.70),
    "Jambi": (-1.60, 103.60),
    "Sumatera Selatan": (-3.20, 104.20),
    "Kalimantan Barat": (-0.10, 110.00),
    "Kalimantan Tengah": (-1.70, 113.40),
    "Kalimantan Selatan": (-3.00, 115.40),
}

# Used only for an initial run if there is no prior JSON.
# The output explicitly records these as seed_snapshot, not today's data.
SEED_DEFAULTS = {
    "total_hotspots": None,
    "fire_spots": None,
    "burned_area_24h_ha": None,
    "handled_area_today_ha": None,
    "personnel": None,
    "air_units": None,
    "affected_regencies_cities": None,
    "handled_area_ground_ha": None,
    "handled_area_air_ha": None,
    "uncontrolled_area_ha": None,
    "total_burned_area_situation_ha": None,
}


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def now_local_naive() -> datetime:
    return datetime.now().astimezone().replace(tzinfo=None)


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def canon(url: str) -> str:
    try:
        p = urlparse(url)
        # Drop tracking query strings; keep path.
        return p._replace(fragment="", query="").geturl().rstrip("/")
    except Exception:
        return url


def same_domain(url: str, domains: Sequence[str]) -> bool:
    host = urlparse(url).netloc.lower().split(":")[0]
    return any(host == d.lower() or host.endswith("." + d.lower()) for d in domains)


def looks_like_html_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    blocked = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf", ".zip", ".mp4", ".mp3")
    return not path.endswith(blocked)


def safe_json_load(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def parse_number(raw: Optional[str]) -> Optional[float]:
    """Handle Indonesian and international number conventions."""
    if raw is None:
        return None

    s = str(raw).strip().replace(" ", "").replace("\u00a0", "")
    s = re.sub(r"[^\d,.\-+]", "", s)

    if not s:
        return None

    # 48.889,69 -> 48889.69
    # 48,889.69 -> 48889.69
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        # Treat comma as decimal only when it looks like a decimal amount.
        if re.search(r",\d{1,2}$", s):
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", s):
        # Indonesian thousands: 5.283 -> 5283, 48.889.69 is handled above
        # because it contains both separators.
        s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def as_int(value: Optional[float]) -> Optional[int]:
    return int(round(value)) if value is not None else None


def extract_first_date(text: str) -> Optional[datetime]:
    """
    Extract the newest explicit date from Indonesian/English prose.
    This is useful when websites do not expose structured metadata.
    """
    months = {
        "januari": 1, "februari": 2, "maret": 3, "april": 4, "mei": 5, "juni": 6,
        "juli": 7, "agustus": 8, "september": 9, "oktober": 10, "november": 11, "desember": 12,
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }
    pat = re.compile(
        r"\b(\d{1,2})\s+("
        + "|".join(re.escape(x) for x in months)
        + r")\s+(20\d{2})\b",
        re.I,
    )
    vals = []
    for d, m, y in pat.findall(text or ""):
        try:
            vals.append(datetime(int(y), months[m.lower()], int(d)))
        except Exception:
            continue
    return max(vals) if vals else None


def extract_structured_date(html: str, text: str) -> Optional[datetime]:
    soup = BeautifulSoup(html or "", "html.parser")

    # JSON-LD
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw = script.string or script.get_text(" ", strip=True)
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        objs = obj if isinstance(obj, list) else [obj]
        stack: List[Any] = list(objs)
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                for key in ("datePublished", "dateModified", "uploadDate"):
                    value = item.get(key)
                    if isinstance(value, str):
                        dt = parse_isoish_date(value)
                        if dt:
                            return dt
                graph = item.get("@graph")
                if isinstance(graph, list):
                    stack.extend(graph)

    # Common metadata
    meta_candidates = [
        ("meta", {"property": "article:published_time"}, "content"),
        ("meta", {"property": "article:modified_time"}, "content"),
        ("meta", {"name": "publish-date"}, "content"),
        ("meta", {"name": "date"}, "content"),
        ("meta", {"name": "dc.date"}, "content"),
        ("time", {"datetime": True}, "datetime"),
    ]
    for tag_name, attrs, attr_name in meta_candidates:
        tag = soup.find(tag_name, attrs=attrs)
        if tag:
            value = tag.get(attr_name)
            dt = parse_isoish_date(value)
            if dt:
                return dt

    return extract_first_date(text)


def parse_isoish_date(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone().replace(tzinfo=None) if dt.tzinfo else dt
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
    ):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def fetch(url: str) -> Tuple[str, int, str]:
    try:
        r = SESSION.get(
            url,
            timeout=TIMEOUT,
            allow_redirects=True,
        )
        if not r.ok:
            return "", r.status_code, r.url
        content_type = r.headers.get("content-type", "")
        if content_type and not any(x in content_type.lower() for x in ("html", "xml", "text")):
            return "", r.status_code, r.url
        return r.text, r.status_code, r.url
    except requests.RequestException:
        return "", 0, url


def text_from_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header"]):
        tag.decompose()
    return norm(soup.get_text(" ", strip=True))


def title_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return norm(soup.title.get_text(" ", strip=True)) if soup.title else ""


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def robots_sitemaps(domain: str) -> List[str]:
    html, _, _ = fetch(f"https://{domain}/robots.txt")
    out: List[str] = []
    for line in (html or "").splitlines():
        if line.lower().startswith("sitemap:"):
            out.append(line.split(":", 1)[1].strip())
    return list(dict.fromkeys(out))[:10] or [f"https://{domain}/sitemap.xml"]


def parse_sitemap(url: str, seen: Optional[set] = None) -> List[str]:
    seen = seen or set()
    url = canon(url)
    if url in seen or len(seen) > 40:
        return []
    seen.add(url)

    html, _, _ = fetch(url)
    if not html:
        return []

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        try:
            soup = BeautifulSoup(html, "xml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

    out: List[str] = []
    for loc in soup.find_all("loc"):
        value = norm(loc.get_text(" ", strip=True))
        if not value.startswith("http"):
            continue

        low = value.lower()
        if low.endswith(".xml") or "sitemap" in low:
            out.extend(parse_sitemap(value, seen))
        elif looks_like_html_url(value):
            out.append(value)

        if len(out) >= MAX_SITEMAP_URLS:
            break

    return list(dict.fromkeys(out))[:MAX_SITEMAP_URLS]


def page_links(url: str, html: str, domains: Sequence[str], limit: int = 100) -> List[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        u = canon(urljoin(url, href))
        if same_domain(u, domains) and looks_like_html_url(u) and u not in out:
            out.append(u)
        if len(out) >= limit:
            break
    return out


def internal_search_urls(
    domain: str,
    keywords: Sequence[str],
) -> List[str]:
    """
    Try common site-search URL conventions without assuming one CMS.
    """
    base = f"https://{domain}"
    candidates = [
        f"{base}/search?q={quote_plus(keywords[0])}",
        f"{base}/search?query={quote_plus(keywords[0])}",
        f"{base}/?s={quote_plus(keywords[0])}",
        f"{base}/cari?search={quote_plus(keywords[0])}",
    ]
    out: List[str] = []
    for u in candidates:
        html, _, final_url = fetch(u)
        if not html:
            continue
        out.extend(page_links(final_url, html, [domain], limit=40))
    return list(dict.fromkeys(out))


def search_engine_site_search(domain: str, query: str) -> List[str]:
    """
    External search discovery is a supplement, not a single point of failure.
    """
    q = quote_plus(f"site:{domain} {query}")
    html, _, _ = fetch(f"https://html.duckduckgo.com/html/?q={q}")
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if not href:
            continue
        if "uddg=" in href:
            try:
                parsed = urlparse(href)
                href = unquote(parse_qs(parsed.query).get("uddg", [""])[0])
            except Exception:
                pass

        href = canon(href)
        if same_domain(href, [domain]) and looks_like_html_url(href) and href not in out:
            out.append(href)
        if len(out) >= MAX_SEARCH_RESULTS:
            break
    return out


def build_candidate_urls(src: Dict[str, Any], extra_keywords: Sequence[str]) -> List[str]:
    keywords = list(dict.fromkeys(src["keywords"] + list(extra_keywords)))
    queries = list(dict.fromkeys(src["queries"] + list(extra_keywords)))

    discovered: List[str] = []

    for domain in src["domains"]:
        for sitemap in robots_sitemaps(domain):
            discovered.extend(parse_sitemap(sitemap))

        home_html, _, home_final = fetch(f"https://{domain}/")
        discovered.extend(page_links(home_final, home_html, src["domains"], limit=100))

        for u in internal_search_urls(domain, keywords[:2]):
            discovered.append(u)

        for q in queries[:8]:
            discovered.extend(search_engine_site_search(domain, q))
            time.sleep(CRAWL_DELAY)

    uniq: List[str] = []
    for url in discovered:
        c = canon(url)
        if c and same_domain(c, src["domains"]) and looks_like_html_url(c) and c not in uniq:
            uniq.append(c)
        if len(uniq) >= MAX_CANDIDATES_PER_SOURCE:
            break

    return uniq


def keyword_variants(keyword: str) -> List[str]:
    base = keyword.lower().strip()
    variants = {base}
    if base == "karhutla":
        variants.update({"kebakaran hutan dan lahan", "kebakaran lahan"})
    elif base == "hotspot":
        variants.update({"titik panas", "firespot", "fire spot"})
    elif base == "omc":
        variants.update({"operasi modifikasi cuaca"})
    return list(variants)


def score_candidate(
    url: str,
    title: str,
    text: str,
    keywords: Sequence[str],
    date: Optional[datetime],
    reference_terms: Sequence[str],
) -> Tuple[float, List[str]]:
    tl = title.lower()
    ul = url.lower()
    txt = text[:MAX_TEXT_CHARS].lower()
    hay = f"{ul} {tl} {txt}"

    score = 0.0
    hits: List[str] = []

    for kw in keywords:
        variants = keyword_variants(kw)
        hit = False
        best_points = 0.0
        for variant in variants:
            if variant in tl:
                best_points = max(best_points, 11.0)
                hit = True
            elif variant in ul:
                best_points = max(best_points, 6.0)
                hit = True
            elif variant in txt:
                best_points = max(best_points, 2.5)
                hit = True
        if hit:
            score += best_points
            hits.append(kw)

    if "2026" in hay:
        score += 2.0

    # Favor current/recent pages, but do not overpower topical relevance.
    today = now_local_naive()
    if date:
        age_days = (today - date).days
        if age_days <= 2:
            score += 18
        elif age_days <= 7:
            score += 13
        elif age_days <= 14:
            score += 9
        elif age_days <= 30:
            score += 5
        elif age_days <= 60:
            score += 2
        elif age_days > 365:
            score -= 8

    for ref in reference_terms:
        if ref.lower() in hay:
            score += 2

    # A report page with an article-like URL is usually stronger than a generic
    # homepage/landing page when topic words are present.
    path = urlparse(url).path.lower()
    if any(token in path for token in ("/berita/", "/news/", "/artikel/", "/siaran-pers/", "/press-release/")):
        score += 3

    return score, hits


def discover_source(
    src: Dict[str, Any],
    extra_keywords: Sequence[str],
    max_age_days: int,
) -> Dict[str, Any]:
    keywords = list(dict.fromkeys(src["keywords"] + list(extra_keywords)))
    urls = build_candidate_urls(src, extra_keywords)

    ranked: List[Dict[str, Any]] = []
    checked = 0

    for url in urls:
        if checked >= MAX_PAGES_PER_RUN:
            break

        html, status_code, final_url = fetch(url)
        checked += 1
        if not html or len(html) < 200:
            continue

        text = text_from_html(html)
        if len(text) < 100:
            continue

        title = title_from_html(html) or urlparse(final_url).path.rsplit("/", 1)[-1]
        pub_date = extract_structured_date(html, text)

        # Hard age filter only applies to pages with a trustworthy date.
        if pub_date and (now_local_naive() - pub_date).days > max_age_days:
            # Keep older pages only if they look like durable dashboard/report pages.
            if not any(x in final_url.lower() for x in ("dashboard", "gis", "karhutla2026")):
                continue

        score, hits = score_candidate(
            final_url,
            title,
            text,
            keywords,
            pub_date,
            reference_terms=("karhutla", "hotspot", "kebakaran hutan dan lahan"),
        )

        ranked.append(
            {
                "url": canon(final_url),
                "title": title,
                "text": text[:MAX_TEXT_CHARS],
                "score": round(score, 2),
                "keyword_hits": hits,
                "status_code": status_code,
                "published_at": pub_date.strftime("%Y-%m-%d") if pub_date else None,
                "pages_checked": checked,
            }
        )

    ranked.sort(
        key=lambda x: (
            x["score"],
            x["published_at"] or "",
            len(x["keyword_hits"]),
        ),
        reverse=True,
    )

    best = ranked[0] if ranked else None

    if best and best["score"] >= RELEVANCE_THRESHOLD:
        best.update(
            {
                "reference_type": "keyword_discovered",
                "validation_passed": True,
                "validation_threshold": RELEVANCE_THRESHOLD,
                "discovery_method": (
                    "robots/sitemap + homepage/internal links + "
                    "site-restricted search + relevance/recency ranking"
                ),
                "pages_ranked": len(ranked),
            }
        )
        return best

    # No discovered page cleared the threshold.
    # Use the safety reference only as a secondary fallback.
    fallback_url = SAFETY_REFERENCES.get(src["key"])
    if fallback_url:
        html, status_code, final_url = fetch(fallback_url)
        text = text_from_html(html)
        title = title_from_html(html) or src["name"]
        pub_date = extract_structured_date(html, text)
        score, hits = score_candidate(
            final_url,
            title,
            text,
            keywords,
            pub_date,
            reference_terms=("karhutla", "hotspot"),
        )

        return {
            "url": canon(final_url),
            "title": title,
            "text": text[:MAX_TEXT_CHARS],
            "score": round(score, 2),
            "keyword_hits": hits,
            "status_code": status_code,
            "published_at": pub_date.strftime("%Y-%m-%d") if pub_date else None,
            "reference_type": "safety_reference_fallback",
            "validation_passed": False,
            "validation_threshold": RELEVANCE_THRESHOLD,
            "discovery_method": "safety reference fallback",
            "pages_checked": checked,
            "pages_ranked": len(ranked),
        }

    return {
        "url": None,
        "title": src["name"],
        "text": "",
        "score": 0,
        "keyword_hits": [],
        "status_code": 0,
        "published_at": None,
        "reference_type": "unavailable",
        "validation_passed": False,
        "validation_threshold": RELEVANCE_THRESHOLD,
        "discovery_method": "no usable source found",
        "pages_checked": checked,
        "pages_ranked": len(ranked),
    }


# ---------------------------------------------------------------------------
# Adaptive extraction helpers
# ---------------------------------------------------------------------------

def find_num(text: str, patterns: Sequence[str]) -> Optional[float]:
    for pattern in patterns:
        m = re.search(pattern, text or "", re.I | re.S)
        if m:
            value = parse_number(m.group(1))
            if value is not None:
                return value
    return None


def find_near_label(
    text: str,
    labels: Sequence[str],
    window: int = 45,
) -> Optional[float]:
    """
    Find a number immediately before or after a label.

    Tight context is intentional: a broad 200+ character search can
    accidentally associate a metric with a later unrelated number in a news
    article, e.g. "43 firespot" followed later by "157,3 hektare".
    """
    compact = norm(text)

    for label in labels:
        escaped = re.escape(label)

        patterns = [
            # Label : 123 / Label sebanyak 123
            rf"{escaped}[^0-9]{{0,{window}}}?([\d][\d.,]*)",
            # 123 Label / 123 firespot
            rf"([\d][\d.,]*)[^0-9]{{0,{window}}}?{escaped}",
        ]

        for pattern in patterns:
            m = re.search(pattern, compact, re.I | re.S)
            if m:
                value = parse_number(m.group(1))
                if value is not None:
                    return value

    return None


def find_area_after_phrase(text: str, phrases: Sequence[str], window: int = 180) -> Optional[float]:
    for phrase in phrases:
        m = re.search(
            rf"{re.escape(phrase)}.{{0,{window}}}?([\d][\d.,]*)\s*(?:hektare|ha)\b",
            text or "",
            re.I | re.S,
        )
        if m:
            value = parse_number(m.group(1))
            if value is not None:
                return value
    return None


def extract_current_metrics(text: str) -> Dict[str, Any]:
    """
    Broad but conservative metric extraction.

    The function intentionally avoids treating every occurrence of
    "lahan terbakar" or "berhasil dipadamkan" as a "today/24h" KPI because
    government articles often mix cumulative and single-day figures.
    """
    t = norm(text)

    total_hotspots = (
        find_near_label(
            t,
            [
                "Total Hotspot Hari Ini",
                "Total Hotspot",
                "Jumlah Hotspot",
                "Hotspot Hari Ini",
                "Total Titik Panas",
                "Titik Panas Hari Ini",
            ],
        )
    )

    # A province ranking such as "Kalimantan Tengah sebanyak 5.283 titik"
    # should NOT be interpreted as Indonesia's total hotspot count.
    if total_hotspots is None:
        m = re.search(
            r"(?:total|jumlah)\s+(?:hotspot|titik panas)[^0-9]{0,80}([\d.,]+)",
            t,
            re.I,
        )
        if m:
            total_hotspots = parse_number(m.group(1))

    fire_spots = find_num(
        t,
        [
            r"([\d.,]+)\s+firespot\b",
            r"([\d.,]+)\s+fire\s*spot\b",
        ],
    )
    if fire_spots is None:
        fire_spots = find_near_label(
            t,
            ["Jumlah Firespot", "Total Fire Spot", "Fire Spot"],
            window=25,
        )

    burned_area_24h = find_area_after_phrase(
        t,
        [
            "Luas Terbakar 24H Terakhir",
            "Luas Terbakar 24 H Terakhir",
            "lahan terbakar dalam 24 jam",
            "lahan terbakar selama 24 jam",
            "lahan terbakar dalam sehari",
            "lahan terbakar pada hari tersebut",
        ],
    )

    handled_today = find_area_after_phrase(
        t,
        [
            "Luas Ditangani Hari Ini",
            "Luas Ditangani 24H",
            "luas yang ditangani hari ini",
            "berhasil dipadamkan pada hari tersebut",
            "berhasil ditangani pada hari tersebut",
        ],
    )

    personnel = (
        find_near_label(
            t,
            [
                "Personel Gabungan",
                "jumlah personel gabungan",
                "jumlah personel",
                "personel yang dikerahkan",
            ],
        )
        or find_num(
            t,
            [
                r"([\d.,]+)\s+(?:personel|orang)\s+(?:gabungan|dikerahkan|terlibat)",
            ],
        )
    )

    air_units = (
        find_near_label(
            t,
            [
                "Total Unit Udara Hari Ini",
                "Jumlah Unit Udara",
                "Total Unit Udara",
            ],
        )
        or find_num(
            t,
            [
                r"([\d.,]+)\s+(?:unit\s+)?(?:pesawat|helikopter)\s+(?:dikerahkan|disiagakan|digunakan)",
            ],
        )
    )

    affected = find_near_label(
        t,
        [
            "Kab/Kota Terdampak",
            "Kabupaten/Kota Terdampak",
            "kabupaten/kota terdampak",
        ],
    )

    ground = find_area_after_phrase(
        t,
        [
            "operasi darat",
            "melalui operasi darat",
            "satgas darat",
        ],
    )

    air = find_area_after_phrase(
        t,
        [
            "operasi udara",
            "melalui operasi udara",
            "water bombing",
        ],
    )

    uncontrolled = find_area_after_phrase(
        t,
        [
            "tersisa",
            "masih dalam penanganan",
            "belum tertangani",
            "belum padam",
            "masih berasap",
        ],
    )

    total_situation = find_area_after_phrase(
        t,
        [
            "total luas lahan terbakar",
            "total luas lahan yang terbakar",
            "total lahan terbakar",
            "total luas kebakaran",
        ],
    )

    return {
        "total_hotspots": as_int(total_hotspots),
        "fire_spots": as_int(fire_spots),
        "burned_area_24h_ha": burned_area_24h,
        "handled_area_today_ha": handled_today,
        "personnel": as_int(personnel),
        "air_units": as_int(air_units),
        "affected_regencies_cities": as_int(affected),
        "handled_area_ground_ha": ground,
        "handled_area_air_ha": air,
        "uncontrolled_area_ha": uncontrolled,
        "total_burned_area_situation_ha": total_situation,
    }



def extract_province_metrics(
    text: str,
    provinces: Sequence[str],
) -> List[Dict[str, Any]]:
    """
    Province extraction is context-based rather than tied to one exact
    article sentence template.
    """
    out: List[Dict[str, Any]] = []
    t = norm(text)

    for province in provinces:
        # Look in a local context window around each province name.
        matches = list(re.finditer(re.escape(province), t, re.I))
        best_area: Optional[float] = None
        best_hotspot: Optional[float] = None

        for m in matches:
            ctx = t[m.start() : m.start() + 450]

            area = find_num(
                ctx,
                [
                    r"(?:luas|terbakar|mencapai|sekitar|total).*?([\d.,]+)\s*(?:hektare|ha)",
                ],
            )
            hotspot = find_num(
                ctx,
                [
                    r"(?:sebanyak|tercatat|mencapai|hingga).*?([\d.,]+)\s*(?:titik|hotspot)",
                ],
            )

            # Prefer larger context-confirmed metrics over accidental small numbers.
            if area is not None:
                best_area = area
            if hotspot is not None:
                best_hotspot = hotspot

        if best_area is not None or best_hotspot is not None:
            row = {
                "name": province,
                "key": re.sub(r"[^a-z0-9]+", "_", province.lower()).strip("_"),
            }
            if best_area is not None:
                row["burned_area_ha"] = best_area
            if best_hotspot is not None:
                row["hotspots"] = as_int(best_hotspot)

            lat, lng = PROVINCE_COORDS.get(province, (None, None))
            row["lat"] = lat
            row["lng"] = lng
            row["source_method"] = "adaptive_context_extraction"
            out.append(row)

    return out


def extract_hotspot_ranking(
    text: str,
    provinces: Sequence[str],
) -> List[Dict[str, Any]]:
    rows = []
    t = norm(text)

    for province in provinces:
        for m in re.finditer(re.escape(province), t, re.I):
            ctx = t[m.start() : m.start() + 320]
            n = find_num(
                ctx,
                [
                    r"(?:sebanyak|tercatat|mencapai|hingga)\s*([\d.,]+)\s*(?:titik panas|titik|hotspot)\b",
                    r"([\d.,]+)\s*(?:titik panas|titik|hotspot)\b",
                ],
            )
            if n is not None:
                rows.append({"province": province, "hotspots": as_int(n)})
                break

    # One province may appear more than once in a long article; keep the
    # largest context-confirmed figure instead of duplicating the row.
    dedup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        prev = dedup.get(row["province"])
        if prev is None or row["hotspots"] > prev["hotspots"]:
            dedup[row["province"]] = row

    return list(dedup.values())



def extract_personnel_composition(text: str) -> List[Dict[str, Any]]:
    groups = [
        "TNI",
        "Relawan",
        "Polri",
        "Perusahaan",
        "MPA",
        "BPBD",
        "Pemda",
        "Satpol PP",
        "Manggala Agni",
        "Polisi Hutan",
        "Pemerintah Pusat",
    ]
    out = []
    t = norm(text)

    for group in groups:
        n = find_num(
            t,
            [
                rf"\b{re.escape(group)}\b\s*[:\-]?\s*([\d.,]+)",
                rf"([\d.,]+)\s+\b{re.escape(group)}\b",
            ],
        )
        if n is not None:
            out.append({"group": group, "count": as_int(n)})

    return out


def extract_source_date_map(selected: Dict[str, Dict[str, Any]]) -> Dict[str, Optional[str]]:
    return {
        key: value.get("published_at")
        for key, value in selected.items()
    }


# ---------------------------------------------------------------------------
# Carry-forward / data quality
# ---------------------------------------------------------------------------

def previous_current_values(previous: Dict[str, Any]) -> Dict[str, Any]:
    current = previous.get("current")
    return dict(current) if isinstance(current, dict) else {}


def resolve_current(
    extracted: Dict[str, Any],
    previous: Dict[str, Any],
    seed_defaults: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[str], List[str], List[str]]:
    prev = previous_current_values(previous)
    final: Dict[str, Any] = {}
    fresh: List[str] = []
    carried: List[str] = []
    missing: List[str] = []

    for key in seed_defaults.keys():
        value = extracted.get(key)
        if value is not None:
            final[key] = value
            fresh.append(key)
            continue

        if prev.get(key) is not None:
            final[key] = prev[key]
            carried.append(key)
            continue

        if seed_defaults.get(key) is not None:
            final[key] = seed_defaults[key]
            carried.append(key)
            continue

        final[key] = None
        missing.append(key)

    return final, fresh, carried, missing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Adaptive government scraper for the Karhutla Indonesia dashboard."
    )
    parser.add_argument(
        "--keyword",
        action="append",
        default=[],
        help="Additional keyword. Repeat the option for multiple keywords.",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="Additional discovery search query.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=60,
        help="Maximum age of dated candidate pages (default: 60).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=RELEVANCE_THRESHOLD,
        help="Minimum relevance score to accept discovered pages.",
    )
    parser.add_argument(
        "--provinces",
        default=",".join(DEFAULT_PRIORITY_PROVINCES),
        help="Comma-separated provinces to extract (default: six priority provinces).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    global RELEVANCE_THRESHOLD
    RELEVANCE_THRESHOLD = args.threshold

    extra_keywords = list(dict.fromkeys(args.keyword))
    extra_queries = list(dict.fromkeys(args.query))

    priority_provinces = [
        norm(x) for x in args.provinces.split(",") if norm(x)
    ]
    for province in priority_provinces:
        if province not in PROVINCE_COORDS:
            # Province without stored coordinates remains valid for text
            # extraction; map fields will be null until coordinates are added.
            PROVINCE_COORDS.setdefault(province, (None, None))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    previous = safe_json_load(OUTPUT_JSON)

    print("=" * 60)
    print(" KARHUTLA INDONESIA 2026 — ADAPTIVE GOVERNMENT SCRAPER")
    print("=" * 60)
    print(f"Run time: {now_iso()}")
    print(f"Discovery threshold: {RELEVANCE_THRESHOLD}")
    print(f"Max page age: {args.days_back} days")

    selected: Dict[str, Dict[str, Any]] = {}

    for src in SOURCE_CONFIG:
        # Inject CLI query additions without mutating global config.
        local_src = dict(src)
        local_src["keywords"] = list(dict.fromkeys(src["keywords"] + extra_keywords))
        local_src["queries"] = list(dict.fromkeys(src["queries"] + extra_queries + extra_keywords))

        result = discover_source(
            local_src,
            extra_keywords=extra_keywords,
            max_age_days=max(1, args.days_back),
        )
        selected[src["key"]] = result

        print(
            f"[{src['name']}] "
            f"{result['reference_type']} | "
            f"score={result['score']} | "
            f"date={result.get('published_at')} | "
            f"{result.get('url')}"
        )

    # Build source text pools. The best recent source from each institution is
    # deliberately combined so metrics can be sourced from different reports.
    bnpb_text = " ".join(
        selected[k]["text"] for k in selected if k in {"bnpb", "gis_bnpb"}
    )
    bmkg_text = selected.get("bmkg", {}).get("text", "")
    kemenhut_text = selected.get("kemenhut", {}).get("text", "")

    combined_text = " ".join(x for x in (bnpb_text, bmkg_text, kemenhut_text) if x)

    extracted = extract_current_metrics(combined_text)

    # Use institution-specific source text if a combined text pattern misses.
    for source_text in (bnpb_text, bmkg_text, kemenhut_text):
        candidate = extract_current_metrics(source_text)
        for key, value in candidate.items():
            if extracted.get(key) is None and value is not None:
                extracted[key] = value

    current, fresh_fields, carried_fields, missing_fields = resolve_current(
        extracted,
        previous,
        SEED_DEFAULTS,
    )

    # Province metrics are recalculated from current discovered text.
    province_text = " ".join(
        selected[k]["text"] for k in selected if k in {"bnpb", "bmkg", "kemenhut"}
    )
    provinces = extract_province_metrics(province_text, priority_provinces)
    hotspot_ranking = extract_hotspot_ranking(province_text, priority_provinces)

    # If no province-specific metric was available this run, preserve the
    # previous dashboard province rows rather than inventing "today's" values.
    previous_provinces = previous.get("priority_provinces")
    if not provinces and isinstance(previous_provinces, list):
        provinces = previous_provinces

    for row in provinces:
        name = row["name"]
        if row.get("lat") is None or row.get("lng") is None:
            lat, lng = PROVINCE_COORDS.get(name, (None, None))
            row["lat"] = lat
            row["lng"] = lng
        row.setdefault("coverage", "Priority response")

    personnel = extract_personnel_composition(selected.get("bnpb", {}).get("text", ""))
    if not personnel and isinstance(previous.get("personnel_composition"), list):
        personnel = previous["personnel_composition"]

    source_cards = []
    for src in SOURCE_CONFIG:
        s = selected[src["key"]]
        source_cards.append(
            {
                "source_key": src["key"],
                "title": src["name"],
                "url": s.get("url"),
                "published_at": s.get("published_at"),
                "reference_type": s.get("reference_type"),
                "relevance_score": s.get("score"),
                "validation_threshold": RELEVANCE_THRESHOLD,
                "validation_passed": s.get("validation_passed"),
                "keyword_hits": s.get("keyword_hits", []),
                "pages_checked": s.get("pages_checked", 0),
                "pages_ranked": s.get("pages_ranked", 0),
                "discovery_method": s.get("discovery_method"),
                "fetch_status": "OK" if s.get("status_code") == 200 else "UNAVAILABLE",
            }
        )

    # Determine the newest trustworthy discovered date across the source set.
    published_dates = [
        parse_isoish_date(s.get("published_at"))
        for s in selected.values()
        if s.get("published_at")
    ]
    published_dates = [d for d in published_dates if d is not None]
    latest_source_date = max(published_dates).strftime("%Y-%m-%d") if published_dates else None

    data = {
        "metadata": {
            "last_updated": now_iso(),
            "last_updated_display": now_local_naive().strftime("%d %B %Y, %H:%M WIB"),
            "report_title": "Karhutla Indonesia 2026 — Situasi & Penanganan",
            "scope": "Six priority provinces",
            "discovery_mode": (
                "adaptive keywords + robots/sitemap + homepage/internal links + "
                "site-restricted search + relevance/recency validation"
            ),
            "relevance_threshold": RELEVANCE_THRESHOLD,
            "max_source_age_days": args.days_back,
            "latest_discovered_source_date": latest_source_date,
            "fallback_policy": (
                "Safety references are secondary only. Missing metric values are "
                "carried forward from the previous JSON and explicitly marked as such."
            ),
            "author": "Kelvin Irawan",
            "data_quality": {
                "fresh_fields": fresh_fields,
                "carried_forward_fields": carried_fields,
                "missing_fields": missing_fields,
                "metric_extraction_mode": "adaptive_patterns",
            },
            "source_dates": extract_source_date_map(selected),
        },
        "current": current,
        "priority_provinces": provinces,
        "hotspot_window_latest": {
            "as_of_source_date": latest_source_date,
            "high_confidence": hotspot_ranking,
            "source_method": "adaptive province-context extraction",
        },
        "personnel_composition": personnel,
        "sources": source_cards,
    }

    if not provinces and isinstance(previous.get("priority_provinces"), list):
        data["priority_provinces"] = previous["priority_provinces"]

    write_json(data, OUTPUT_JSON)
    write_json(data["priority_provinces"], PROVINCE_OUTPUT)
    write_json(
        {
            "generated_at": now_iso(),
            "discovery_mode": data["metadata"]["discovery_mode"],
            "sources": source_cards,
        },
        REGISTRY_OUTPUT,
    )

    print("-" * 60)
    print("SCRAPING COMPLETE")
    print(f"JSON: {OUTPUT_JSON}")
    print(f"Province metrics: {PROVINCE_OUTPUT}")
    print(f"Source registry: {REGISTRY_OUTPUT}")
    print(
        f"Fresh fields: {len(fresh_fields)} | "
        f"Carried forward: {len(carried_fields)} | "
        f"Missing: {len(missing_fields)}"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as exc:
        print("\nSCRAPER ERROR:")
        print(repr(exc))
        raise
