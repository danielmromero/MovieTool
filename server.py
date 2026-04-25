#!/usr/bin/env python3
from __future__ import annotations
import json
import math
import os
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
CACHE_FILE = DATA_DIR / "catalog_cache.json"
ACTOR_CACHE_FILE = DATA_DIR / "actor_cache.json"
INDEX_FILE = APP_DIR / "index.html"
PID_FILE = DATA_DIR / "server.pid"
STATE_FILE = DATA_DIR / "server_state.json"
LOG_FILE = DATA_DIR / "server.log"
HOST = os.getenv("HOST", "0.0.0.0" if os.getenv("PORT") else "127.0.0.1")
DEFAULT_PORT = int(os.getenv("PORT", "8766"))
COUNTRY = "US"
LANGUAGE = "en"
MAX_PER_PROVIDER = 2000
PAGE_SIZE = 100
REQUEST_TIMEOUT = 25
AUTO_REFRESH_INTERVAL_SECONDS = 2 * 24 * 60 * 60
AUTO_REFRESH_RETRY_SECONDS = 60 * 60
INITIAL_AUTO_REFRESH_RETRY_SECONDS = 15 * 60
SCHEDULER_POLL_SECONDS = 30 * 60
MANUAL_REFRESH_PASSWORD = os.getenv("MANUAL_REFRESH_PASSWORD", "GetTheNewStuff")

DATA_DIR.mkdir(exist_ok=True)

SERVICE_TARGETS = {
    "netflix": {
        "label": "Netflix",
        "match_any": ["netflix"],
        "exclude_any": ["kids", "dvd", "channel", "store", "amazon"],
        "prefer_slug": ["netflix"],
    },
    "hulu": {
        "label": "Hulu",
        "match_any": ["hulu"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["hulu"],
    },
    "max": {
        "label": "HBO Max",
        "match_any": ["hbo max", "max"],
        "exclude_any": ["channel", "amazon", "apple tv channel", "sling"],
        "prefer_slug": ["max", "hbo-max"],
    },
    "disney": {
        "label": "Disney+",
        "match_any": ["disney+", "disney plus"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["disney-plus"],
    },
    "paramount": {
        "label": "Paramount+",
        "match_any": ["paramount+", "paramount plus"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["paramount-plus"],
    },
    "peacock": {
        "label": "Peacock",
        "match_any": ["peacock premium", "peacock premium plus", "peacock"],
        "exclude_any": ["amazon channel", "apple tv channel"],
        "prefer_slug": ["peacock", "peacocktv"],
    },
    "prime": {
        "label": "Prime Video",
        "match_any": ["amazon prime video", "prime video"],
        "exclude_any": ["channel", "apple tv", "dvd", "store"],
        "prefer_slug": ["amazonprimevideo", "amazon-prime-video"],
    },
    "apple": {
        "label": "Apple TV+",
        "match_any": ["apple tv+", "apple tv plus", "apple tv"],
        "exclude_any": ["store", "amazon channel", "apple tv channel"],
        "prefer_slug": ["apple-tv-plus"],
    },
    "starz": {
        "label": "Starz",
        "match_any": ["starz"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["starz"],
    },
    "amc": {
        "label": "AMC+",
        "match_any": ["amc+", "amc plus"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["amc-plus", "amcplus"],
    },
    "mgm": {
        "label": "MGM+",
        "match_any": ["mgm+", "mgm plus", "epix"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["mgm-plus", "epix"],
    },
    "criterion": {
        "label": "Criterion Channel",
        "match_any": ["criterion channel"],
        "exclude_any": ["amazon", "apple tv channel"],
        "prefer_slug": ["criterion-channel"],
    },
    "crunchyroll": {
        "label": "Crunchyroll",
        "match_any": ["crunchyroll"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["crunchyroll"],
    },
    "tubi": {
        "label": "Tubi",
        "match_any": ["tubi"],
        "exclude_any": ["channel", "amazon", "apple tv channel"],
        "prefer_slug": ["tubi-tv", "tubi"],
    },
}

PRIMARY_MONETIZATION = {"flatrate", "ads", "free"}
SECONDARY_MONETIZATION = {"rent", "buy"}

GENRE_LABELS = {
    "action": "Action",
    "act": "Action",
    "adventure": "Adventure",
    "adv": "Adventure",
    "animation": "Animation",
    "ani": "Animation",
    "anime": "Anime",
    "arthouse": "Arthouse",
    "children": "Children",
    "comedy": "Comedy",
    "cmy": "Comedy",
    "crime": "Crime",
    "crm": "Crime",
    "documentary": "Documentary",
    "doc": "Documentary",
    "drama": "Drama",
    "drm": "Drama",
    "european": "European",
    "family": "Family",
    "fml": "Family",
    "fantasy": "Fantasy",
    "fnt": "Fantasy",
    "history": "History",
    "hst": "History",
    "horror": "Horror",
    "hrr": "Horror",
    "music": "Music",
    "msc": "Music",
    "musical": "Musical",
    "mystery": "Mystery",
    "mys": "Mystery",
    "reality": "Reality",
    "romance": "Romance",
    "rom": "Romance",
    "rma": "Romance",
    "science-fiction": "Sci-Fi",
    "sci-fi": "Sci-Fi",
    "scifi": "Sci-Fi",
    "sci": "Sci-Fi",
    "sfc": "Sci-Fi",
    "sport": "Sport",
    "spt": "Sport",
    "thriller": "Thriller",
    "trl": "Thriller",
    "war": "War",
    "western": "Western",
    "wst": "Western",
}

QUERIES = {
    "popular": (
        "query GetPopularTitles("
        "$popularTitlesFilter: TitleFilter "
        "$country: Country! "
        "$language: Language! "
        "$first: Int! = 70 "
        "$formatPoster: ImageFormat, "
        "$formatOfferIcon: ImageFormat, "
        "$profile: PosterProfile "
        "$backdropProfile: BackdropProfile, "
        "$filter: OfferFilter!, "
        "$offset: Int = 0"
        ") {"
        " popularTitles(country: $country filter: $popularTitlesFilter first: $first sortBy: POPULAR sortRandomSeed: 0 offset: $offset) {"
        "  __typename edges { node { ...TitleDetails __typename } __typename }"
        " }"
        "}"
        " fragment TitleDetails on MovieOrShowOrSeasonOrEpisode {"
        "  id objectId objectType"
        "  content(country: $country, language: $language) { ...ContentDetails __typename }"
        "  ...StreamingChartInfoFragment"
        "  ... on Show { totalSeasonCount }"
        "  ... on Season { totalEpisodeCount }"
        "  offers(country: $country, platform: WEB, filter: $filter) { ...TitleOffer }"
        "  __typename"
        " }"
        " fragment StreamingChartInfoFragment on MovieOrShowOrSeason {"
        "  streamingCharts(country: $country) {"
        "   edges { streamingChartInfo { rank trend trendDifference daysInTop3 daysInTop10 daysInTop100 daysInTop1000 topRank updatedAt __typename } __typename }"
        "   __typename"
        "  }"
        " }"
        " fragment ContentDetails on MovieOrShowOrSeasonOrEpisodeContent {"
        "  title originalReleaseYear originalReleaseDate runtime shortDescription ...FullContentDetails"
        "  ... on MovieOrShowContent { ageCertification }"
        "  ... on SeasonContent { seasonNumber }"
        "  ... on EpisodeContent { seasonNumber episodeNumber }"
        " }"
        " fragment FullContentDetails on MovieOrShowOrSeasonContent {"
        "  fullPath genres { shortName __typename } externalIds { imdbId tmdbId __typename }"
        "  posterUrl(profile: $profile, format: $formatPoster)"
        "  backdrops(profile: $backdropProfile, format: $formatPoster) { backdropUrl __typename }"
        "  scoring { imdbScore imdbVotes tmdbPopularity tmdbScore tomatoMeter certifiedFresh jwRating __typename }"
        "  interactions { likelistAdditions dislikelistAdditions __typename }"
        " }"
        " fragment TitleOffer on Offer {"
        "  id monetizationType presentationType retailPrice(language: $language) retailPriceValue currency lastChangeRetailPriceValue type"
        "  package { ...PackageDetails } standardWebURL elementCount availableTo"
        "  deeplinkRoku: deeplinkURL(platform: ROKU_OS) subtitleLanguages videoTechnology audioTechnology audioLanguages __typename"
        " }"
        " fragment PackageDetails on Package {"
        "  id packageId clearName technicalName shortName slug monetizationTypes icon(profile: S100, format: $formatOfferIcon) __typename"
        " }"
    ),
    "providers": (
        "query GetProviders($country: Country!, $formatOfferIcon: ImageFormat) {"
        " packages(country: $country, platform: WEB, includeAddons: true) {"
        "  id packageId clearName technicalName shortName slug monetizationTypes icon(profile: S100, format: $formatOfferIcon) __typename"
        " } __typename }"
    ),
}

TASKS = {}
TASK_LOCK = threading.Lock()
AUTO_REFRESH_SCHEDULER_STARTED = False
ACTOR_CACHE_LOCK = threading.Lock()


def log(message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}\n"
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(line, end="")


def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            log(f"Failed to load cache: {exc}")
    return {
        "generated_at": None,
        "coverage": {},
        "items": [],
        "notes": [],
        "last_error": None,
        "last_attempted_at": None,
        "source": "JustWatch unofficial GraphQL endpoint",
        "region": COUNTRY,
    }


def save_cache(data: dict) -> None:
    CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_actor_cache() -> dict:
    if ACTOR_CACHE_FILE.exists():
        try:
            data = json.loads(ACTOR_CACHE_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            log(f"Failed to load actor cache: {exc}")
    return {}


def save_actor_cache(data: dict) -> None:
    ACTOR_CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def merge_genre_labels(labels: list[str]) -> list[str]:
    merged = [label for label in labels if label]
    label_set = set(merged)
    if "Romance" in label_set and "Comedy" in label_set and "Romantic Comedy" not in label_set:
        merged.append("Romantic Comedy")
    return sorted(set(merged))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def seconds_since(value: str | None, now: datetime | None = None) -> float | None:
    parsed = parse_iso_datetime(value)
    if parsed is None:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (now - parsed).total_seconds())


def should_auto_refresh(cache: dict) -> bool:
    now = datetime.now(timezone.utc)
    has_items = bool(cache.get("items"))
    generated_age = seconds_since(cache.get("generated_at"), now)
    attempted_age = seconds_since(cache.get("last_attempted_at"), now)
    if not has_items:
        return attempted_age is None or attempted_age >= INITIAL_AUTO_REFRESH_RETRY_SECONDS
    if generated_age is None:
        return attempted_age is None or attempted_age >= INITIAL_AUTO_REFRESH_RETRY_SECONDS
    if generated_age < AUTO_REFRESH_INTERVAL_SECONDS:
        return False
    return attempted_age is None or attempted_age >= AUTO_REFRESH_RETRY_SECONDS


def next_auto_refresh_iso(cache: dict) -> str | None:
    generated = parse_iso_datetime(cache.get("generated_at"))
    if generated is None:
        return None
    return (generated + timedelta(seconds=AUTO_REFRESH_INTERVAL_SECONDS)).isoformat()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def genre_label(raw: str) -> str:
    if not raw:
        return "Unknown"
    key = raw.strip().lower()
    if key in GENRE_LABELS:
        return GENRE_LABELS[key]
    key = key.replace("_", " ").replace("-", " ")
    return " ".join(word.capitalize() for word in key.split())


def service_match(provider: dict, target: dict) -> bool:
    haystack = " ".join(
        [
            provider.get("clearName", ""),
            provider.get("technicalName", ""),
            provider.get("shortName", ""),
            provider.get("slug", ""),
        ]
    ).lower()
    if target.get("prefer_slug"):
        slug = (provider.get("slug") or "").lower()
        if slug in target["prefer_slug"]:
            return True
    if not any(term in haystack for term in target["match_any"]):
        return False
    if any(term in haystack for term in target.get("exclude_any", [])):
        return False
    return True


def json_response(handler: BaseHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
    body = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def read_json_body(handler: BaseHTTPRequestHandler) -> dict:
    try:
        length = int(handler.headers.get("Content-Length", "0") or "0")
    except Exception:
        length = 0
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    try:
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def post_graphql(payload: dict) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://apis.justwatch.com/graphql",
        data=body,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.justwatch.com",
            "Referer": "https://www.justwatch.com/",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)
    if "errors" in data:
        raise RuntimeError(str(data["errors"]))
    return data


def fetch_providers(country: str = COUNTRY) -> list[dict]:
    payload = {
        "operationName": "GetProviders",
        "variables": {"country": country.upper(), "formatOfferIcon": "PNG"},
        "query": QUERIES["providers"],
    }
    data = post_graphql(payload)
    return data.get("data", {}).get("packages", [])


def resolve_service_codes(country: str = COUNTRY) -> dict:
    providers = fetch_providers(country)
    resolved = {}
    for key, target in SERVICE_TARGETS.items():
        matches = [p for p in providers if service_match(p, target)]
        codes = []
        names = []
        for provider in matches:
            code = provider.get("shortName")
            if code and code not in codes:
                codes.append(code)
            name = provider.get("clearName") or provider.get("technicalName") or code
            if name and name not in names:
                names.append(name)
        resolved[key] = {
            "label": target["label"],
            "codes": codes,
            "provider_names": names,
        }
    return resolved


def make_popular_payload(provider_codes: list[str], offset: int, count: int = PAGE_SIZE) -> dict:
    return {
        "operationName": "GetPopularTitles",
        "variables": {
            "first": count,
            "popularTitlesFilter": {"packages": provider_codes},
            "formatPoster": "JPG",
            "formatOfferIcon": "PNG",
            "profile": "S718",
            "backdropProfile": "S1920",
            "filter": {"bestOnly": True},
            "country": COUNTRY,
            "language": LANGUAGE,
            "offset": offset,
        },
        "query": QUERIES["popular"],
    }


def parse_entry(node: dict) -> dict | None:
    try:
        content = node["content"]
    except Exception:
        return None
    backdrops = []
    for item in content.get("backdrops", []) or []:
        url = item.get("backdropUrl")
        if url:
            backdrops.append("https://images.justwatch.com" + url)
    full_path = content.get("fullPath") or ""
    imdb_votes = None
    scoring = content.get("scoring") or {}
    imdb = scoring.get("imdbScore")
    imdb_votes = scoring.get("imdbVotes")
    try:
        imdb_votes = int(imdb_votes) if imdb_votes is not None else None
    except Exception:
        imdb_votes = None
    genres = [g.get("shortName") for g in (content.get("genres") or []) if g and g.get("shortName")]
    offers = []
    for offer in node.get("offers", []) or []:
        package = offer.get("package") or {}
        offers.append(
            {
                "monetization_type": offer.get("monetizationType"),
                "presentation_type": offer.get("presentationType"),
                "url": offer.get("standardWebURL"),
                "available_to": offer.get("availableTo"),
                "package": {
                    "clear_name": package.get("clearName"),
                    "technical_name": package.get("technicalName"),
                    "short_name": package.get("shortName"),
                    "slug": package.get("slug"),
                },
            }
        )
    chart_info = None
    chart_edges = (((node.get("streamingCharts") or {}).get("edges") or []))
    if chart_edges:
        chart_info = chart_edges[0].get("streamingChartInfo")
    ext = content.get("externalIds") or {}
    object_type = (node.get("objectType") or "").upper()
    total_season_count = node.get("totalSeasonCount") if object_type == "SHOW" else None
    return {
        "entry_id": node.get("id"),
        "object_id": node.get("objectId"),
        "object_type": object_type,
        "title": content.get("title"),
        "year": content.get("originalReleaseYear"),
        "release_date": content.get("originalReleaseDate"),
        "runtime_minutes": content.get("runtime"),
        "description": content.get("shortDescription"),
        "genres": genres,
        "genre_labels": merge_genre_labels(sorted({genre_label(g) for g in genres})),
        "imdb_id": ext.get("imdbId"),
        "tmdb_id": ext.get("tmdbId"),
        "poster_url": ("https://images.justwatch.com" + content.get("posterUrl")) if content.get("posterUrl") else None,
        "backdrops": backdrops,
        "age_certification": content.get("ageCertification"),
        "imdb_score": imdb,
        "imdb_votes": imdb_votes,
        "tmdb_popularity": scoring.get("tmdbPopularity"),
        "tmdb_score": scoring.get("tmdbScore"),
        "tomatometer": scoring.get("tomatoMeter"),
        "jw_rating": scoring.get("jwRating"),
        "certified_fresh": scoring.get("certifiedFresh"),
        "chart_rank": (chart_info or {}).get("rank") if chart_info else None,
        "chart_top_rank": (chart_info or {}).get("topRank") if chart_info else None,
        "chart_trend": (chart_info or {}).get("trend") if chart_info else None,
        "chart_trend_difference": (chart_info or {}).get("trendDifference") if chart_info else None,
        "chart_updated_at": (chart_info or {}).get("updatedAt") if chart_info else None,
        "total_season_count": total_season_count,
        "justwatch_url": f"https://www.justwatch.com{full_path}" if full_path else None,
        "offers": offers,
    }


def select_service_offer(entry: dict, service_codes: list[str]) -> dict | None:
    primary = []
    secondary = []
    for offer in entry.get("offers", []):
        code = (((offer.get("package") or {}).get("short_name")) or "").lower()
        if code not in {c.lower() for c in service_codes}:
            continue
        monetization = (offer.get("monetization_type") or "").lower()
        if monetization in PRIMARY_MONETIZATION:
            primary.append(offer)
        elif monetization in SECONDARY_MONETIZATION:
            secondary.append(offer)
    if primary:
        return primary[0]
    if secondary:
        return secondary[0]
    return None


def movie_key(entry: dict) -> str:
    if entry.get("imdb_id"):
        return f"imdb:{entry['imdb_id']}"
    if entry.get("object_id"):
        return f"jw:{entry['object_id']}"
    base = f"{(entry.get('object_type') or '').lower()}:{normalize_text(entry.get('title') or '')}:{entry.get('year') or ''}"
    return f"title:{base}"


def merge_movie(existing: dict | None, incoming: dict, service_key: str, service_label: str, service_offer: dict | None) -> dict:
    if existing is None:
        existing = {
            "key": movie_key(incoming),
            "object_type": incoming.get("object_type"),
            "type_label": "TV Show" if incoming.get("object_type") == "SHOW" else "Movie",
            "title": incoming.get("title"),
            "year": incoming.get("year"),
            "release_date": incoming.get("release_date"),
            "runtime_minutes": incoming.get("runtime_minutes"),
            "total_season_count": incoming.get("total_season_count"),
            "description": incoming.get("description"),
            "genres": incoming.get("genres") or [],
            "genre_labels": incoming.get("genre_labels") or [],
            "imdb_id": incoming.get("imdb_id"),
            "tmdb_id": incoming.get("tmdb_id"),
            "poster_url": incoming.get("poster_url"),
            "backdrops": incoming.get("backdrops") or [],
            "age_certification": incoming.get("age_certification"),
            "imdb_score": incoming.get("imdb_score"),
            "imdb_votes": incoming.get("imdb_votes"),
            "tmdb_popularity": incoming.get("tmdb_popularity"),
            "tmdb_score": incoming.get("tmdb_score"),
            "tomatometer": incoming.get("tomatometer"),
            "jw_rating": incoming.get("jw_rating"),
            "certified_fresh": incoming.get("certified_fresh"),
            "chart_rank": incoming.get("chart_rank"),
            "chart_top_rank": incoming.get("chart_top_rank"),
            "chart_trend": incoming.get("chart_trend"),
            "chart_trend_difference": incoming.get("chart_trend_difference"),
            "chart_updated_at": incoming.get("chart_updated_at"),
            "justwatch_url": incoming.get("justwatch_url"),
            "services": [],
            "service_links": {},
            "service_offer_types": {},
            "service_added_at": {},
        }
    else:
        for field in [
            "object_type",
            "year",
            "release_date",
            "runtime_minutes",
            "total_season_count",
            "description",
            "poster_url",
            "age_certification",
            "imdb_score",
            "imdb_votes",
            "tmdb_popularity",
            "tmdb_score",
            "tomatometer",
            "jw_rating",
            "certified_fresh",
            "chart_rank",
            "chart_top_rank",
            "chart_trend",
            "chart_trend_difference",
            "chart_updated_at",
            "justwatch_url",
        ]:
            if existing.get(field) in (None, "", [], {}) and incoming.get(field) not in (None, "", [], {}):
                existing[field] = incoming.get(field)
        existing["type_label"] = "TV Show" if existing.get("object_type") == "SHOW" else "Movie"
        if incoming.get("poster_url") and not existing.get("poster_url"):
            existing["poster_url"] = incoming["poster_url"]
        if incoming.get("description") and len(incoming["description"] or "") > len(existing.get("description") or ""):
            existing["description"] = incoming["description"]
        existing["genres"] = sorted(set((existing.get("genres") or []) + (incoming.get("genres") or [])))
        existing["genre_labels"] = merge_genre_labels(sorted(set((existing.get("genre_labels") or []) + (incoming.get("genre_labels") or []))))
        existing["backdrops"] = list(dict.fromkeys((existing.get("backdrops") or []) + (incoming.get("backdrops") or [])))
        if not existing.get("total_season_count") and incoming.get("total_season_count"):
            existing["total_season_count"] = incoming.get("total_season_count")
    if service_label not in existing["services"]:
        existing["services"].append(service_label)
        existing["services"].sort()
    if service_offer:
        if service_offer.get("url"):
            existing["service_links"][service_key] = service_offer["url"]
        existing["service_offer_types"][service_key] = service_offer.get("monetization_type")
        if service_offer.get("available_to"):
            existing["service_added_at"][service_key] = service_offer.get("available_to")
    return existing


def fetch_service_movies(service_key: str, service_info: dict) -> tuple[list[dict], dict]:
    codes = service_info.get("codes") or []
    label = service_info.get("label") or service_key
    if not codes:
        return [], {"label": label, "codes": [], "resolved_provider_names": [], "pages": 0, "fetched": 0, "titles": 0, "partial": False, "error": "No provider codes resolved."}
    movies = {}
    fetched = 0
    pages = 0
    partial = False
    for offset in range(0, MAX_PER_PROVIDER, PAGE_SIZE):
        payload = make_popular_payload(codes, offset, PAGE_SIZE)
        data = post_graphql(payload)
        edges = data.get("data", {}).get("popularTitles", {}).get("edges", [])
        pages += 1
        if not edges:
            break
        fetched += len(edges)
        for edge in edges:
            node = edge.get("node") or {}
            entry = parse_entry(node)
            if not entry:
                continue
            if entry.get("object_type") not in {"MOVIE", "SHOW"}:
                continue
            service_offer = select_service_offer(entry, codes)
            if not service_offer:
                continue
            key = movie_key(entry)
            movies[key] = merge_movie(movies.get(key), entry, service_key, label, service_offer)
        if len(edges) < PAGE_SIZE:
            break
    if fetched >= MAX_PER_PROVIDER:
        partial = True
    return list(movies.values()), {
        "label": label,
        "codes": codes,
        "resolved_provider_names": service_info.get("provider_names") or [],
        "pages": pages,
        "fetched": fetched,
        "titles": len(movies),
        "partial": partial,
        "error": None,
    }


def compute_discovery_score(movie: dict) -> float:
    score = 0.0
    imdb = movie.get("imdb_score")
    votes = movie.get("imdb_votes") or 0
    rank = movie.get("chart_rank")
    provider_count = len(movie.get("services") or [])
    if isinstance(imdb, (int, float)):
        score += imdb * 10.0
    if votes:
        score += min(25.0, math.log10(max(votes, 1)) * 6.0)
    if rank:
        score += max(0.0, 20.0 - min(rank, 200) / 10.0)
    score += provider_count * 2.5
    return round(score, 2)


def fetch_text(url: str, headers: dict | None = None) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            **(headers or {}),
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _extract_actor_names_from_jsonld(node) -> list[str]:
    names = []
    if isinstance(node, dict):
        node_type = str(node.get("@type") or "")
        actors = node.get("actor") or node.get("actors")
        if actors and ("Movie" in node_type or "TV" in node_type or not node_type):
            if not isinstance(actors, list):
                actors = [actors]
            for actor in actors:
                if isinstance(actor, dict):
                    name = actor.get("name")
                    if isinstance(name, str) and name.strip():
                        names.append(name.strip())
            if names:
                return names
        for value in node.values():
            names = _extract_actor_names_from_jsonld(value)
            if names:
                return names
    elif isinstance(node, list):
        for value in node:
            names = _extract_actor_names_from_jsonld(value)
            if names:
                return names
    return []


def fetch_actor_preview_from_imdb(imdb_id: str) -> dict:
    url = f"https://www.imdb.com/title/{imdb_id}/"
    html = fetch_text(url)
    matches = re.findall(r"<script[^>]+type=[\"']application/ld\\+json[\"'][^>]*>(.*?)</script>", html, flags=re.I | re.S)
    actor_names: list[str] = []
    for raw in matches:
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        actor_names = _extract_actor_names_from_jsonld(payload)
        if actor_names:
            break
    actor_names = [name for name in actor_names if name][:6]
    if not actor_names:
        raise RuntimeError("Could not extract cast preview from IMDb page")
    return {
        "imdb_id": imdb_id,
        "actors": actor_names,
        "fetched_at": utc_now_iso(),
        "source": "IMDb title page",
        "error": None,
    }


def get_actor_preview(imdb_id: str) -> dict:
    if not imdb_id:
        return {"imdb_id": imdb_id, "actors": [], "fetched_at": utc_now_iso(), "source": "none", "error": "Missing IMDb id"}
    with ACTOR_CACHE_LOCK:
        cache = load_actor_cache()
        cached = cache.get(imdb_id)
        if isinstance(cached, dict) and (cached.get("actors") or cached.get("error")):
            return cached
    try:
        result = fetch_actor_preview_from_imdb(imdb_id)
    except Exception as exc:
        result = {
            "imdb_id": imdb_id,
            "actors": [],
            "fetched_at": utc_now_iso(),
            "source": "IMDb title page",
            "error": str(exc),
        }
    with ACTOR_CACHE_LOCK:
        cache = load_actor_cache()
        cache[imdb_id] = result
        save_actor_cache(cache)
    return result


def get_actor_previews(imdb_ids: list[str]) -> dict:
    results = {}
    seen = set()
    for imdb_id in imdb_ids[:24]:
        if not imdb_id or imdb_id in seen:
            continue
        seen.add(imdb_id)
        results[imdb_id] = get_actor_preview(imdb_id)
    return results


def generate_catalog() -> dict:
    start = time.time()
    log("Refreshing catalog...")
    service_map = resolve_service_codes(COUNTRY)
    merged = {}
    coverage = {}
    errors = []
    for service_key, service_info in service_map.items():
        try:
            items, info = fetch_service_movies(service_key, service_info)
            coverage[service_key] = info
            for item in items:
                key = item["key"]
                if key in merged:
                    merged[key] = merge_movie(merged[key], item, service_key, SERVICE_TARGETS[service_key]["label"], None)
                    merged[key]["service_links"].update(item.get("service_links") or {})
                    merged[key]["service_offer_types"].update(item.get("service_offer_types") or {})
                    merged[key]["service_added_at"].update(item.get("service_added_at") or {})
                    merged[key]["services"] = sorted(set((merged[key].get("services") or []) + (item.get("services") or [])))
                else:
                    merged[key] = item
        except Exception as exc:
            msg = f"{SERVICE_TARGETS[service_key]['label']}: {exc}"
            coverage[service_key] = {
                "label": SERVICE_TARGETS[service_key]["label"],
                "codes": service_info.get("codes") or [],
                "resolved_provider_names": service_info.get("provider_names") or [],
                "pages": 0,
                "fetched": 0,
                "titles": 0,
                "partial": False,
                "error": str(exc),
            }
            errors.append(msg)
            log(f"Service refresh failed: {msg}")
    items = list(merged.values())
    for item in items:
        item["genre_labels"] = merge_genre_labels(item.get("genre_labels") or [])
        item["service_count"] = len(item.get("services") or [])
        item["discovery_score"] = compute_discovery_score(item)
        item["primary_watch_link"] = next(iter((item.get("service_links") or {}).values()), None) or item.get("justwatch_url")
        item["imdb_url"] = f"https://www.imdb.com/title/{item['imdb_id']}/" if item.get("imdb_id") else None
        item["type_label"] = "TV Show" if item.get("object_type") == "SHOW" else "Movie"
    items.sort(key=lambda x: (-(x.get("imdb_score") or 0), -(x.get("imdb_votes") or 0), x.get("title") or ""))
    notes = [
        "This app refreshes live streaming availability and ratings from JustWatch's public-facing GraphQL endpoint.",
        "It includes both movies and TV shows; use the type filter to switch between them or view both together.",
        "Coverage is strongest for notable and actively surfaced titles; very large libraries may be capped by the public endpoint.",
        "Prime Video and Netflix can hit the public endpoint limit sooner than smaller catalogs.",
    ]
    if errors:
        notes.append("Some services returned errors during refresh: " + "; ".join(errors))
    result = {
        "generated_at": utc_now_iso(),
        "region": COUNTRY,
        "source": "JustWatch unofficial GraphQL endpoint",
        "coverage": coverage,
        "items": items,
        "notes": notes,
        "last_error": None,
        "last_attempted_at": utc_now_iso(),
        "elapsed_seconds": round(time.time() - start, 2),
    }
    save_cache(result)
    log(f"Refresh complete: {len(items)} titles in {result['elapsed_seconds']}s")
    return result


def start_refresh_task(auto: bool = False) -> str:
    task_id = f"task-{int(time.time() * 1000)}"
    task = {
        "id": task_id,
        "status": "running",
        "started_at": utc_now_iso(),
        "finished_at": None,
        "result_summary": None,
        "error": None,
        "auto": auto,
    }
    with TASK_LOCK:
        TASKS[task_id] = task

    def runner():
        try:
            catalog = generate_catalog()
            task["status"] = "done"
            task["result_summary"] = {
                "items": len(catalog.get("items", [])),
                "generated_at": catalog.get("generated_at"),
                "elapsed_seconds": catalog.get("elapsed_seconds"),
            }
        except Exception as exc:
            task["status"] = "error"
            task["error"] = str(exc)
            cache = load_cache()
            cache["last_error"] = str(exc)
            cache["last_attempted_at"] = utc_now_iso()
            notes = list(cache.get("notes") or [])
            message = f"Latest refresh failed: {exc}"
            notes = [n for n in notes if not str(n).startswith("Latest refresh failed:")]
            notes.append(message)
            cache["notes"] = notes
            save_cache(cache)
            log(f"Refresh task failed: {exc}")
        finally:
            task["finished_at"] = utc_now_iso()

    threading.Thread(target=runner, daemon=True).start()
    return task_id


def get_running_task() -> dict | None:
    with TASK_LOCK:
        running = [t for t in TASKS.values() if t.get("status") == "running"]
        return running[-1] if running else None


def auto_refresh_scheduler_loop() -> None:
    while True:
        time.sleep(SCHEDULER_POLL_SECONDS)
        try:
            if get_running_task():
                continue
            cache = load_cache()
            if should_auto_refresh(cache):
                log("Auto-refresh scheduler starting a background refresh.")
                start_refresh_task(auto=True)
        except Exception as exc:
            log(f"Auto-refresh scheduler error: {exc}")


def ensure_auto_refresh() -> None:
    global AUTO_REFRESH_SCHEDULER_STARTED
    if not AUTO_REFRESH_SCHEDULER_STARTED:
        AUTO_REFRESH_SCHEDULER_STARTED = True
        threading.Thread(target=auto_refresh_scheduler_loop, daemon=True).start()
    if get_running_task():
        return
    cache = load_cache()
    if should_auto_refresh(cache):
        reason = "initial catalog build" if not cache.get("items") else "catalog older than two days"
        log(f"Auto-refresh trigger: {reason}.")
        start_refresh_task(auto=True)


def build_catalog_payload() -> dict:
    payload = load_cache()
    running = get_running_task()
    if running:
        payload["running_task"] = {
            "id": running.get("id"),
            "status": running.get("status"),
            "started_at": running.get("started_at"),
            "auto": running.get("auto", False),
        }
    payload["auto_refresh_interval_days"] = 2
    payload["next_auto_refresh_at"] = next_auto_refresh_iso(payload)
    payload["manual_refresh_requires_password"] = True
    return payload


def get_index_html() -> str:
    return INDEX_FILE.read_text(encoding="utf-8")



class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:
        return

    def do_HEAD(self):
        if self.path in ("/health", "/", "/index.html", "/favicon.ico"):
            self.send_response(200)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            json_response(self, {"ok": True})
            return
        if self.path == "/favicon.ico":
            text_response(self, "", content_type="image/x-icon")
            return
        ensure_auto_refresh()
        if self.path in ("/", "/index.html"):
            text_response(self, get_index_html(), content_type="text/html; charset=utf-8")
            return
        if self.path == "/api/catalog":
            json_response(self, build_catalog_payload())
            return
        if self.path.startswith("/api/actors"):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            ids_raw = params.get("ids", [""])[0]
            imdb_ids = [value.strip() for value in ids_raw.split(",") if value.strip()]
            json_response(self, {"actors": get_actor_previews(imdb_ids)})
            return
        if self.path.startswith("/api/task/"):
            task_id = self.path.rsplit("/", 1)[-1]
            with TASK_LOCK:
                task = TASKS.get(task_id)
            if task:
                json_response(self, task)
            else:
                json_response(self, {"error": "Task not found"}, status=404)
            return
        json_response(self, {"error": "Not found"}, status=404)

    def do_POST(self):
        if self.path == "/api/refresh":
            payload = read_json_body(self)
            password = str(payload.get("password") or "")
            if password != MANUAL_REFRESH_PASSWORD:
                json_response(self, {"error": "Incorrect refresh password."}, status=403)
                return
            running = get_running_task()
            if running:
                json_response(self, {"task_id": running["id"], "status": "already_running"})
                return
            task_id = start_refresh_task(auto=False)
            json_response(self, {"task_id": task_id, "status": "started"})
            return
        json_response(self, {"error": "Not found"}, status=404)


def choose_port(start: int = DEFAULT_PORT) -> int:
    if os.getenv("PORT"):
        return start
    for port in range(start, start + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((HOST, port))
            except OSError:
                continue
            return port
    raise RuntimeError("No open port found.")


def write_state(port: int) -> None:
    STATE_FILE.write_text(json.dumps({"pid": os.getpid(), "port": port, "url": f"http://{HOST}:{port}/"}, indent=2), encoding="utf-8")
    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")


def main():
    port = choose_port()
    write_state(port)
    server = ThreadingHTTPServer((HOST, port), Handler)
    log(f"Server starting on http://{HOST}:{port}/")
    try:
        server.serve_forever()
    finally:
        try:
            PID_FILE.unlink(missing_ok=True)
            STATE_FILE.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
