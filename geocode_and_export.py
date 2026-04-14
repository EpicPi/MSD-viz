"""Geocode consolidated Etsy sales and emit site/data/sales.json.

Partitions rows into:
  - in_person: all of city/state/country blank (counted as "store pickup" events)
  - locatable: everything else, geocoded against GeoNames cities500 dump

Outputs one aggregated record per unique (lat, lon) with a sorted dates array,
plus an in_person bucket.
"""

import html
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

import geonamescache
import pandas as pd

REPO = Path(__file__).parent
SALES_CSV = REPO / "orders" / "consolidated_sales.csv"
CACHE_DIR = REPO / ".cache"
CITIES_TXT = CACHE_DIR / "cities500.txt"
# Per-country dumps contain small populated places that cities500 excludes.
# Keep the list focused on countries where we actually have failures.
COUNTRY_DUMPS = ["US", "CA", "GB", "FR", "AU", "CZ", "CH", "KR", "ZA", "BE", "NL", "KW", "JP"]
OUT_JSON = REPO / "site" / "data" / "sales.json"
FAILURES_TXT = REPO / "geocode_failures.txt"

# GeoNames country name → ISO-2 covers most cases; these two don't match by exact name.
COUNTRY_ISO_OVERRIDES = {
    "Czech Republic": "CZ",
    "Türkiye": "TR",
}


def ascii_fold(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


_PUNCT_RE = re.compile(r"[.'\u2019`]")
_NONALPHA_RE = re.compile(r"[^a-z0-9]")


def norm_name(s: str) -> str:
    # strip punctuation so "O'Fallon" and "OFallon" collide, then fold + lower
    folded = ascii_fold(html.unescape(s))
    return _PUNCT_RE.sub("", folded).strip().lower()


def compact_name(s: str) -> str:
    # All letters+digits, no spaces/punct: "O Fallon" and "O'Fallon" both → "ofallon"
    return _NONALPHA_RE.sub("", norm_name(s))


# Common abbreviations in Etsy shipping address city names.
PREFIX_EXPANSIONS = [
    ("n ", "north "), ("s ", "south "), ("e ", "east "), ("w ", "west "),
    ("mt ", "mount "), ("st ", "saint "), ("ft ", "fort "),
    ("mc ", "mc"),  # "Mc Kittrick" → "Mckittrick"; index is case-folded so this collides with "McKittrick"
]
SUFFIX_STRIPS = [" township", " twp", " city"]
# Common US shipping-address abbreviations at the end of a city name.
SUFFIX_EXPANSIONS = [
    (" sta", " station"),
    (" boro", " borough"),
    (" cty", " city"),
    (" hts", " heights"),
    (" jct", " junction"),
    (" pt", " point"),
]


def name_candidates(raw: str) -> list[str]:
    """Yield normalized search-key variants for a free-text city name.

    Handles common Etsy data quirks: abbreviated directional prefixes, township
    suffixes, "neighborhood, city" strings, and trailing postal suffixes like
    "Dublin 15" or "København V".
    """
    seen = set()
    out = []

    def push(x: str):
        k = norm_name(x)
        if k and k not in seen:
            seen.add(k)
            out.append(k)

    push(raw)
    # also search by the compact (no-whitespace) form
    ckey = compact_name(raw)
    if ckey and ckey not in seen:
        seen.add(ckey)
        out.append(ckey)

    # split on comma, hyphen-with-spaces, open-paren
    for sep in [",", " - ", "("]:
        if sep in raw:
            for part in raw.split(sep):
                push(part.rstrip(") "))

    # strip township-style suffixes
    low = norm_name(raw)
    for suf in SUFFIX_STRIPS:
        if low.endswith(suf):
            push(low[: -len(suf)])

    # expand common prefixes
    for short, full in PREFIX_EXPANSIONS:
        if low.startswith(short):
            push(full + low[len(short):])

    # expand common suffix abbreviations
    for short, full in SUFFIX_EXPANSIONS:
        if low.endswith(short):
            push(low[: -len(short)] + full)

    # trailing token that's a number or single letter (postal remnant): drop it
    parts = low.split()
    if len(parts) > 1 and (parts[-1].isdigit() or len(parts[-1]) == 1):
        push(" ".join(parts[:-1]))

    return out


def load_country_iso_map() -> dict[str, str]:
    gc = geonamescache.GeonamesCache()
    m = {c["name"]: c["iso"] for c in gc.get_countries().values()}
    m.update(COUNTRY_ISO_OVERRIDES)
    return m


GEONAMES_COLS = [
    "geonameid", "name", "asciiname", "alternatenames",
    "latitude", "longitude", "fclass", "fcode",
    "country", "cc2", "admin1", "admin2", "admin3", "admin4",
    "population", "elevation", "dem", "timezone", "modified",
]


def read_geonames(path: Path, geocode_candidates_only: bool = False) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep="\t", header=None, names=GEONAMES_COLS,
        dtype=str, keep_default_na=False, na_filter=False,
    )
    if geocode_candidates_only:
        # Populated places (P.*) plus local admin divisions (ADM3/ADM4) so we
        # also match townships, boroughs, and urban wards.
        keep = (df["fclass"] == "P") | (df["fcode"].isin(["ADM3", "ADM4"]))
        df = df[keep]
    df = df.copy()
    df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0).astype(int)
    df["latitude"] = pd.to_numeric(df["latitude"])
    df["longitude"] = pd.to_numeric(df["longitude"])
    return df


def load_all_cities() -> pd.DataFrame:
    if not CITIES_TXT.exists():
        sys.exit(
            f"Missing {CITIES_TXT}. Download cities500.zip from "
            "https://download.geonames.org/export/dump/ and extract it there."
        )

    frames = [read_geonames(CITIES_TXT)]
    print(f"  cities500: {len(frames[0])} cities")

    for cc in COUNTRY_DUMPS:
        p = CACHE_DIR / f"{cc}.txt"
        if not p.exists():
            print(f"  (skipping {cc}: {p.name} not found)", file=sys.stderr)
            continue
        extra = read_geonames(p, geocode_candidates_only=True)
        print(f"  {cc}: {len(extra)} populated places")
        frames.append(extra)

    combined = pd.concat(frames, ignore_index=True)
    # Dedupe by geonameid so cities500 + country dumps don't double-index the same place
    combined = combined.drop_duplicates(subset=["geonameid"])
    return combined


def build_city_index(cities: pd.DataFrame) -> dict:
    """Key: (ascii_lower_name, iso2, admin1_upper_or_empty) → list of (pop, lat, lon).

    Every city is indexed under its primary name AND each alternate name, and
    additionally under an admin1='' key so we can fall back when state is unknown
    or wrong. Within each key we keep all entries sorted by population descending
    so the lookup picks the largest matching city.
    """
    idx: dict[tuple[str, str, str], list[tuple[int, float, float]]] = defaultdict(list)

    for row in cities.itertuples(index=False):
        iso = (row.country or "").upper()
        admin1 = (row.admin1 or "").upper()
        pop = int(row.population)
        lat = float(row.latitude)
        lon = float(row.longitude)

        names = {row.name, row.asciiname}
        if row.alternatenames:
            names.update(row.alternatenames.split(","))

        for n in names:
            if not n:
                continue
            nkey = norm_name(n)
            ckey = compact_name(n)
            for k in {nkey, ckey}:
                if not k:
                    continue
                idx[(k, iso, admin1)].append((pop, lat, lon))
                idx[(k, iso, "")].append((pop, lat, lon))

    # sort each list by population desc
    for k in idx:
        idx[k].sort(key=lambda t: -t[0])
    return idx


def lookup(idx: dict, city: str, iso: str, admin1: str) -> tuple[float, float] | None:
    if not city or not iso:
        return None
    iso = iso.upper()
    admin1 = (admin1 or "").upper()
    for key_city in name_candidates(city):
        hits = idx.get((key_city, iso, admin1))
        if not hits and admin1:
            hits = idx.get((key_city, iso, ""))
        if hits:
            _, lat, lon = hits[0]
            return (lat, lon)
    return None


def main() -> int:
    if not SALES_CSV.exists():
        sys.exit(f"Missing {SALES_CSV}")

    sales = pd.read_csv(SALES_CSV, dtype=str).fillna("")
    print(f"Loaded {len(sales)} sale rows")

    blank_mask = (sales["city"] == "") & (sales["state"] == "") & (sales["country"] == "")
    in_person = sales[blank_mask].copy()
    locatable = sales[~blank_mask].copy()
    print(f"  in-person (all blank): {len(in_person)}")
    print(f"  locatable:             {len(locatable)}")

    country_to_iso = load_country_iso_map()
    locatable["iso"] = locatable["country"].map(country_to_iso).fillna("")

    missing_country = locatable[locatable["iso"] == ""]["country"].unique().tolist()
    if missing_country:
        print(f"  WARN: countries with no ISO mapping: {missing_country}", file=sys.stderr)

    print("Loading GeoNames data…")
    cities = load_all_cities()
    print(f"  {len(cities)} total places after dedupe")
    print("Building name index…")
    idx = build_city_index(cities)

    unique = (
        locatable[["city", "state", "country", "iso"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    print(f"Unique (city,state,country) tuples: {len(unique)}")

    coords: dict[tuple[str, str, str], tuple[float, float]] = {}
    misses: list[tuple[str, str, str]] = []
    for row in unique.itertuples(index=False):
        iso, state = row.iso, row.state
        # Puerto Rico addresses arrive as country=US, state=PR; GeoNames files them under iso=PR.
        if iso == "US" and state == "PR":
            iso, state = "PR", ""
        coord = lookup(idx, row.city, iso, state)
        key = (row.city, row.state, row.country)
        if coord:
            coords[key] = coord
        else:
            misses.append(key)

    hit_rate = 1 - len(misses) / max(len(unique), 1)
    print(f"Geocode hit rate: {hit_rate:.1%} ({len(unique) - len(misses)}/{len(unique)})")

    if misses:
        with FAILURES_TXT.open("w") as f:
            f.write(f"# {len(misses)} unique (city, state, country) tuples could not be geocoded\n")
            for c, s, co in sorted(misses):
                n = ((locatable["city"] == c) & (locatable["state"] == s) & (locatable["country"] == co)).sum()
                f.write(f"{n}\t{c}\t{s}\t{co}\n")
        print(f"Wrote {FAILURES_TXT.name}")

    # Join coords back onto locatable
    def coord_of(r):
        return coords.get((r["city"], r["state"], r["country"]))

    locatable["coord"] = locatable.apply(coord_of, axis=1)
    geocoded = locatable[locatable["coord"].notna()].copy()
    dropped = len(locatable) - len(geocoded)
    if dropped:
        print(f"Dropping {dropped} sale rows with un-geocodable locations")
    geocoded["lat"] = geocoded["coord"].map(lambda t: t[0])
    geocoded["lon"] = geocoded["coord"].map(lambda t: t[1])

    # Aggregate by (lat, lon); attach representative city/state/country (most common)
    locations = []
    for (lat, lon), group in geocoded.groupby(["lat", "lon"], sort=False):
        rep = group.iloc[0]
        dates = sorted(group["sale_date"].tolist())
        locations.append({
            "lat": round(float(lat), 4),
            "lon": round(float(lon), 4),
            "city": rep["city"],
            "state": rep["state"],
            "country": rep["country"],
            "count": len(dates),
            "dates": dates,
        })

    # sort locations by count desc so the frontend's rendering order is stable
    locations.sort(key=lambda d: -d["count"])

    out = {
        "locations": locations,
        "in_person": {
            "count": int(len(in_person)),
            "dates": sorted(in_person["sale_date"].tolist()),
        },
        "meta": {
            "total_sales": int(len(sales)),
            "geocoded_sales": int(len(geocoded)),
            "in_person_sales": int(len(in_person)),
            "dropped_sales": int(dropped),
            "unique_locations": len(locations),
            "date_min": sales["sale_date"].min(),
            "date_max": sales["sale_date"].max(),
        },
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w") as f:
        json.dump(out, f, separators=(",", ":"))

    size_kb = OUT_JSON.stat().st_size / 1024
    print(f"\nWrote {OUT_JSON} ({size_kb:.1f} KB)")
    print(f"  {len(locations)} unique map locations, {len(in_person)} in-person sales")
    return 0


if __name__ == "__main__":
    sys.exit(main())
