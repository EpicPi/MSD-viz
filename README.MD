# etsy-viz

One-off visualization of an Etsy store's sales (2018–2025) on a world map.

Toggles between a 3D globe and a Mercator projection. Animates through time
week-by-week, in either an **accumulate** mode (dots persist) or a **flash**
mode (dots blink on the week of sale). In-person / storefront-pickup sales
(rows with no shipping address) appear as a small storefront docked to the
side of the map, which pulses when sales happen.

## Layout

```
etsy-viz/
├── orders/                         raw Etsy CSV exports (gitignored)
│   ├── EtsySoldOrders2018.csv
│   ├── …
│   └── consolidated_sales.csv      produced by consolidate_sales.py
├── consolidate_sales.py            merges yearly exports into one CSV
├── geocode_and_export.py           geocodes and emits site/data/sales.json
├── site/                           the static website
│   ├── index.html
│   ├── app.js                      D3-based map + animation
│   ├── style.css
│   └── data/sales.json             generated
└── .cache/                         GeoNames reference data (gitignored)
    ├── cities500.txt
    ├── US.txt, CA.txt, GB.txt, …
```

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Download the GeoNames reference data into `.cache/`:

```bash
mkdir -p .cache
# Global baseline (any city with population ≥ 500)
curl -sL https://download.geonames.org/export/dump/cities500.zip -o /tmp/cities500.zip
unzip -p /tmp/cities500.zip cities500.txt > .cache/cities500.txt

# Per-country dumps (catch smaller towns + admin divisions)
for cc in US CA GB FR AU CZ CH KR ZA BE NL KW JP; do
  curl -sL "https://download.geonames.org/export/dump/${cc}.zip" -o /tmp/${cc}.zip
  unzip -p /tmp/${cc}.zip ${cc}.txt > .cache/${cc}.txt
done
```

The list of per-country dumps is controlled by `COUNTRY_DUMPS` in
`geocode_and_export.py`. Add more country codes there if your sales data
includes places missing from the cities500 baseline.

## Regenerating the data

Every time the raw CSVs in `orders/` change:

```bash
.venv/bin/python consolidate_sales.py      # merge yearly CSVs
.venv/bin/python geocode_and_export.py     # geocode and emit sales.json
```

The geocoder logs misses to `geocode_failures.txt`. Current coverage is ~99.7%
of unique `(city, state, country)` tuples.

## Running locally

```bash
python3 -m http.server 8765 --directory site
```

Open <http://localhost:8765>.

URL parameters for deep-linking: `?week=<N>&proj=globe|mercator&mode=accumulate|flash`.

## Deploying

`site/` is a plain static directory. Drop it onto any static host
(GitHub Pages, Netlify, Cloudflare Pages) — no server-side code, no build step.

## Attribution

City lat/lon data is derived from [GeoNames](https://www.geonames.org/)
(Creative Commons Attribution 4.0).
