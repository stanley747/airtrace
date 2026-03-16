# AirTrace

AirTrace is a Next.js pollution attribution dashboard for Kathmandu, Nepal. It combines live PM2.5 measurements, wind data, and fire-hotspot evidence to estimate how much of the current particulate load is likely local versus imported.

## Current Behavior

- live-only data flow, no seeded fallback snapshot
- OpenAQ station search across Nepal with Kathmandu-aware ranking
- multi-station PM2.5 consensus from several fresh valid stations
- Open-Meteo wind context and 48h wind trail
- Open-Meteo air-quality model context for PM2.5, dust, aerosol optical depth, and AQI agreement
- NASA FIRMS hotspot integration for upwind fire activity evidence
- OSM/Overpass cropland filtering for hotspot classification
- OSM/Overpass source registries for industrial zones and brick-kiln evidence
- optional NOAA HYSPLIT trajectory support when credentials are available
- local SQLite persistence for snapshots, fire evidence, and cropland-cache results
- deterministic attribution engine with:
  - source corridor weighting
  - 24-48h trajectory-lite transport signals
  - 12h/24h PM2.5 persistence
  - seasonal priors
  - confidence scoring based on evidence quality
- API route at `/api/city/kathmandu`

If live upstream data is unavailable or invalid, the app returns no city snapshot instead of showing fabricated fallback data.

## Data Sources

- OpenAQ: live PM2.5 stations and hourly history
- Open-Meteo: current and historical wind data
- Open-Meteo Air Quality: modeled PM2.5, AQI, dust, and aerosol context
- NASA FIRMS: recent fire hotspots in an upwind regional bounding box
- OpenStreetMap / Overpass: cropland proximity checks for hotspot classification
- OpenStreetMap / Overpass: industrial and brick-kiln registry counts around local and upwind corridors
- NOAA READY / HYSPLIT Web API: optional back trajectories when authenticated

## Environment Variables

Create `.env.local` with:

```bash
OPENAQ_API_KEY=...
OPEN_METEO_BASE_URL=https://api.open-meteo.com
FIRMS_MAP_KEY=...
HYSPLIT_AUTH_HEADER_NAME=...
HYSPLIT_AUTH_HEADER_VALUE=...
```

Notes:

- `OPENAQ_API_KEY` is required for live PM2.5 data.
- `FIRMS_MAP_KEY` is optional but recommended. When present, FIRMS hotspots influence upwind fire-activity attribution.
- `OPEN_METEO_BASE_URL` is optional and defaults to `https://api.open-meteo.com`.
- air-quality requests default to `https://air-quality-api.open-meteo.com`.
- HYSPLIT is optional. When `HYSPLIT_AUTH_HEADER_NAME` and `HYSPLIT_AUTH_HEADER_VALUE` are present, the app will attempt true back trajectories through the READY API. Otherwise it falls back to the existing wind-model trajectory logic.

## Local Development

1. Install dependencies:

```bash
npm install
```

2. Start the dev server:

```bash
npm run dev
```

3. Open:

```bash
http://localhost:3000
```

4. Verify the live API route:

```bash
curl -i 'http://localhost:3000/api/city/kathmandu'
```

## API Response

The route returns:

- `city.updatedAt`: UTC timestamp of the selected PM2.5 measurement
- `city.pm25`: consensus PM2.5 from weighted OpenAQ stations
- `city.importedShare` / `city.localShare`
- `city.confidence`
- `city.sources`
- `city.windTrail`
- `city.feed`

If live data cannot be established, the route returns:

- `503 Service Unavailable`
- `city: null`
- structured error details

## Attribution Model

The current attribution engine is deterministic and runs in `lib/airtrace-data.ts`.

It uses:

- weighted Nepal station ranking
- multi-station PM2.5 consensus
- wind corridor alignment
- recent wind trajectory consistency
- PM2.5 persistence and spike behavior
- seasonal priors
- FIRMS fire hotspot density, FRP, and hotspot geometry
- cropland-aligned hotspot filtering
- Open-Meteo modeled PM2.5, dust, and aerosol agreement
- dynamic OSM source registries for brick kilns and industrial zones
- optional HYSPLIT back trajectories

Confidence is based on:

- measurement freshness
- station quality
- station agreement
- hourly evidence coverage
- wind consistency
- trajectory consistency
- source-score separation
- modeled-vs-observed agreement

The app does not require OpenAI or any LLM dependency. Attribution is fully deterministic.

## Useful Commands

```bash
npm run dev
npm run typecheck
```

## Next Improvements

- integrate true back trajectories such as HYSPLIT instead of only trajectory-lite wind inference
- add richer source registries beyond OSM counts, such as explicit urban-emissions inventories
- extend SQLite-backed historical views with source evidence trends and anomaly scoring
- calibrate attribution weights against real-world episodes
