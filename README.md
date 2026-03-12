# AirTrace

AirTrace is a Vercel-friendly Next.js MVP for air pollution source attribution.

## What is included

- app-router Next.js scaffold
- styled landing page and dashboard with live city switching
- live OpenAQ + Open-Meteo fetch layer with fallback snapshots
- real Leaflet map with OpenStreetMap tiles
- API route at `/api/city/[slug]`
- seeded fallback for sparse or failing upstream data

## Local development

1. Install dependencies:

```bash
npm install
```

2. Start the dev server:

```bash
npm run dev
```

3. Open `http://localhost:3000`

## Next implementation steps

- replace mock city data in `lib/airtrace-data.ts` with live fetchers
- enrich the Leaflet map with live source overlays and trajectory data
- add caching and error handling for upstream environmental APIs
- add time-window controls and confidence scoring
