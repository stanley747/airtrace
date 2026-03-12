---
name: airtrace
description: Build and iterate on AirTrace, a Vercel-hosted web app for air pollution source attribution. Use this skill when working on features, copy, UX, architecture, or data flows related to PM2.5 source mapping, wind-driven transport, regional attribution, and pollution intelligence dashboards for cities, researchers, NGOs, or governments.
---

# AirTrace

AirTrace is a pollution attribution web app. Its core job is not just reporting pollution levels, but estimating where the pollution likely came from and how it moved.

Use this skill when the task involves:

- product or UX work for pollution source attribution
- implementing dashboards, maps, or reports tied to PM2.5 transport and origin
- defining data flows from public environmental APIs
- shipping or refining a Vercel-hosted MVP
- writing product copy or app copy about transboundary pollution

## Product Frame

AirTrace should answer questions like:

- How bad is the air here right now?
- How much of that pollution is likely local versus imported?
- Which upwind regions are the most probable contributors?
- What changed over the last 24 to 72 hours?

The app is effectively "Google Analytics for air pollution sources."

Key concepts:

- pollution origin vs pollution accumulation
- atmospheric transport modeling
- source attribution
- satellite and sensor fusion
- regional cross-border pollution intelligence

## MVP Scope

Default to a simple Vercel-friendly MVP unless the user asks for a deeper scientific system.

MVP inputs:

- city or map selection
- current timestamp or recent historical window

MVP outputs:

- current PM2.5 and related pollutant readings
- wind direction and speed
- probable source regions
- simple local vs imported attribution estimate
- map overlays for pollution, wind flow, and source hints

Useful first experiences:

1. City detail page with current pollution and attribution summary
2. Interactive map with pollution and wind layers
3. Daily insight card such as "Today's PM2.5 spike in Kathmandu likely originated from agricultural burning upwind."
4. Lightweight report/export view for policy or research users

## Suggested Stack

Prefer this baseline unless the repo already dictates otherwise:

- frontend: Next.js on Vercel
- maps: Mapbox GL JS or Leaflet
- charts: a lightweight React charting library
- backend: Next.js route handlers for API orchestration
- heavier modeling: Python microservice only if the user explicitly needs HYSPLIT-like workflows or scientific compute that does not fit Vercel well

Bias toward keeping the first version inside the Next.js app so deployment stays simple.

## Data Sources

Start with free public data unless the user explicitly wants paid APIs.

Recommended sources:

- OpenAQ for PM2.5, PM10, NO2, O3, CO, and historical air-quality observations
- Open-Meteo for wind, pressure, and weather context
- NASA datasets for fires, smoke, and satellite-derived environmental signals
- Copernicus or Sentinel data when satellite overlays are needed

Treat NOAA HYSPLIT as an advanced step, not a day-one requirement. For MVP work, a transparent heuristic using wind vectors, upwind regions, and fire or emissions indicators is acceptable if clearly labeled as an estimate.

## Attribution Logic

Do not overclaim scientific certainty. Present attribution as probabilistic unless the user explicitly asks for rigorous scientific reporting and the system has the evidence to support it.

Good framing:

- "likely source regions"
- "estimated imported contribution"
- "upwind contributors over the last 24 hours"
- "confidence: low, medium, high"

Avoid framing like:

- "proved source"
- "definitive polluter"

For MVP implementations, a reasonable hierarchy is:

1. read current pollutant data
2. read recent wind patterns
3. identify upwind regions in the relevant time window
4. overlay known indicators such as fires or industrial belts when available
5. produce a ranked list of probable sources with confidence notes

## UX Direction

Default UX priorities:

- dashboard-first product experience
- clear map-first workspace
- dense information layout that feels like an operator console
- fixed-viewport desktop app when the user asks for terminal-style UI
- concise plain-language summaries
- prominent health severity state for current air quality
- obvious distinction between local emissions and imported pollution
- mobile-friendly dashboards

Avoid defaulting to a marketing landing page. AirTrace should feel like an operational tool first.
When asked for a Bloomberg-terminal style UI, prioritize compact panels, tabular data, dense metrics, dark console visuals, and minimal decorative whitespace.

Important interface modules:

- utility header with city controls and timestamps
- immediate above-the-fold data and map visibility
- map with toggles for PM2.5, wind, and source regions
- current metrics row for PM2.5, imported share, local share, and confidence
- timeline for the last 24 to 72 hours
- evidence panel explaining why the app made a source estimate

When writing copy, keep the message practical:

- what happened
- where it likely came from
- how confident the model is
- what the user should do next

## Vercel Delivery Rules

This app is hosted on Vercel, so optimize for:

- serverless-friendly API routes
- edge-safe read-heavy requests where appropriate
- cached upstream API fetches
- minimal operational complexity
- environment-variable based configuration

Prefer:

- ISR or cached fetches for semi-static environmental data
- route handlers for proxying and normalization
- client components only where map interactivity requires them

Avoid introducing heavy always-on infrastructure unless the user explicitly asks for it.

If a modeling step needs long-running jobs, call that out early and suggest either:

- a separate Python service
- scheduled preprocessing outside the request path

## Build Priorities

When deciding what to implement first, prefer this order:

1. city search or selection
2. current air-quality data
3. wind context
4. source-attribution summary
5. interactive map layers
6. historical trends
7. downloadable reports

## Audience Modes

AirTrace may serve several audiences. Match the output to the audience named in the task.

Citizen mode:

- alerts
- short explanations
- health guidance

Research or NGO mode:

- methodology visibility
- exportable evidence
- regional comparisons

Government or policy mode:

- local vs imported split
- recurring seasonal patterns
- hotspot reporting
- supporting narrative for cross-border policy discussions

## Implementation Heuristics

When building features:

- prefer explainable heuristics over opaque ML for the first release
- keep all scientific claims traceable to visible inputs
- show timestamps and source provenance
- expose uncertainty where inputs are incomplete
- make the app useful even if one upstream data source is temporarily unavailable
- if using an LLM, use it for attribution explanation and structured hypotheses grounded in measured data; do not let it invent unsupported certainty

If data quality is weak, degrade gracefully:

- show pollution readings without attribution
- show attribution without satellite overlays
- show last-updated timestamps and partial-data notices

## Writing and Messaging

Use concise, confident language. Avoid climate-tech buzzword overload.

Good phrases:

- "Pollution likely arrived from upwind regions."
- "Air quality is poor, and transport conditions suggest imported PM2.5."
- "Wind patterns and regional fire activity point to probable external contributors."

Avoid:

- "AI-powered environmental revolution"
- "precise blame engine"

## When Extending Beyond MVP

Only expand into these areas when the task requires them:

- isotope or chemical fingerprint workflows
- legal-grade evidence generation
- industrial source registries
- automated seasonal anomaly detection
- cross-border policy reporting
- mobile push alerts

When those come up, keep the distinction clear between:

- scientific inference
- operational product UX
- legal or policy evidence
