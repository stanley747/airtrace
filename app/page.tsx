import { AttributionMap } from "@/components/attribution-map";
import { AutoRefresh } from "@/components/auto-refresh";
import { TimelinePlayer } from "@/components/timeline-player";
import {
  getHistoricalTrend,
  getSnapshot,
  SnapshotError,
  type FireEvidence,
  type ModelEvidence,
  type RegistryEvidence,
  type TrajectoryEvidence
} from "@/lib/airtrace-data";

export const dynamic = "force-dynamic";

function formatShare(value: number) {
  return `${value}%`;
}

function formatKathmanduTime(value: string) {
  return `${new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kathmandu",
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  })
    .format(new Date(value))
    .replace(",", "")} NPT`;
}

function formatTrendDay(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kathmandu",
    month: "short",
    day: "2-digit"
  }).format(new Date(`${value}T00:00:00+05:45`));
}

function formatScore(value: number) {
  return `${Math.round(value)}%`;
}

function pluralize(count: number, singular: string, plural = `${singular}s`) {
  return `${count} ${count === 1 ? singular : plural}`;
}

function formatModelSourceSummary(modelEvidence: ModelEvidence) {
  if (modelEvidence.modeledPm25 === null) {
    return "Wind-only support; secondary air-quality model unavailable";
  }

  return "Wind, modeled PM2.5, dust, and aerosol support";
}

function formatFireSourceSummary(fireEvidence: FireEvidence) {
  if (fireEvidence.hotspotCount === 0) {
    return "No active hotspots in the current upwind corridor";
  }

  if (fireEvidence.croplandMatchCount === 0) {
    return `${pluralize(fireEvidence.hotspotCount, "hotspot")} detected / no cropland alignment yet`;
  }

  return `${pluralize(fireEvidence.hotspotCount, "hotspot")} / ${pluralize(
    fireEvidence.croplandMatchCount,
    "cropland-aligned match",
    "cropland-aligned matches"
  )}`;
}

function formatTrajectorySourceSummary(trajectoryEvidence: TrajectoryEvidence) {
  if (trajectoryEvidence.provider === "hysplit") {
    return "NOAA HYSPLIT back trajectories";
  }

  return "Fallback transport estimate from live wind history";
}

function formatRegistrySourceSummary(registryEvidence: RegistryEvidence) {
  const localMatches =
    registryEvidence.localKilnCount + registryEvidence.localIndustrialCount;
  const upwindMatches =
    registryEvidence.upwindKilnCount + registryEvidence.upwindIndustrialCount;

  if (localMatches + upwindMatches === 0) {
    return "No mapped kiln or industrial matches along the current path";
  }

  const parts = [];

  if (localMatches > 0) {
    parts.push(`${pluralize(localMatches, "local site")}`);
  }

  if (upwindMatches > 0) {
    parts.push(`${pluralize(upwindMatches, "upwind site")}`);
  }

  return parts.join(" / ");
}

function formatSqliteSourceSummary(hasTrend: boolean) {
  if (hasTrend) {
    return "Snapshots, fire evidence, signal cache, 7-day trend";
  }

  return "Snapshots and signal cache; the 7-day trend is still filling in";
}

function formatTrajectoryProviderShort(trajectoryEvidence: TrajectoryEvidence) {
  if (trajectoryEvidence.provider === "hysplit") {
    return "HYSPLIT trajectory";
  }

  return "Wind transport fallback";
}

type HomePageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function HomePage({ searchParams }: HomePageProps) {
  await searchParams;
  let selectedCity;
  let errorMessage: string | null = null;
  let errorLabel: string | null = null;

  try {
    selectedCity = await getSnapshot();
  } catch (error) {
    if (error instanceof SnapshotError) {
      errorMessage = error.message;
      errorLabel = error.causeLabel;
    } else if (error instanceof Error) {
      errorMessage = error.message;
    } else {
      errorMessage = "Unknown live data failure";
    }
  }

  if (!selectedCity) {
    return (
      <main className="site-shell">
        <AutoRefresh />
        <section className="empty-panel">
          <span className="eyebrow">AirTrace</span>
          <h1>No live data available</h1>
          {errorLabel ? <p>{errorLabel}</p> : null}
          {errorMessage ? <p>{errorMessage}</p> : null}
        </section>
      </main>
    );
  }

  const navItems = [
    { id: "overview", label: "Overview" },
    { id: "how-it-works", label: "How it works" },
    { id: "snapshot", label: "Snapshot" },
    { id: "attribution", label: "Attribution" },
    { id: "playback", label: "Playback" },
    { id: "history", label: "History" },
    { id: "evidence", label: "Evidence" }
  ];
  const sortedSources = [...selectedCity.sources].sort((a, b) => b.share - a.share);
  const strongestSource = sortedSources[0];
  const strongestWind = selectedCity.windTrail[0];
  const trend = await getHistoricalTrend(30, selectedCity.stationSummary);
  const hasTrendHistory = trend.length > 0;
  const topStations = selectedCity.stationEvidence.slice(0, 4);
  const confidenceRows = [
    { label: "Overall", value: selectedCity.confidenceBreakdown.overall },
    { label: "Freshness", value: selectedCity.confidenceBreakdown.freshness },
    { label: "Station agreement", value: selectedCity.confidenceBreakdown.stationAgreement },
    { label: "Model agreement", value: selectedCity.confidenceBreakdown.modelAgreement },
    { label: "Transport", value: selectedCity.confidenceBreakdown.transport },
    { label: "History", value: selectedCity.confidenceBreakdown.history },
    { label: "Hotspot support", value: selectedCity.confidenceBreakdown.hotspotSupport }
  ];
  const howItWorks = [
    {
      title: "Observe the live baseline",
      detail: `OpenAQ readings from ${selectedCity.stationEvidence.length} fresh Nepal stations are combined into a current PM2.5 estimate for Kathmandu.`
    },
    {
      title: "Layer transport and source evidence",
      detail: `${selectedCity.trajectoryEvidence.provider} transport, ${selectedCity.fireEvidence.hotspotCount} FIRMS hotspots, and modeled aerosol support are compared before contributors are ranked.`
    },
    {
      title: "Store and replay the result",
      detail: "Each snapshot is written to SQLite, and the history view pulls a 30-day upstream backfill so the timeline is not limited to whatever this browser session captured."
    }
  ];

  return (
    <main className="site-shell">
      <AutoRefresh />
      <div className="site-layout">
        <aside className="site-sidebar">
          <div className="site-brand">
            <div className="brand-mark">A</div>
            <div className="brand-copy">
              <strong>airtrace</strong>
              <span>Live pollution attribution</span>
            </div>
          </div>

          <nav className="site-nav" aria-label="Page sections">
            <div className="site-nav-section">
              <span className="site-nav-heading">Navigate</span>
              {navItems.map((item) => (
                <a key={item.id} className="site-nav-link" href={`#${item.id}`}>
                  {item.label}
                </a>
              ))}
            </div>
            <div className="site-nav-section">
              <span className="site-nav-heading">Live now</span>
              <div className="sidebar-stat-list">
                <div className="sidebar-stat">
                  <span className="sidebar-stat-label">Location</span>
                  <strong>{selectedCity.city}, Nepal</strong>
                </div>
                <div className="sidebar-stat">
                  <span className="sidebar-stat-label">PM2.5</span>
                  <strong>{selectedCity.pm25} ug/m3</strong>
                </div>
                <div className="sidebar-stat">
                  <span className="sidebar-stat-label">AQI</span>
                  <strong>{selectedCity.aqiCategory}</strong>
                </div>
              </div>
            </div>
          </nav>

          <div className="site-sidebar-foot">
            <span>{selectedCity.dataMode === "live" ? "Live feed" : "Offline"}</span>
            <span>{formatTrajectoryProviderShort(selectedCity.trajectoryEvidence)}</span>
          </div>
        </aside>

        <div className="site-main">
          <section id="overview" className="hero-block">
            <div className="hero-kicker-row">
              <div className="live-kicker">
                <span className="live-dot" />
                Live in Kathmandu. Deterministic attribution refreshed every 10 minutes.
              </div>
              <code className="hero-code">/api/city/kathmandu</code>
            </div>

            <div className="hero-grid">
              <div className="hero-copy">
                <h1 className="hero-title">
                  <span className="hero-highlight">Track live pollution.</span>
                  <br />
                  See what moved it.
                </h1>
                <p className="hero-description">
                  AirTrace combines observed PM2.5, wind transport, satellite hotspots,
                  modeled aerosol support, and local source registries to estimate where
                  Kathmandu&apos;s particulate load is coming from right now.
                </p>
                <div className="hero-summary">
                  <span className="eyebrow">Primary call</span>
                  <p>{selectedCity.summary}</p>
                </div>
              </div>

              <div className="hero-meta">
                <div className="meta-item">
                  <span className="eyebrow">PM2.5 updated</span>
                  <strong>{formatKathmanduTime(selectedCity.updatedAt)}</strong>
                </div>
                <div className="meta-item">
                  <span className="eyebrow">Model refreshed</span>
                  <strong>{formatKathmanduTime(selectedCity.generatedAt)}</strong>
                </div>
                <div className="meta-item">
                  <span className="eyebrow">Confidence</span>
                  <strong>{selectedCity.confidence}</strong>
                </div>
                <div className="meta-item">
                  <span className="eyebrow">Dominant source</span>
                  <strong>{strongestSource?.name ?? "N/A"}</strong>
                </div>
              </div>
            </div>

            <div className="browser-card">
              <div className="browser-top">
                <div className="browser-dots">
                  <span />
                  <span />
                  <span />
                </div>
                <div className="browser-address">Live attribution preview</div>
              </div>
              <div className="browser-body">
                <div className="browser-map">
                  <AttributionMap city={selectedCity} />
                </div>
                <div className="preview-rail">
                  <div className="preview-card">
                    <span className="eyebrow">PM2.5</span>
                    <strong className="preview-value">{selectedCity.pm25}</strong>
                    <span>{selectedCity.category}</span>
                  </div>
                  <div className="preview-card">
                    <span className="eyebrow">Imported / local</span>
                    <strong className="preview-value">
                      {formatShare(selectedCity.importedShare)} /{" "}
                      {formatShare(selectedCity.localShare)}
                    </strong>
                    <span>Estimated share of current load</span>
                  </div>
                  <div className="preview-card">
                    <span className="eyebrow">Wind</span>
                    <strong className="preview-value">
                      {strongestWind?.direction ?? "N/A"} {strongestWind?.speedKph ?? 0}
                    </strong>
                    <span>Current transport signal</span>
                  </div>
                  <div className="preview-card">
                    <span className="eyebrow">Model agreement</span>
                    <strong className="preview-value">
                      {selectedCity.modelEvidence.agreement}
                    </strong>
                    <span>
                      Modeled PM2.5 {selectedCity.modelEvidence.modeledPm25 ?? "N/A"} ug/m3
                    </span>
                  </div>
                </div>
              </div>
            </div>
          </section>

          <section id="how-it-works" className="content-section content-section-tight">
            <div className="section-topline">
              <div>
                <span className="eyebrow">How it works</span>
                <h2 className="section-title">A small evidence stack, refreshed continuously</h2>
              </div>
              <p className="section-copy">
                The site stays minimal, but the attribution model still pulls from
                observations, transport, hotspots, and stored history before it shows a
                claim.
              </p>
            </div>
            <div className="steps-card">
              <div className="step-list">
                {howItWorks.map((step, index) => (
                  <div key={step.title} className="step-row">
                    <span className="step-index">{index + 1}</span>
                    <div className="step-copy">
                      <strong>{step.title}</strong>
                      <p>{step.detail}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </section>

          <section id="snapshot" className="content-section">
            <div className="section-topline">
              <div>
                <span className="eyebrow">Current snapshot</span>
                <h2 className="section-title">The main numbers at a glance</h2>
              </div>
              <p className="section-copy">
                A compact readout of current particulate load, health severity, source
                balance, and live transport context.
              </p>
            </div>
            <div className="stat-grid">
              <article className="stat-card">
                <span className="eyebrow">PM2.5</span>
                <strong className="stat-value">{selectedCity.pm25}</strong>
                <span className="stat-note">ug/m3 current load</span>
              </article>
              <article className="stat-card">
                <span className="eyebrow">AQI</span>
                <strong className="stat-value">{selectedCity.aqi}</strong>
                <span className="stat-note">{selectedCity.aqiCategory}</span>
              </article>
              <article className="stat-card">
                <span className="eyebrow">Outside Nepal</span>
                <strong className="stat-value">{formatShare(selectedCity.importedShare)}</strong>
                <span className="stat-note">Estimated imported share</span>
              </article>
              <article className="stat-card">
                <span className="eyebrow">Within Nepal</span>
                <strong className="stat-value">{formatShare(selectedCity.localShare)}</strong>
                <span className="stat-note">Estimated local share</span>
              </article>
              <article className="stat-card">
                <span className="eyebrow">Current wind</span>
                <strong className="stat-value">
                  {strongestWind?.speedKph ?? 0} kph
                </strong>
                <span className="stat-note">{strongestWind?.direction ?? "N/A"}</span>
              </article>
            </div>
          </section>

          <section id="attribution" className="content-section">
            <div className="section-topline">
              <div>
                <span className="eyebrow">Attribution</span>
                <h2 className="section-title">Ranked contributors</h2>
              </div>
              <p className="section-copy">
                Each contributor is generated from the current evidence graph, not from a
                hardcoded source list.
              </p>
            </div>
            <div className="dual-grid">
              <div className="panel-card">
                <div className="panel-card-head">
                  <span className="eyebrow">Source attribution</span>
                  <span className="panel-inline-value">Live ranking</span>
                </div>
                <div className="source-list">
                  {sortedSources.map((source) => (
                    <div key={source.name} className="source-item">
                      <div className="source-head">
                        <strong>{source.name}</strong>
                        <span className="source-share">{formatShare(source.share)}</span>
                      </div>
                      <p className="source-evidence">{source.evidence}</p>
                      <div className="source-bar">
                        <div
                          className="source-bar-fill"
                          style={{ width: `${source.share}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="panel-card">
                <div className="panel-card-head">
                  <span className="eyebrow">Supporting signals</span>
                  <span className="panel-inline-value">Observed + modeled</span>
                </div>
                <div className="metric-list signal-list">
                  <div className="metric-row">
                    <span>Model support</span>
                    <strong>
                      {selectedCity.modelEvidence.modeledPm25 ?? "N/A"} ug/m3 /{" "}
                      {selectedCity.modelEvidence.agreement}
                    </strong>
                  </div>
                  <div className="metric-row">
                    <span>Fire evidence</span>
                    <strong>{formatFireSourceSummary(selectedCity.fireEvidence)}</strong>
                  </div>
                  <div className="metric-row">
                    <span>Trajectory</span>
                    <strong>{formatTrajectorySourceSummary(selectedCity.trajectoryEvidence)}</strong>
                  </div>
                  <div className="metric-row">
                    <span>Registry support</span>
                    <strong>{formatRegistrySourceSummary(selectedCity.registryEvidence)}</strong>
                  </div>
                </div>
              </div>
            </div>
          </section>

          <section id="playback" className="content-section">
            <div className="section-topline">
              <div>
                <span className="eyebrow">Playback</span>
                <h2 className="section-title">Review the last 24 hours</h2>
              </div>
              <p className="section-copy">
                Hourly PM2.5, imported/local split, and dominant source are replayed from
                the live attribution model.
              </p>
            </div>
            <div className="dual-grid">
              <div className="panel-card panel-card-wide">
                <TimelinePlayer
                  key={selectedCity.generatedAt}
                  cityName={selectedCity.city}
                  coordinates={selectedCity.coordinates}
                  frames={selectedCity.timeline24h}
                />
              </div>
              <div className="panel-card">
                <div className="panel-card-head">
                  <span className="eyebrow">Confidence breakdown</span>
                  <span className="panel-inline-value">{selectedCity.confidence}</span>
                </div>
                <div className="confidence-list">
                  {confidenceRows.map((row) => (
                    <div key={row.label} className="confidence-row">
                      <div className="confidence-main">
                        <span>{row.label}</span>
                        <strong>{formatScore(row.value)}</strong>
                      </div>
                      <div className="confidence-meter">
                        <div
                          className="confidence-fill"
                          style={{ width: `${row.value}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section id="history" className="content-section">
            <div className="section-topline">
              <div>
                <span className="eyebrow">History</span>
                <h2 className="section-title">Thirty-day trend from upstream history</h2>
              </div>
              <p className="section-copy">
                One daily row is rebuilt from the last 30 days of observed PM2.5, wind,
                hotspot, and modeled support data, with SQLite used as cache and fallback.
                {trend.length > 0
                  ? ` Currently showing ${trend.length} day${trend.length === 1 ? "" : "s"} with usable observations.`
                  : ""}
              </p>
            </div>
            <div className="panel-card">
              <div className="trend-head">
                <span>Day</span>
                <span>PM2.5</span>
                <span>Import / local</span>
                <span>Hotspots</span>
                <span>Cropland</span>
                <span>Dominant source</span>
              </div>
              <div className="trend-list">
                {trend.length > 0 ? (
                  trend.map((entry) => (
                    <div key={entry.sourceUpdatedAt} className="trend-row">
                      <span>{formatTrendDay(entry.localDay)}</span>
                      <strong>{Math.round(entry.pm25)}</strong>
                      <span>
                        {entry.importedShare}% / {entry.localShare}%
                      </span>
                      <span>{entry.hotspotCount}</span>
                      <span>{entry.croplandMatchCount}</span>
                      <strong>{entry.dominantSource}</strong>
                    </div>
                  ))
                ) : (
                  <p className="trend-empty">
                    Thirty-day history is unavailable right now, so the panel is waiting on
                    upstream backfill data.
                  </p>
                )}
              </div>
            </div>
          </section>

          <section id="evidence" className="content-section">
            <div className="section-topline">
              <div>
                <span className="eyebrow">Evidence</span>
                <h2 className="section-title">What the model is using right now</h2>
              </div>
              <p className="section-copy">
                These inputs are the current verification stack behind the attribution
                model.
              </p>
            </div>
            <div className="dual-grid">
              <div className="panel-card">
                <div className="panel-card-head">
                  <span className="eyebrow">Station evidence</span>
                  <span className="panel-inline-value">
                    {selectedCity.stationEvidence.length} stations
                  </span>
                </div>
                <div className="station-list">
                  {topStations.map((station) => (
                    <div key={station.locationId} className="station-row">
                      <div className="station-main">
                        <strong>{station.label}</strong>
                        <span>
                          PM2.5 {station.pm25} ug/m3 {" / "} {station.distanceKm} km {" / "}
                          weight {station.weight}%
                        </span>
                      </div>
                      <div className="station-side">
                        <span>fresh {station.freshnessHours}h</span>
                        <span>quality {station.stationQuality}%</span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="panel-card">
                <div className="panel-card-head">
                  <span className="eyebrow">Data sources</span>
                  <span className="panel-inline-value">Live stack</span>
                </div>
                <div className="data-list">
                  <div className="metric-row">
                    <span>OpenAQ</span>
                    <strong>Observed PM2.5 + hourly history</strong>
                  </div>
                  <div className="metric-row">
                    <span>Open-Meteo</span>
                    <strong>{formatModelSourceSummary(selectedCity.modelEvidence)}</strong>
                  </div>
                  <div className="metric-row">
                    <span>NASA FIRMS</span>
                    <strong>{formatFireSourceSummary(selectedCity.fireEvidence)}</strong>
                  </div>
                  <div className="metric-row">
                    <span>Trajectory</span>
                    <strong>
                      {formatTrajectorySourceSummary(selectedCity.trajectoryEvidence)}
                    </strong>
                  </div>
                  <div className="metric-row">
                    <span>Registry</span>
                    <strong>{formatRegistrySourceSummary(selectedCity.registryEvidence)}</strong>
                  </div>
                  <div className="metric-row">
                    <span>SQLite</span>
                    <strong>{formatSqliteSourceSummary(hasTrendHistory)}</strong>
                  </div>
                </div>
              </div>
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
