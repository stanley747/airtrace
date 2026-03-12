import { AttributionMap } from "@/components/attribution-map";
import { getSnapshot } from "@/lib/airtrace-data";

export const dynamic = "force-dynamic";

function formatShare(value: number) {
  return `${value}%`;
}

function formatTimestamp(value: string) {
  return new Date(value).toUTCString().replace(" GMT", " UTC");
}

function formatFeedTimestamp(value: string) {
  const date = new Date(value);
  const hour = `${date.getUTCHours()}`.padStart(2, "0");
  const month = `${date.getUTCMonth() + 1}`.padStart(2, "0");
  const day = `${date.getUTCDate()}`.padStart(2, "0");
  return `${month}/${day} ${hour}:00 UTC`;
}

type HomePageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function HomePage({ searchParams }: HomePageProps) {
  await searchParams;
  const selectedCity = await getSnapshot();
  const strongestSource = selectedCity.sources[0];
  const strongestWind = selectedCity.windTrail[0];

  return (
    <main className="terminal-shell">
      <section className="terminal-grid">
        <header className="terminal-panel terminal-header">
          <div className="header-brand">
            <span className="terminal-tag">AIRTRACE // ATTRIBUTION TERMINAL</span>
            <h1>{selectedCity.city.toUpperCase()} POLLUTION FLOW CONSOLE</h1>
          </div>
          <div className="header-status">
            <div className="status-block">
              <span className="status-label">FEED</span>
              <strong>{selectedCity.dataMode === "live" ? "LIVE" : "FALLBACK"}</strong>
            </div>
            <div className="status-block">
              <span className="status-label">UPDATED</span>
              <strong>{formatTimestamp(selectedCity.updatedAt)}</strong>
            </div>
            <div className="status-block">
              <span className="status-label">CONFIDENCE</span>
              <strong>{selectedCity.confidence.toUpperCase()}</strong>
            </div>
          </div>
        </header>

        <section className="terminal-panel ticker-panel">
          <span className="ticker-label">ACTIVE READOUT</span>
          <p className="ticker-text">
            PM2.5 {selectedCity.pm25} ug/m3 // {selectedCity.category.toUpperCase()} // Imported {formatShare(selectedCity.importedShare)} // Local {formatShare(selectedCity.localShare)} // Dominant source {strongestSource?.name ?? "N/A"} // Prevailing wind {strongestWind?.direction ?? "N/A"} {strongestWind?.speedKph ?? 0} kph
          </p>
        </section>

        <section className="metric-strip">
          <article className="terminal-panel metric-panel">
            <span className="panel-label">PM2.5</span>
            <strong className="metric-number">{selectedCity.pm25}</strong>
            <span className="metric-sub">UG/M3 CURRENT LOAD</span>
          </article>
          <article className="terminal-panel metric-panel">
            <span className="panel-label">POLLUTION FROM OUTSIDE NEPAL</span>
            <strong className="metric-number">{selectedCity.importedShare}</strong>
            <span className="metric-sub">ESTIMATED SHARE OF CURRENT LOAD</span>
          </article>
          <article className="terminal-panel metric-panel">
            <span className="panel-label">POLLUTION FROM WITHIN NEPAL</span>
            <strong className="metric-number">{selectedCity.localShare}</strong>
            <span className="metric-sub">ESTIMATED SHARE OF CURRENT LOAD</span>
          </article>
          <article className="terminal-panel metric-panel">
            <span className="panel-label">WIND</span>
            <strong className="metric-number">
              {strongestWind?.speedKph ?? 0}
            </strong>
            <span className="metric-sub">
              KPH {strongestWind?.direction ?? "N/A"}
            </span>
          </article>
        </section>

        <section className="terminal-panel map-panel">
          <div className="panel-head">
            <span className="panel-label">TRANSPORT MAP</span>
            <span className="panel-value">REGIONAL FLOW MODEL</span>
          </div>
          <div className="terminal-map">
            <AttributionMap city={selectedCity} />
          </div>
        </section>

        <section className="terminal-panel source-panel">
          <div className="panel-head">
            <span className="panel-label">SOURCE ATTRIBUTION</span>
            <span className="panel-value">RANKED CONTRIBUTORS</span>
          </div>
          <div className="table-head">
            <span>SOURCE</span>
            <span>SHARE</span>
          </div>
          <div className="table-list">
            {selectedCity.sources.map((source) => (
              <div key={source.name} className="table-row">
                <div className="table-main">
                  <strong>{source.name}</strong>
                  <span>{source.evidence}</span>
                </div>
                <div className="table-side">
                  <strong>{formatShare(source.share)}</strong>
                  <div className="terminal-bar">
                    <div
                      className="terminal-bar-fill"
                      style={{ width: `${source.share}%` }}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="terminal-panel wind-panel">
          <div className="panel-head">
            <span className="panel-label">BACK TRAJECTORY</span>
            <span className="panel-value">48H WIND TRACE</span>
          </div>
          <div className="matrix-head">
            <span>WINDOW</span>
            <span>DIR</span>
            <span>SPEED</span>
          </div>
          <div className="matrix-list">
            {selectedCity.windTrail.map((point) => (
              <div key={point.hour} className="matrix-row">
                <span>{point.hour}</span>
                <strong>{point.direction}</strong>
                <span>{point.speedKph} KPH</span>
              </div>
            ))}
          </div>
        </section>

        <section className="terminal-panel evidence-panel">
          <div className="panel-head">
            <span className="panel-label">INTELLIGENCE</span>
            <span className="panel-value">MODEL NOTES + GLOBAL WATCHLIST</span>
          </div>
          <div className="intel-grid">
            <div className="note-stack">
              <div className="note-item">
                <span className="note-label">PRIMARY CALL</span>
                <p>{selectedCity.summary}</p>
              </div>
              <div className="note-item">
                <span className="note-label">LEADING SOURCE</span>
                <p>
                  {strongestSource?.name ?? "N/A"} currently ranks highest with an
                  estimated {formatShare(strongestSource?.share ?? 0)} contribution.
                </p>
              </div>
              <div className="note-item">
                <span className="note-label">PROVENANCE</span>
                <p>
                  OpenAQ readings + Open-Meteo wind vectors +{" "}
                  {selectedCity.interpretationMode === "ai"
                    ? "OpenAI-generated attribution layer."
                    : "heuristic attribution layer."}
                </p>
              </div>
              <div className="note-item">
                <span className="note-label">SCOPE</span>
                <p>Single-city tracking is active for Kathmandu, Nepal.</p>
              </div>
            </div>

            <div className="feed-block">
              <div className="panel-head panel-head-inline">
                <span className="panel-label">SIX-HOUR FEED</span>
                <span className="panel-value">ROLLING HISTORY</span>
              </div>
              <div className="feed-list">
                {selectedCity.feed.map((entry) => (
                  <div key={entry.timestamp} className="feed-row">
                    <div className="feed-meta">
                      <strong>{formatFeedTimestamp(entry.timestamp)}</strong>
                      <span>
                        PM2.5 {entry.pm25} // IMP {entry.importedShare}% // LOC {entry.localShare}% // {entry.windDirection} {entry.windSpeedKph} KPH
                      </span>
                    </div>
                    <p>{entry.headline}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>
      </section>
    </main>
  );
}
