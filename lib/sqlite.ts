import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import type { CitySnapshot } from "@/lib/airtrace-data";

const DEFAULT_DB_PATH = path.join(process.cwd(), "data", "airtrace.sqlite");
const SERVERLESS_DB_PATH = path.join(process.env.TMPDIR ?? "/tmp", "airtrace.sqlite");

type DatabaseHandle = Database.Database;

export type SevenDayTrendEntry = {
  localDay: string;
  sourceUpdatedAt: string;
  pm25: number;
  importedShare: number;
  localShare: number;
  hotspotCount: number;
  croplandMatchCount: number;
  dominantSource: string;
};

export type RecentSnapshotEntry = {
  generatedAt: string;
  updatedAt: string;
  city: string;
  country: string;
  pm25: number;
  aqi: number;
  aqiCategory: string;
  category: string;
  importedShare: number;
  localShare: number;
  confidence: "Low" | "Medium" | "High";
  summary: string;
};

declare global {
  var __airtraceDb: DatabaseHandle | undefined;
}

function getDatabasePath() {
  if (process.env.AIRTRACE_DB_PATH) {
    return process.env.AIRTRACE_DB_PATH;
  }

  if (process.env.VERCEL || process.cwd().startsWith("/var/task")) {
    return SERVERLESS_DB_PATH;
  }

  return DEFAULT_DB_PATH;
}

function initializeDatabase(db: DatabaseHandle) {
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(`
    CREATE TABLE IF NOT EXISTS snapshots (
      generated_at TEXT PRIMARY KEY,
      source_updated_at TEXT NOT NULL,
      city TEXT NOT NULL,
      country TEXT NOT NULL,
      pm25 REAL NOT NULL,
      aqi INTEGER NOT NULL,
      aqi_category TEXT NOT NULL,
      category TEXT NOT NULL,
      imported_share INTEGER NOT NULL,
      local_share INTEGER NOT NULL,
      confidence TEXT NOT NULL,
      summary TEXT NOT NULL,
      latitude REAL NOT NULL,
      longitude REAL NOT NULL,
      data_mode TEXT NOT NULL,
      interpretation_mode TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS source_contributions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_generated_at TEXT NOT NULL,
      position INTEGER NOT NULL,
      source_name TEXT NOT NULL,
      share INTEGER NOT NULL,
      evidence TEXT NOT NULL,
      FOREIGN KEY (snapshot_generated_at) REFERENCES snapshots(generated_at) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_snapshots_source_updated_at
      ON snapshots(source_updated_at DESC);

    CREATE INDEX IF NOT EXISTS idx_source_contributions_snapshot
      ON source_contributions(snapshot_generated_at, position);

    CREATE TABLE IF NOT EXISTS snapshot_fire_evidence (
      snapshot_generated_at TEXT PRIMARY KEY,
      hotspot_count INTEGER NOT NULL,
      hotspot_density REAL NOT NULL,
      mean_frp REAL NOT NULL,
      crop_belt_hotspot_count INTEGER NOT NULL,
      cropland_match_count INTEGER NOT NULL,
      inferred_source_name TEXT NOT NULL,
      inferred_evidence_label TEXT NOT NULL,
      FOREIGN KEY (snapshot_generated_at) REFERENCES snapshots(generated_at) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_snapshot_fire_evidence_source
      ON snapshot_fire_evidence(inferred_source_name);

    CREATE TABLE IF NOT EXISTS external_signal_cache (
      cache_key TEXT PRIMARY KEY,
      signal_kind TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      checked_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_external_signal_cache_kind
      ON external_signal_cache(signal_kind, checked_at DESC);

    CREATE TABLE IF NOT EXISTS cropland_cache (
      hotspot_key TEXT PRIMARY KEY,
      latitude REAL NOT NULL,
      longitude REAL NOT NULL,
      near_cropland INTEGER NOT NULL,
      checked_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_cropland_cache_checked_at
      ON cropland_cache(checked_at DESC);
  `);
}

function createDatabase() {
  const databasePath = getDatabasePath();
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });
  const db = new Database(databasePath);
  initializeDatabase(db);
  return db;
}

function getDatabase() {
  if (!global.__airtraceDb) {
    global.__airtraceDb = createDatabase();
  }

  initializeDatabase(global.__airtraceDb);

  return global.__airtraceDb;
}

export function persistSnapshot(snapshot: CitySnapshot) {
  const db = getDatabase();
  const insertSnapshot = db.prepare(`
    INSERT OR REPLACE INTO snapshots (
      generated_at,
      source_updated_at,
      city,
      country,
      pm25,
      aqi,
      aqi_category,
      category,
      imported_share,
      local_share,
      confidence,
      summary,
      latitude,
      longitude,
      data_mode,
      interpretation_mode
    ) VALUES (
      @generatedAt,
      @updatedAt,
      @city,
      @country,
      @pm25,
      @aqi,
      @aqiCategory,
      @category,
      @importedShare,
      @localShare,
      @confidence,
      @summary,
      @latitude,
      @longitude,
      @dataMode,
      @interpretationMode
    )
  `);
  const clearSources = db.prepare(`
    DELETE FROM source_contributions
    WHERE snapshot_generated_at = ?
  `);
  const upsertFireEvidence = db.prepare(`
    INSERT OR REPLACE INTO snapshot_fire_evidence (
      snapshot_generated_at,
      hotspot_count,
      hotspot_density,
      mean_frp,
      crop_belt_hotspot_count,
      cropland_match_count,
      inferred_source_name,
      inferred_evidence_label
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertSource = db.prepare(`
    INSERT INTO source_contributions (
      snapshot_generated_at,
      position,
      source_name,
      share,
      evidence
    ) VALUES (?, ?, ?, ?, ?)
  `);

  const transaction = db.transaction((currentSnapshot: CitySnapshot) => {
    insertSnapshot.run({
      generatedAt: currentSnapshot.generatedAt,
      updatedAt: currentSnapshot.updatedAt,
      city: currentSnapshot.city,
      country: currentSnapshot.country,
      pm25: currentSnapshot.pm25,
      aqi: currentSnapshot.aqi,
      aqiCategory: currentSnapshot.aqiCategory,
      category: currentSnapshot.category,
      importedShare: currentSnapshot.importedShare,
      localShare: currentSnapshot.localShare,
      confidence: currentSnapshot.confidence,
      summary: currentSnapshot.summary,
      latitude: currentSnapshot.coordinates.lat,
      longitude: currentSnapshot.coordinates.lng,
      dataMode: currentSnapshot.dataMode,
      interpretationMode: currentSnapshot.interpretationMode
    });

    clearSources.run(currentSnapshot.generatedAt);

    currentSnapshot.sources.forEach((source, index) => {
      insertSource.run(
        currentSnapshot.generatedAt,
        index,
        source.name,
        source.share,
        source.evidence
      );
    });

    upsertFireEvidence.run(
      currentSnapshot.generatedAt,
      currentSnapshot.fireEvidence.hotspotCount,
      currentSnapshot.fireEvidence.hotspotDensity,
      currentSnapshot.fireEvidence.meanFrp,
      currentSnapshot.fireEvidence.cropBeltHotspotCount,
      currentSnapshot.fireEvidence.croplandMatchCount,
      currentSnapshot.fireEvidence.inferredSourceName,
      currentSnapshot.fireEvidence.inferredEvidenceLabel
    );
  });

  transaction(snapshot);
}

export function getRecentSnapshots(limit = 24) {
  const db = getDatabase();
  return db
    .prepare(
      `
        SELECT
          generated_at as generatedAt,
          source_updated_at as updatedAt,
          city,
          country,
          pm25,
          aqi,
          aqi_category as aqiCategory,
          category,
          imported_share as importedShare,
          local_share as localShare,
          confidence,
          summary
        FROM snapshots
        ORDER BY generated_at DESC
        LIMIT ?
      `
    )
    .all(limit) as RecentSnapshotEntry[];
}

export function getRecentFireEvidence(limit = 24) {
  const db = getDatabase();
  return db
    .prepare(
      `
        SELECT
          snapshot_generated_at as generatedAt,
          hotspot_count as hotspotCount,
          hotspot_density as hotspotDensity,
          mean_frp as meanFrp,
          crop_belt_hotspot_count as cropBeltHotspotCount,
          cropland_match_count as croplandMatchCount,
          inferred_source_name as inferredSourceName,
          inferred_evidence_label as inferredEvidenceLabel
        FROM snapshot_fire_evidence
        ORDER BY snapshot_generated_at DESC
        LIMIT ?
      `
    )
    .all(limit);
}

export function getSignalCacheEntry(cacheKey: string) {
  const db = getDatabase();
  return db
    .prepare(
      `
        SELECT
          cache_key as cacheKey,
          signal_kind as signalKind,
          payload_json as payloadJson,
          checked_at as checkedAt
        FROM external_signal_cache
        WHERE cache_key = ?
      `
    )
    .get(cacheKey) as
    | {
        cacheKey: string;
        signalKind: string;
        payloadJson: string;
        checkedAt: string;
      }
    | undefined;
}

export function setSignalCacheEntry(input: {
  cacheKey: string;
  signalKind: string;
  payloadJson: string;
  checkedAt: string;
}) {
  const db = getDatabase();
  db.prepare(
    `
      INSERT OR REPLACE INTO external_signal_cache (
        cache_key,
        signal_kind,
        payload_json,
        checked_at
      ) VALUES (
        @cacheKey,
        @signalKind,
        @payloadJson,
        @checkedAt
      )
    `
  ).run(input);
}

export function getSevenDayTrend(limit = 7) {
  const db = getDatabase();
  return db
    .prepare(
      `
        WITH ranked AS (
          SELECT
            s.generated_at,
            s.source_updated_at,
            s.pm25,
            s.imported_share,
            s.local_share,
            sfe.hotspot_count,
            sfe.cropland_match_count,
            COALESCE(sc.source_name, 'Unavailable') AS dominant_source,
            date(
              datetime(
                s.source_updated_at,
                '+5 hours',
                '+45 minutes'
              )
            ) AS local_day,
            ROW_NUMBER() OVER (
              PARTITION BY date(
                datetime(
                  s.source_updated_at,
                  '+5 hours',
                  '+45 minutes'
                )
              )
              ORDER BY s.generated_at DESC
            ) AS row_num
          FROM snapshots s
          LEFT JOIN snapshot_fire_evidence sfe
            ON sfe.snapshot_generated_at = s.generated_at
          LEFT JOIN source_contributions sc
            ON sc.snapshot_generated_at = s.generated_at
           AND sc.position = 0
        )
        SELECT
          local_day as localDay,
          source_updated_at as sourceUpdatedAt,
          pm25,
          imported_share as importedShare,
          local_share as localShare,
          COALESCE(hotspot_count, 0) as hotspotCount,
          COALESCE(cropland_match_count, 0) as croplandMatchCount,
          dominant_source as dominantSource
        FROM ranked
        WHERE row_num = 1
        ORDER BY source_updated_at DESC
        LIMIT ?
      `
    )
    .all(limit) as SevenDayTrendEntry[];
}

export function getCroplandCacheEntry(hotspotKey: string) {
  const db = getDatabase();
  return db
    .prepare(
      `
        SELECT
          hotspot_key as hotspotKey,
          latitude,
          longitude,
          near_cropland as nearCropland,
          checked_at as checkedAt
        FROM cropland_cache
        WHERE hotspot_key = ?
      `
    )
    .get(hotspotKey) as
    | {
        hotspotKey: string;
        latitude: number;
        longitude: number;
        nearCropland: number;
        checkedAt: string;
      }
    | undefined;
}

export function setCroplandCacheEntry(input: {
  hotspotKey: string;
  latitude: number;
  longitude: number;
  nearCropland: boolean;
  checkedAt: string;
}) {
  const db = getDatabase();
  db.prepare(
    `
      INSERT OR REPLACE INTO cropland_cache (
        hotspot_key,
        latitude,
        longitude,
        near_cropland,
        checked_at
      ) VALUES (
        @hotspotKey,
        @latitude,
        @longitude,
        @nearCropland,
        @checkedAt
      )
    `
  ).run({
    hotspotKey: input.hotspotKey,
    latitude: input.latitude,
    longitude: input.longitude,
    nearCropland: input.nearCropland ? 1 : 0,
    checkedAt: input.checkedAt
  });
}
