import { unstable_noStore as noStore } from "next/cache";
import JSZip from "jszip";

import {
  getCroplandCacheEntry,
  getRecentSnapshots,
  getSevenDayTrend,
  getSignalCacheEntry,
  persistSnapshot,
  setSignalCacheEntry,
  setCroplandCacheEntry
} from "@/lib/sqlite";

export type SourceContribution = {
  name: string;
  share: number;
  evidence: string;
};

export type MapEvidencePoint = {
  lat: number;
  lng: number;
  label: string;
  kind:
    | "station"
    | "hotspot"
    | "cropland-hotspot"
    | "kiln"
    | "industrial";
  weight?: number;
};

export type MapEvidenceLine = {
  label: string;
  kind: "trajectory";
  points: [number, number][];
};

export type MapEvidence = {
  stations: MapEvidencePoint[];
  hotspots: MapEvidencePoint[];
  registry: MapEvidencePoint[];
  lines: MapEvidenceLine[];
};

export type WindPoint = {
  hour: string;
  direction: string;
  speedKph: number;
};

export type TimelineFrame = {
  timestamp: string;
  pm25: number;
  aqi: number;
  aqiCategory: string;
  importedShare: number;
  localShare: number;
  confidence: "Low" | "Medium" | "High";
  dominantSource: string;
  windDirection: string;
  windSpeedKph: number;
  transportPath: [number, number][];
  regime: AttributionRegime;
};

export type AttributionRegime = {
  name:
    | "Local accumulation"
    | "Regional transport"
    | "Fire-influenced transport"
    | "Dust-influenced transport"
    | "Mixed transition";
  confidence: number;
};

export type TrajectoryEvidence = {
  provider: "hysplit" | "wind-model";
  available: boolean;
  originBearing: number | null;
  originDistanceKm: number | null;
  pathPoints: [number, number][];
  agriTransport: number;
  industrialTransport: number;
  localRetention: number;
  dustTransport: number;
  trajectoryConsistency: number;
};

export type ModelEvidence = {
  modeledPm25: number | null;
  modeledUsAqi: number | null;
  modeledDust: number | null;
  aerosolOpticalDepth: number | null;
  agreement: "Strong" | "Moderate" | "Weak" | "Unavailable";
};

export type StationEvidence = {
  locationId: number;
  sensorId: number | null;
  label: string;
  pm25: number;
  updatedAt: string;
  latitude: number | null;
  longitude: number | null;
  distanceKm: number;
  stationQuality: number;
  weight: number;
  freshnessHours: number;
  consensusDelta: number;
  outlierPenalty: number;
};

export type StationSummary = {
  primarySensorId: number | null;
  stationQuality: number;
  agreementScore: number;
  stationCount: number;
  spreadUgM3: number;
  outlierCount: number;
  consensusMethod: "robust-weighted-mean";
};

export type ConfidenceBreakdown = {
  freshness: number;
  stationAgreement: number;
  modelAgreement: number;
  transport: number;
  history: number;
  hotspotSupport: number;
  overall: number;
};

export type FireEvidence = {
  hotspotCount: number;
  hotspotDensity: number;
  meanFrp: number;
  cropBeltHotspotCount: number;
  croplandMatchCount: number;
  inferredSourceName: string;
  inferredEvidenceLabel: string;
  hotspots: Array<{
    lat: number;
    lng: number;
    frp: number;
    date: string;
    nearCropland: boolean | null;
  }>;
};

export type RegistryMatch = {
  lat: number;
  lng: number;
  label: string;
};

export type RegistryEvidence = {
  provider: "osm";
  localKilnCount: number;
  localIndustrialCount: number;
  upwindIndustrialCount: number;
  upwindKilnCount: number;
  localKilns: RegistryMatch[];
  localIndustrialSites: RegistryMatch[];
  upwindIndustrialSites: RegistryMatch[];
  upwindKilns: RegistryMatch[];
};

export type CitySnapshot = {
  city: string;
  country: string;
  updatedAt: string;
  generatedAt: string;
  pm25: number;
  aqi: number;
  aqiCategory: string;
  category: string;
  importedShare: number;
  localShare: number;
  confidence: "Low" | "Medium" | "High";
  summary: string;
  coordinates: {
    lat: number;
    lng: number;
  };
  sources: SourceContribution[];
  windTrail: WindPoint[];
  feed: FeedEntry[];
  timeline24h: TimelineFrame[];
  trajectoryEvidence: TrajectoryEvidence;
  registryEvidence: RegistryEvidence;
  stationEvidence: StationEvidence[];
  stationSummary: StationSummary;
  modelEvidence: ModelEvidence;
  confidenceBreakdown: ConfidenceBreakdown;
  fireEvidence: FireEvidence;
  mapEvidence: MapEvidence;
  regime: AttributionRegime;
  dataMode: "live";
  interpretationMode: "heuristic";
};

export type FeedEntry = {
  timestamp: string;
  pm25: number;
  importedShare: number;
  localShare: number;
  windDirection: string;
  windSpeedKph: number;
  headline: string;
};

export class SnapshotError extends Error {
  constructor(message: string, readonly causeLabel: string) {
    super(message);
    this.name = "SnapshotError";
  }
}

type OpenAQLocation = {
  id: number;
  locality?: string | null;
  name?: string | null;
  coordinates?: {
    latitude?: number;
    longitude?: number;
  };
};

type OpenAQLatestMeasurement = {
  value: number;
  sensorsId?: number;
  datetime?: {
    utc?: string;
  };
};

type OpenAQHourlyMeasurement = {
  value: number;
  datetime?: {
    utc?: string;
  };
  period?: {
    datetimeFrom?: {
      utc?: string;
    };
    datetimeTo?: {
      utc?: string;
    };
  };
  coverage?: {
    datetimeFrom?: {
      utc?: string;
    };
    datetimeTo?: {
      utc?: string;
    };
  };
};

type OpenAQSensor = {
  id: number;
  name?: string | null;
  parameter?: {
    id?: number;
    name?: string | null;
  };
};

type OpenAQListResponse<T> = {
  results?: T[];
};

type OpenMeteoResponse = {
  current?: {
    time?: string;
    wind_speed_10m?: number;
    wind_direction_10m?: number;
  };
  hourly?: {
    time?: string[];
    wind_speed_10m?: number[];
    wind_direction_10m?: number[];
  };
};

type OpenMeteoAirQualityResponse = {
  current?: {
    time?: string;
    pm2_5?: number;
    us_aqi?: number;
    dust?: number;
    aerosol_optical_depth?: number;
  };
  hourly?: {
    time?: string[];
    pm2_5?: number[];
    us_aqi?: number[];
    dust?: number[];
    aerosol_optical_depth?: number[];
  };
};

type AiAttributionResponse = {
  summary: string;
  importedShare: number;
  localShare: number;
  confidence: "Low" | "Medium" | "High";
  confidenceBreakdown: ConfidenceBreakdown;
  sources: Array<SourceContribution & { imported: boolean }>;
  regime: AttributionRegime;
};

type ModelEvidenceContext = ModelEvidence & {
  agreementScore: number;
};

type FireSignal = FireEvidence & {
  hotspotCount: number;
  hotspotDensity: number;
  meanFrp: number;
  cropBeltHotspotCount: number;
  croplandMatchCount: number;
  inferredSourceName: string;
  inferredEvidenceLabel: string;
};

type FireHotspot = {
  lat: number;
  lng: number;
  frp: number;
  date: string;
  nearCropland?: boolean | null;
};

type RegistryEvidenceInternal = RegistryEvidence & {
  provider: "osm";
};

type TrajectoryPoint = {
  ageHours: number;
  latitude: number;
  longitude: number;
  heightM: number;
  bearing: number;
  distanceKm: number;
};

type ContributorCandidate = {
  name: string;
  evidence: string;
  score: number;
  imported: boolean;
};

export type HistoricalTrendEntry = {
  localDay: string;
  sourceUpdatedAt: string;
  pm25: number;
  importedShare: number;
  localShare: number;
  hotspotCount: number;
  croplandMatchCount: number;
  dominantSource: string;
};

const PM25_PARAMETER_ID = 2;
const OPENAQ_BASE_URL = "https://api.openaq.org/v3";
const OPEN_METEO_BASE_URL =
  process.env.OPEN_METEO_BASE_URL ?? "https://api.open-meteo.com";
const OPEN_METEO_ARCHIVE_BASE_URL =
  process.env.OPEN_METEO_ARCHIVE_BASE_URL ?? "https://archive-api.open-meteo.com";
const OPEN_METEO_AIR_QUALITY_BASE_URL =
  process.env.OPEN_METEO_AIR_QUALITY_BASE_URL ?? "https://air-quality-api.open-meteo.com";
const HYSPLIT_BASE_URL =
  process.env.HYSPLIT_BASE_URL ?? "https://apps.arl.noaa.gov/ready2";
const MAX_MEASUREMENT_AGE_HOURS = 24;
const OPENAQ_LOCATION_CACHE_TTL_HOURS = 24;
const FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv";
const FIRMS_SOURCE = "VIIRS_SNPP_NRT";
const OVERPASS_API_URL = "https://overpass-api.de/api/interpreter";
const FIRMS_UPWIND_BBOX = "80,24,89,31";
const CROPLAND_PROXIMITY_METERS = 750;
const MAX_CROPLAND_HOTSPOT_SAMPLES = 12;
const MAX_HOTSPOTS_FOR_MAP = 80;
const MAX_REGISTRY_POINTS = 24;
const CROPLAND_CACHE_TTL_HOURS = 24 * 14;
const SIGNAL_CACHE_TTL_HOURS = 24 * 7;
const HISTORICAL_TREND_CACHE_TTL_HOURS = 6;
const LOCAL_REGISTRY_RADIUS_METERS = 30000;
const UPWIND_REGISTRY_RADIUS_METERS = 50000;
const NORTH_INDIA_CROP_BELT_BBOX = {
  minLat: 27.2,
  maxLat: 31.4,
  minLng: 74.0,
  maxLng: 78.8
};

const kathmandu = {
  city: "Kathmandu",
  country: "Nepal",
  coordinates: {
    lat: 27.7172,
    lng: 85.324
  },
  localityHints: ["Kathmandu", "Lalitpur", "Bhaktapur"]
};

function getCategory(pm25: number) {
  if (pm25 <= 12) return "Good";
  if (pm25 <= 35.4) return "Moderate";
  if (pm25 <= 55.4) return "Unhealthy for Sensitive Groups";
  if (pm25 <= 150.4) return "Very Unhealthy";
  if (pm25 <= 250.4) return "Hazardous";
  return "Severe";
}

function getAqiFromPm25(pm25: number) {
  const breakpoints = [
    { cLow: 0, cHigh: 12, iLow: 0, iHigh: 50 },
    { cLow: 12.1, cHigh: 35.4, iLow: 51, iHigh: 100 },
    { cLow: 35.5, cHigh: 55.4, iLow: 101, iHigh: 150 },
    { cLow: 55.5, cHigh: 150.4, iLow: 151, iHigh: 200 },
    { cLow: 150.5, cHigh: 250.4, iLow: 201, iHigh: 300 },
    { cLow: 250.5, cHigh: 350.4, iLow: 301, iHigh: 400 },
    { cLow: 350.5, cHigh: 500.4, iLow: 401, iHigh: 500 }
  ];
  const truncated = Math.floor(pm25 * 10) / 10;
  const range =
    breakpoints.find(
      (breakpoint) => truncated >= breakpoint.cLow && truncated <= breakpoint.cHigh
    ) ?? breakpoints[breakpoints.length - 1];

  return Math.round(
    ((range.iHigh - range.iLow) / (range.cHigh - range.cLow)) *
      (truncated - range.cLow) +
      range.iLow
  );
}

function getAqiCategory(aqi: number) {
  if (aqi <= 50) return "Good";
  if (aqi <= 100) return "Moderate";
  if (aqi <= 150) return "USG";
  if (aqi <= 200) return "Unhealthy";
  if (aqi <= 300) return "Very Unhealthy";
  return "Hazardous";
}

function directionFromDegrees(degrees: number) {
  const normalized = ((degrees % 360) + 360) % 360;
  const directions = [
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW"
  ];
  const index = Math.round(normalized / 22.5) % 16;
  return directions[index];
}

function getKathmanduLocalDay(timestamp: string) {
  const offsetMs = (5 * 60 + 45) * 60 * 1000;
  return new Date(new Date(timestamp).getTime() + offsetMs).toISOString().slice(0, 10);
}

function toUtcDateString(timestamp: string) {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function toIsoDateString(date: Date) {
  return date.toISOString().slice(0, 10);
}

function buildWindTrail(weather: OpenMeteoResponse): WindPoint[] {
  const hourly = weather.hourly;
  const times = hourly?.time ?? [];
  const speeds = hourly?.wind_speed_10m ?? [];
  const directions = hourly?.wind_direction_10m ?? [];
  const indexes = [
    times.length - 1,
    times.length - 7,
    times.length - 13,
    times.length - 25,
    times.length - 49
  ];
  const labels = ["Now", "-6h", "-12h", "-24h", "-48h"];

  return indexes.map((index, idx) => {
    const safeIndex = index >= 0 ? index : 0;
    return {
      hour: labels[idx],
      direction: directionFromDegrees(
        directions[safeIndex] ?? weather.current?.wind_direction_10m ?? 0
      ),
      speedKph: Math.round(
        speeds[safeIndex] ?? weather.current?.wind_speed_10m ?? 0
      )
    };
  });
}

function findHourlyIndex(times: string[], timestamp: string) {
  const exactIndex = times.findIndex((time) => time === timestamp);

  if (exactIndex >= 0) {
    return exactIndex;
  }

  const targetTime = new Date(timestamp).getTime();
  let bestIndex = -1;
  let bestDistance = Number.POSITIVE_INFINITY;

  times.forEach((time, index) => {
    const candidateTime = new Date(time).getTime();
    const distance = Math.abs(candidateTime - targetTime);

    if (distance < bestDistance) {
      bestDistance = distance;
      bestIndex = index;
    }
  });

  return bestDistance <= 90 * 60 * 1000 ? bestIndex : -1;
}

function getHourlyMeasurementTimestamp(measurement: OpenAQHourlyMeasurement) {
  return (
    measurement.datetime?.utc ??
    measurement.period?.datetimeTo?.utc ??
    measurement.coverage?.datetimeTo?.utc ??
    measurement.period?.datetimeFrom?.utc ??
    measurement.coverage?.datetimeFrom?.utc ??
    null
  );
}

function buildModelEvidence(
  observedPm25: number,
  airQuality: OpenMeteoAirQualityResponse | null,
  timestamp: string
): ModelEvidenceContext {
  const hourlyTimes = airQuality?.hourly?.time ?? [];
  const hourlyPm25 = airQuality?.hourly?.pm2_5 ?? [];
  const hourlyUsAqi = airQuality?.hourly?.us_aqi ?? [];
  const hourlyDust = airQuality?.hourly?.dust ?? [];
  const hourlyAod = airQuality?.hourly?.aerosol_optical_depth ?? [];
  const index = timestamp ? findHourlyIndex(hourlyTimes, timestamp) : -1;
  const modeledPm25 =
    index >= 0
      ? hourlyPm25[index] ?? airQuality?.current?.pm2_5 ?? null
      : airQuality?.current?.pm2_5 ?? null;
  const modeledUsAqi =
    index >= 0
      ? hourlyUsAqi[index] ?? airQuality?.current?.us_aqi ?? null
      : airQuality?.current?.us_aqi ?? null;
  const modeledDust =
    index >= 0
      ? hourlyDust[index] ?? airQuality?.current?.dust ?? null
      : airQuality?.current?.dust ?? null;
  const aerosolOpticalDepth =
    index >= 0
      ? hourlyAod[index] ?? airQuality?.current?.aerosol_optical_depth ?? null
      : airQuality?.current?.aerosol_optical_depth ?? null;

  if (typeof modeledPm25 !== "number" || !Number.isFinite(modeledPm25)) {
    return {
      modeledPm25: null,
      modeledUsAqi: typeof modeledUsAqi === "number" ? Math.round(modeledUsAqi) : null,
      modeledDust: typeof modeledDust === "number" ? Math.round(modeledDust) : null,
      aerosolOpticalDepth:
        typeof aerosolOpticalDepth === "number"
          ? Math.round(aerosolOpticalDepth * 100) / 100
          : null,
      agreement: "Unavailable",
      agreementScore: 0.5
    };
  }

  const relativeDifference = Math.abs(modeledPm25 - observedPm25) / Math.max(observedPm25, 12);
  const absoluteDifference = Math.abs(modeledPm25 - observedPm25);

  if (absoluteDifference <= 8 || relativeDifference <= 0.2) {
    return {
      modeledPm25: Math.round(modeledPm25),
      modeledUsAqi: typeof modeledUsAqi === "number" ? Math.round(modeledUsAqi) : null,
      modeledDust: typeof modeledDust === "number" ? Math.round(modeledDust) : null,
      aerosolOpticalDepth:
        typeof aerosolOpticalDepth === "number"
          ? Math.round(aerosolOpticalDepth * 100) / 100
          : null,
      agreement: "Strong",
      agreementScore: 1
    };
  }

  if (absoluteDifference <= 18 || relativeDifference <= 0.45) {
    return {
      modeledPm25: Math.round(modeledPm25),
      modeledUsAqi: typeof modeledUsAqi === "number" ? Math.round(modeledUsAqi) : null,
      modeledDust: typeof modeledDust === "number" ? Math.round(modeledDust) : null,
      aerosolOpticalDepth:
        typeof aerosolOpticalDepth === "number"
          ? Math.round(aerosolOpticalDepth * 100) / 100
          : null,
      agreement: "Moderate",
      agreementScore: 0.7
    };
  }

  return {
    modeledPm25: Math.round(modeledPm25),
    modeledUsAqi: typeof modeledUsAqi === "number" ? Math.round(modeledUsAqi) : null,
    modeledDust: typeof modeledDust === "number" ? Math.round(modeledDust) : null,
    aerosolOpticalDepth:
      typeof aerosolOpticalDepth === "number"
        ? Math.round(aerosolOpticalDepth * 100) / 100
        : null,
    agreement: "Weak",
    agreementScore: 0.32
  };
}

function isSignalCacheFresh(checkedAt: string, ttlHours = SIGNAL_CACHE_TTL_HOURS) {
  const ageMs = Date.now() - new Date(checkedAt).getTime();
  return ageMs >= 0 && ageMs <= ttlHours * 60 * 60 * 1000;
}

function getRegistryCacheKey(label: string, center: [number, number], radiusMeters: number) {
  return `${label}:${center[0].toFixed(3)},${center[1].toFixed(3)}:${radiusMeters}`;
}

function getTrajectoryCacheKey(timestamp: string) {
  const date = new Date(timestamp);
  return `hysplit:${date.toISOString().slice(0, 13)}`;
}

function estimateImportedShare(
  pm25: number,
  windSpeedKph: number,
  windDirection: number
) {
  const westernFlowBoost =
    windDirection >= 180 && windDirection <= 315 ? 12 : 0;
  const transportBoost = windSpeedKph >= 18 ? 10 : windSpeedKph >= 10 ? 5 : 0;
  const concentrationBoost = pm25 >= 120 ? 8 : pm25 >= 80 ? 4 : 0;

  return Math.max(
    20,
    Math.min(78, 42 + westernFlowBoost + transportBoost + concentrationBoost)
  );
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function parseCsvLine(line: string) {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (char === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values;
}

function average(values: number[]) {
  if (values.length === 0) {
    return 0;
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]) {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const midpoint = Math.floor(sorted.length / 2);

  if (sorted.length % 2 === 0) {
    return (sorted[midpoint - 1] + sorted[midpoint]) / 2;
  }

  return sorted[midpoint];
}

function weightedAverage(values: number[], weights: number[]) {
  const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);

  if (values.length === 0 || totalWeight <= 0) {
    return values[0] ?? 0;
  }

  return values.reduce((sum, value, index) => sum + value * (weights[index] ?? 0), 0) / totalWeight;
}

function weightedStandardDeviation(values: number[], weights: number[], mean: number) {
  const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);

  if (values.length === 0 || totalWeight <= 0) {
    return 0;
  }

  return Math.sqrt(
    values.reduce((sum, value, index) => {
      const diff = value - mean;
      return sum + diff * diff * (weights[index] ?? 0);
    }, 0) / totalWeight
  );
}

function confidenceLabelToScore(confidence: "Low" | "Medium" | "High") {
  if (confidence === "High") return 0.82;
  if (confidence === "Medium") return 0.58;
  return 0.34;
}

function angularDistance(a: number, b: number) {
  const diff = Math.abs((((a - b) % 360) + 540) % 360 - 180);
  return Math.min(diff, 180);
}

function toRadians(value: number) {
  return (value * Math.PI) / 180;
}

function haversineKm(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
) {
  const earthRadiusKm = 6371;
  const dLat = toRadians(lat2 - lat1);
  const dLng = toRadians(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLng / 2) ** 2;

  return 2 * earthRadiusKm * Math.asin(Math.sqrt(a));
}

function bearingDegrees(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
) {
  const phi1 = toRadians(lat1);
  const phi2 = toRadians(lat2);
  const lambda = toRadians(lng2 - lng1);
  const y = Math.sin(lambda) * Math.cos(phi2);
  const x =
    Math.cos(phi1) * Math.sin(phi2) -
    Math.sin(phi1) * Math.cos(phi2) * Math.cos(lambda);

  return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
}

function projectPoint(
  originLat: number,
  originLng: number,
  bearingDegrees: number,
  distanceKm: number
): [number, number] {
  const earthRadiusKm = 6371;
  const bearing = toRadians(bearingDegrees);
  const lat1 = toRadians(originLat);
  const lng1 = toRadians(originLng);
  const angularDistance = distanceKm / earthRadiusKm;

  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angularDistance) +
      Math.cos(lat1) * Math.sin(angularDistance) * Math.cos(bearing)
  );
  const lng2 =
    lng1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angularDistance) * Math.cos(lat1),
      Math.cos(angularDistance) - Math.sin(lat1) * Math.sin(lat2)
    );

  return [lat2 * (180 / Math.PI), lng2 * (180 / Math.PI)];
}

function projectFromKathmandu(bearingDegrees: number, distanceKm: number): [number, number] {
  return projectPoint(
    kathmandu.coordinates.lat,
    kathmandu.coordinates.lng,
    bearingDegrees,
    distanceKm
  );
}

function corridorStrength(directionDegrees: number, center: number, spread: number) {
  const distance = angularDistance(directionDegrees, center);
  return clamp(1 - distance / spread, 0, 1);
}

function scoreLocationQuality(location: OpenAQLocation) {
  const locality = `${location.locality ?? ""} ${location.name ?? ""}`.toLowerCase();
  const exactKathmanduMatch = locality.includes("kathmandu");
  const valleyMatch = kathmandu.localityHints.some((hint) =>
    locality.includes(hint.toLowerCase())
  );
  const lat = location.coordinates?.latitude;
  const lng = location.coordinates?.longitude;
  const distanceKm =
    typeof lat === "number" && typeof lng === "number"
      ? haversineKm(kathmandu.coordinates.lat, kathmandu.coordinates.lng, lat, lng)
      : 80;
  const distanceScore = clamp(1 - distanceKm / 260, 0, 1);

  return {
    distanceKm,
    quality: clamp(
      (exactKathmanduMatch ? 0.45 : 0) +
        (valleyMatch ? 0.3 : 0) +
        distanceScore * 0.25,
      0,
      1
    )
  };
}

function isWithinBounds(
  lat: number,
  lng: number,
  bounds: { minLat: number; maxLat: number; minLng: number; maxLng: number }
) {
  return (
    lat >= bounds.minLat &&
    lat <= bounds.maxLat &&
    lng >= bounds.minLng &&
    lng <= bounds.maxLng
  );
}

function buildRecentPm25Stats(
  hourlyMeasurements: OpenAQHourlyMeasurement[],
  measurementTimestamp: string
) {
  const end = new Date(measurementTimestamp).getTime();
  const valid = hourlyMeasurements
    .filter(
      (item) =>
        typeof item.value === "number" &&
        item.value >= 0 &&
        getHourlyMeasurementTimestamp(item)
    )
    .map((item) => ({
      value: item.value,
      time: new Date(getHourlyMeasurementTimestamp(item)!).getTime()
    }))
    .filter((item) => Number.isFinite(item.time) && item.time <= end)
    .sort((a, b) => a.time - b.time);

  const last12h = valid.filter((item) => end - item.time <= 12 * 60 * 60 * 1000);
  const last24h = valid.filter((item) => end - item.time <= 24 * 60 * 60 * 1000);
  const current = valid.at(-1)?.value ?? 0;
  const avg12h = average(last12h.map((item) => item.value));
  const avg24h = average(last24h.map((item) => item.value));

  return {
    current,
    avg12h,
    avg24h,
    count12h: last12h.length,
    count24h: last24h.length,
    persistence: clamp(avg12h / 80, 0, 1),
    spikeFactor: avg12h > 0 ? clamp((current - avg12h) / Math.max(avg12h, 20), 0, 1) : 0
  };
}

function buildWindStats(weather: OpenMeteoResponse) {
  const times = weather.hourly?.time ?? [];
  const speeds = weather.hourly?.wind_speed_10m ?? [];
  const directions = weather.hourly?.wind_direction_10m ?? [];
  const currentDirection = weather.current?.wind_direction_10m ?? 0;
  const recentIndexes = times
    .map((_, index) => index)
    .slice(Math.max(0, times.length - 12));
  const recentDirections = recentIndexes
    .map((index) => directions[index])
    .filter((value): value is number => typeof value === "number");
  const recentSpeeds = recentIndexes
    .map((index) => speeds[index])
    .filter((value): value is number => typeof value === "number");
  const alignedDirections = recentDirections.filter(
    (value) => angularDistance(value, currentDirection) <= 45
  ).length;

  return {
    avg12hSpeed: average(recentSpeeds),
    windConsistency:
      recentDirections.length > 0 ? alignedDirections / recentDirections.length : 0.5
  };
}

function buildTrajectorySignals(weather: OpenMeteoResponse, measurementTimestamp: string) {
  const measurementTime = new Date(measurementTimestamp).getTime();
  const times = weather.hourly?.time ?? [];
  const speeds = weather.hourly?.wind_speed_10m ?? [];
  const directions = weather.hourly?.wind_direction_10m ?? [];
  const points = times
    .map((time, index) => ({
      time: new Date(time).getTime(),
      direction: directions[index],
      speed: speeds[index]
    }))
    .filter(
      (point): point is { time: number; direction: number; speed: number } =>
        Number.isFinite(point.time) &&
        typeof point.direction === "number" &&
        typeof point.speed === "number" &&
        point.time <= measurementTime &&
        measurementTime - point.time <= 48 * 60 * 60 * 1000
    );

  if (points.length === 0) {
    return {
      agriTransport: 0.35,
      industrialTransport: 0.35,
      localRetention: 0.5,
      dustTransport: 0.3,
      trajectoryConsistency: 0.5
    };
  }

  let totalWeight = 0;
  let agriTransport = 0;
  let industrialTransport = 0;
  let localRetention = 0;
  let dustTransport = 0;
  let alignedWeight = 0;
  const anchorDirection = points.at(-1)?.direction ?? points[0].direction;

  for (const point of points) {
    const hoursAgo = (measurementTime - point.time) / (60 * 60 * 1000);
    const recencyWeight = clamp(1 - hoursAgo / 54, 0.15, 1);
    const speedWeight = clamp(point.speed / 24, 0, 1);
    const weight = recencyWeight * (0.45 + speedWeight * 0.55);
    const agri = corridorStrength(point.direction, 225, 70);
    const industrial = corridorStrength(point.direction, 255, 80);
    const local = 1 - clamp(Math.max(agri, industrial) * 0.92, 0, 1);
    const dust = clamp((point.speed - 12) / 16, 0, 1);

    totalWeight += weight;
    agriTransport += agri * weight;
    industrialTransport += industrial * weight;
    localRetention += local * weight;
    dustTransport += dust * weight;

    if (angularDistance(point.direction, anchorDirection) <= 45) {
      alignedWeight += weight;
    }
  }

  return {
    agriTransport: totalWeight > 0 ? agriTransport / totalWeight : 0.35,
    industrialTransport: totalWeight > 0 ? industrialTransport / totalWeight : 0.35,
    localRetention: totalWeight > 0 ? localRetention / totalWeight : 0.5,
    dustTransport: totalWeight > 0 ? dustTransport / totalWeight : 0.3,
    trajectoryConsistency: totalWeight > 0 ? alignedWeight / totalWeight : 0.5
  };
}

function buildWindTrajectoryPath(weather: OpenMeteoResponse, measurementTimestamp: string) {
  const measurementTime = new Date(measurementTimestamp).getTime();
  const times = weather.hourly?.time ?? [];
  const speeds = weather.hourly?.wind_speed_10m ?? [];
  const directions = weather.hourly?.wind_direction_10m ?? [];
  const segments = times
    .map((time, index) => ({
      time: new Date(time).getTime(),
      direction: directions[index],
      speed: speeds[index]
    }))
    .filter(
      (point): point is { time: number; direction: number; speed: number } =>
        Number.isFinite(point.time) &&
        typeof point.direction === "number" &&
        typeof point.speed === "number" &&
        point.time <= measurementTime &&
        measurementTime - point.time <= 24 * 60 * 60 * 1000
    )
    .sort((a, b) => b.time - a.time)
    .slice(0, 12);

  if (segments.length === 0) {
    return [] as [number, number][];
  }

  let cursor: [number, number] = [kathmandu.coordinates.lat, kathmandu.coordinates.lng];
  const points: [number, number][] = [cursor];

  for (const segment of segments) {
    const stepDistanceKm = clamp(segment.speed * 0.7, 4, 18);
    cursor = projectPoint(cursor[0], cursor[1], segment.direction, stepDistanceKm);
    points.push(cursor);
  }

  return points.reverse();
}

function getSeasonalPriors(measurementTimestamp: string) {
  const month = new Date(measurementTimestamp).getUTCMonth() + 1;

  if ([3, 4, 5].includes(month)) {
    return {
      agricultural: 0.2,
      industrial: 0.08,
      local: -0.05,
      dust: 0.16
    };
  }

  if ([12, 1, 2].includes(month)) {
    return {
      agricultural: 0.02,
      industrial: 0.08,
      local: 0.22,
      dust: -0.03
    };
  }

  if ([6, 7, 8, 9].includes(month)) {
    return {
      agricultural: -0.08,
      industrial: -0.04,
      local: 0.06,
      dust: -0.1
    };
  }

  return {
    agricultural: 0.05,
    industrial: 0.05,
    local: 0.08,
    dust: 0.02
  };
}

function isLikelyCropBurningWindow(timestamp: string) {
  const month = new Date(timestamp).getUTCMonth() + 1;
  return month === 4 || month === 5 || month === 10 || month === 11;
}

function normalizeContributors(candidates: ContributorCandidate[]) {
  const total = candidates.reduce((sum, item) => sum + item.score, 0);
  const normalized = candidates.map((item) => ({
    name: item.name,
    evidence: item.evidence,
    imported: item.imported,
    share: total > 0 ? Math.round((item.score / total) * 100) : Math.round(100 / candidates.length)
  }));
  const roundedTotal = normalized.reduce((sum, item) => sum + item.share, 0);

  if (normalized.length > 0 && roundedTotal !== 100) {
    normalized[0] = {
      ...normalized[0],
      share: clamp(normalized[0].share + (100 - roundedTotal), 0, 100)
    };
  }

  return normalized.sort((a, b) => b.share - a.share);
}

function buildAttributionSummary(input: {
  dominantSource: string;
  importedShare: number;
  windDirection: string;
  regime: AttributionRegime;
}) {
  if (input.regime.name === "Local accumulation") {
    return `${input.dominantSource} is leading under a local-accumulation regime, with weaker ventilation allowing PM2.5 to build inside the Kathmandu valley.`;
  }

  if (input.regime.name === "Fire-influenced transport") {
    return `${input.dominantSource} is leading under a fire-influenced transport regime, with ${input.windDirection} flow carrying upwind smoke toward Kathmandu.`;
  }

  if (input.regime.name === "Dust-influenced transport") {
    return `${input.dominantSource} is leading under a dust-influenced regime, with transport and surface lift both contributing to Kathmandu's particulate load.`;
  }

  if (input.importedShare >= 55 || input.regime.name === "Regional transport") {
    return `${input.dominantSource} likely drove most of the current PM2.5 load, with ${input.windDirection} transport sustaining imported haze into Kathmandu.`;
  }

  return `${input.dominantSource} appears to be the leading contributor, but current conditions still point to a mixed balance between imported haze and local valley emissions.`;
}

function buildConfidenceLevel(score: number): "Low" | "Medium" | "High" {
  if (score >= 0.72) return "High";
  if (score >= 0.48) return "Medium";
  return "Low";
}

function detectRegime(input: {
  localSpike: number;
  stagnation: number;
  transportStrength: number;
  persistence: number;
  agriCorridor: number;
  industrialCorridor: number;
  localShelterFlow: number;
  trajectory: TrajectoryEvidence;
  fireSignal: FireSignal;
  modelEvidence: ModelEvidenceContext;
  registryEvidence: RegistryEvidenceInternal;
  dustPotential: number;
  modeledDustBoost: number;
  aerosolSupport: number;
}): AttributionRegime & {
  localMultiplier: number;
  fireMultiplier: number;
  transportMultiplier: number;
  dustMultiplier: number;
} {
  const localScore =
    input.stagnation * 0.34 +
    input.localSpike * 0.2 +
    input.trajectory.localRetention * 0.18 +
    input.localShelterFlow * 0.16 +
    clamp(
      (input.registryEvidence.localKilnCount * 0.65 +
        input.registryEvidence.localIndustrialCount * 0.35) /
        24,
      0,
      1
    ) *
      0.12;
  const fireScore =
    input.fireSignal.hotspotDensity * 0.42 +
    clamp(input.fireSignal.croplandMatchCount / 10, 0, 1) * 0.24 +
    (input.agriCorridor * 0.2 + input.trajectory.agriTransport * 0.14) +
    input.transportStrength * 0.1;
  const transportScore =
    input.persistence * 0.24 +
    input.aerosolSupport * 0.22 +
    (input.industrialCorridor * 0.18 + input.trajectory.industrialTransport * 0.18) +
    input.transportStrength * 0.12 +
    clamp(input.modelEvidence.agreementScore, 0, 1) * 0.06;
  const dustScore =
    input.modeledDustBoost * 0.38 +
    input.dustPotential * 0.28 +
    input.trajectory.dustTransport * 0.18 +
    input.transportStrength * 0.1 +
    (1 - input.persistence) * 0.06;

  const ranked = [
    {
      name: "Local accumulation" as const,
      score: localScore,
      multipliers: {
        localMultiplier: 1.16,
        fireMultiplier: 0.9,
        transportMultiplier: 0.9,
        dustMultiplier: 0.88
      }
    },
    {
      name: "Fire-influenced transport" as const,
      score: fireScore,
      multipliers: {
        localMultiplier: 0.94,
        fireMultiplier: 1.22,
        transportMultiplier: 1.05,
        dustMultiplier: 0.9
      }
    },
    {
      name: "Regional transport" as const,
      score: transportScore,
      multipliers: {
        localMultiplier: 0.94,
        fireMultiplier: 0.98,
        transportMultiplier: 1.18,
        dustMultiplier: 0.92
      }
    },
    {
      name: "Dust-influenced transport" as const,
      score: dustScore,
      multipliers: {
        localMultiplier: 0.9,
        fireMultiplier: 0.86,
        transportMultiplier: 0.98,
        dustMultiplier: 1.24
      }
    }
  ].sort((a, b) => b.score - a.score);

  const top = ranked[0];
  const second = ranked[1];
  const separation = clamp(top.score - second.score, 0, 1);
  const confidence = clamp(top.score * 0.55 + separation * 0.45, 0, 1);

  if (top.score < 0.34 || separation < 0.08) {
    return {
      name: "Mixed transition",
      confidence: clamp(top.score * 0.45 + separation * 0.25 + 0.2, 0, 1),
      localMultiplier: 1,
      fireMultiplier: 1,
      transportMultiplier: 1,
      dustMultiplier: 1
    };
  }

  return {
    name: top.name,
    confidence,
    ...top.multipliers
  };
}

function smoothAttribution(
  current: AiAttributionResponse,
  recentSnapshots: ReturnType<typeof getRecentSnapshots>,
  windDirection: string
): AiAttributionResponse {
  const recent = recentSnapshots.slice(0, 8);

  if (recent.length === 0) {
    return current;
  }

  let importedAccumulator = current.importedShare * 0.62;
  let confidenceAccumulator = current.confidenceBreakdown.overall * 0.7;
  let weightTotal = 0.62;
  let volatilityAccumulator = 0;
  let volatilityWeightTotal = 0;
  const baselineImported = current.importedShare;

  recent.forEach((snapshot, index) => {
    const ageHours = clamp(
      (Date.now() - new Date(snapshot.generatedAt).getTime()) / (60 * 60 * 1000),
      0,
      72
    );
    const recencyWeight = Math.exp(-ageHours / 8) / (1 + index * 0.25);
    const confidenceWeight = confidenceLabelToScore(snapshot.confidence);
    const weight = recencyWeight * confidenceWeight;

    importedAccumulator += snapshot.importedShare * weight;
    confidenceAccumulator += confidenceLabelToScore(snapshot.confidence) * 100 * weight;
    weightTotal += weight;

    const swing = Math.abs(snapshot.importedShare - baselineImported);
    volatilityAccumulator += swing * weight;
    volatilityWeightTotal += weight;
  });

  const smoothedImported = clamp(Math.round(importedAccumulator / weightTotal), 0, 100);
  const smoothedLocal = 100 - smoothedImported;
  const averageSwing =
    volatilityWeightTotal > 0 ? volatilityAccumulator / volatilityWeightTotal : 0;
  const stabilityPenalty = clamp(averageSwing / 28, 0, 1) * 12;
  const smoothedOverall = clamp(
    Math.round(confidenceAccumulator / weightTotal - stabilityPenalty),
    0,
    100
  );
  const confidenceScore = smoothedOverall / 100;
  const confidence = buildConfidenceLevel(confidenceScore);
  const importedCurrent = current.sources
    .filter((source) => source.imported)
    .reduce((sum, source) => sum + source.share, 0);
  const localCurrent = 100 - importedCurrent;

  const sources = current.sources.map((source) => {
    if (source.imported) {
      const ratio = importedCurrent > 0 ? source.share / importedCurrent : 0;
      return {
        ...source,
        share: Math.round(smoothedImported * ratio)
      };
    }

    const ratio = localCurrent > 0 ? source.share / localCurrent : 0;
    return {
      ...source,
      share: Math.round(smoothedLocal * ratio)
    };
  });
  const totalShare = sources.reduce((sum, source) => sum + source.share, 0);

  if (sources.length > 0 && totalShare !== 100) {
    sources[0] = {
      ...sources[0],
      share: clamp(sources[0].share + (100 - totalShare), 0, 100)
    };
  }

  const dominantSource = sources[0]?.name ?? "Regional transport";

  return {
    ...current,
    importedShare: smoothedImported,
    localShare: smoothedLocal,
    confidence,
    confidenceBreakdown: {
      ...current.confidenceBreakdown,
      history: clamp(Math.round((current.confidenceBreakdown.history + smoothedOverall) / 2), 0, 100),
      overall: smoothedOverall
    },
    summary: buildAttributionSummary({
      dominantSource,
      importedShare: smoothedImported,
      windDirection,
      regime: current.regime
    }),
    sources: sources.sort((a, b) => b.share - a.share)
  };
}

function computeAttribution(input: {
  pm25: number;
  windDirection: string;
  windDirectionDegrees: number;
  windSpeedKph: number;
  measurementTimestamp: string;
  hourlyMeasurements: OpenAQHourlyMeasurement[];
  weather: OpenMeteoResponse;
  stationQuality?: number;
  agreementScore?: number;
  stationCount?: number;
  fireSignal?: FireSignal | null;
  modelEvidence?: ModelEvidenceContext | null;
  trajectoryEvidence?: TrajectoryEvidence | null;
  registryEvidence?: RegistryEvidenceInternal | null;
}): AiAttributionResponse {
  const severity = clamp(input.pm25 / 140, 0, 1);
  const transportStrength = clamp(input.windSpeedKph / 24, 0, 1);
  const stagnation = 1 - clamp(input.windSpeedKph / 18, 0, 1);
  const agriCorridor = corridorStrength(input.windDirectionDegrees, 225, 70);
  const industrialCorridor = corridorStrength(input.windDirectionDegrees, 255, 80);
  const localShelterFlow =
    1 - clamp(Math.max(agriCorridor, industrialCorridor) * 0.9, 0, 1);
  const measurementAgeHours = clamp(
    (Date.now() - new Date(input.measurementTimestamp).getTime()) / (60 * 60 * 1000),
    0,
    48
  );
  const freshness = measurementAgeHours <= 6 ? 1 : measurementAgeHours <= 12 ? 0.8 : 0.55;
  const pm25Stats = buildRecentPm25Stats(input.hourlyMeasurements, input.measurementTimestamp);
  const windStats = buildWindStats(input.weather);
  const trajectory =
    input.trajectoryEvidence?.available
      ? input.trajectoryEvidence
      : {
          provider: "wind-model" as const,
          available: true,
          originBearing: null,
          originDistanceKm: null,
          pathPoints: [],
          ...buildTrajectorySignals(input.weather, input.measurementTimestamp)
        };
  const seasonalPriors = getSeasonalPriors(input.measurementTimestamp);
  const stationQuality = input.stationQuality ?? 0.75;
  const agreementScore = input.agreementScore ?? 0.7;
  const stationCount = input.stationCount ?? 1;
  const registryEvidence = input.registryEvidence ?? {
    provider: "osm" as const,
    localKilnCount: 0,
    localIndustrialCount: 0,
    upwindIndustrialCount: 0,
    upwindKilnCount: 0,
    localKilns: [],
    localIndustrialSites: [],
    upwindIndustrialSites: [],
    upwindKilns: []
  };
  const modelEvidence = input.modelEvidence ?? {
    modeledPm25: null,
    modeledUsAqi: null,
    modeledDust: null,
    aerosolOpticalDepth: null,
    agreement: "Unavailable" as const,
    agreementScore: 0.5
  };
  const fireSignal = input.fireSignal ?? {
    hotspotCount: 0,
    hotspotDensity: 0,
    meanFrp: 0,
    cropBeltHotspotCount: 0,
    croplandMatchCount: 0,
    inferredSourceName: "Upwind fire activity",
    inferredEvidenceLabel: "No FIRMS hotspot evidence",
    hotspots: []
  };
  const fireSourceBoost =
    fireSignal.inferredSourceName === "Probable agricultural burning, northern India"
      ? 1
      : 0.35;
  const persistence = clamp(
    Math.max(pm25Stats.persistence, clamp(pm25Stats.avg24h / 95, 0, 1) * 0.8),
    0,
    1
  );
  const localSpike = pm25Stats.spikeFactor;
  const dustPotential =
    clamp((input.windSpeedKph - 10) / 16, 0, 1) * 0.8 + (1 - persistence) * 0.2;
  const modeledDustBoost = clamp((modelEvidence.modeledDust ?? 0) / 30, 0, 1);
  const aerosolSupport = clamp((modelEvidence.aerosolOpticalDepth ?? 0) / 0.45, 0, 1);
  const localRegistryBoost = clamp(
    (registryEvidence.localKilnCount * 0.7 + registryEvidence.localIndustrialCount * 0.3) /
      24,
    0,
    1
  );
  const industrialRegistryBoost = clamp(
    (registryEvidence.upwindIndustrialCount * 0.8 + registryEvidence.upwindKilnCount * 0.2) /
      20,
    0,
    1
  );
  const regime = detectRegime({
    localSpike,
    stagnation,
    transportStrength,
    persistence,
    agriCorridor,
    industrialCorridor,
    localShelterFlow,
    trajectory,
    fireSignal,
    modelEvidence,
    registryEvidence,
    dustPotential,
    modeledDustBoost,
    aerosolSupport
  });
  const localScore =
    0.72 +
    stagnation * 1.1 +
    (localShelterFlow * 0.45 + trajectory.localRetention * 0.35) +
    severity * 0.4 +
    localSpike * 0.55 +
    localRegistryBoost * 0.35 +
    seasonalPriors.local;
  const fireScore =
    0.4 +
    (agriCorridor * 0.55 + trajectory.agriTransport * 0.45) * 1.15 +
    transportStrength * 0.42 +
    persistence * 0.28 +
    severity * 0.18 +
    fireSignal.hotspotDensity * 0.95 * fireSourceBoost +
    clamp(fireSignal.meanFrp / 40, 0, 1) * 0.22 * fireSourceBoost +
    seasonalPriors.agricultural;
  const transportScore =
    0.45 +
    (industrialCorridor * 0.52 + trajectory.industrialTransport * 0.48) * 1.08 +
    transportStrength * 0.45 +
    persistence * 0.5 +
    severity * 0.28 +
    aerosolSupport * 0.28 +
    industrialRegistryBoost * 0.42 +
    seasonalPriors.industrial;
  const dustScore =
    0.25 +
    (dustPotential * 0.65 + trajectory.dustTransport * 0.35) * 1.05 +
    severity * 0.16 +
    transportStrength * 0.18 +
    modeledDustBoost * 0.42 +
    seasonalPriors.dust;
  const localScoreAdjusted = localScore * regime.localMultiplier;
  const fireScoreAdjusted = fireScore * regime.fireMultiplier;
  const transportScoreAdjusted = transportScore * regime.transportMultiplier;
  const dustScoreAdjusted = dustScore * regime.dustMultiplier;
  const contributors: ContributorCandidate[] = [];

  contributors.push({
    name:
      regime.name === "Local accumulation" || localSpike >= 0.28
        ? "Kathmandu valley accumulation"
        : registryEvidence.localKilnCount >= 4
          ? "Kathmandu local emissions + kilns"
          : input.windSpeedKph < 10
          ? "Kathmandu local emissions build-up"
          : "Kathmandu local emissions",
    evidence:
      registryEvidence.localKilnCount >= 4 || registryEvidence.localIndustrialCount >= 4
        ? `${registryEvidence.localKilnCount} mapped kilns and ${registryEvidence.localIndustrialCount} industrial sites reinforce local valley emissions`
        : input.windSpeedKph < 10
        ? modelEvidence.agreement === "Weak"
          ? "Observed PM2.5 is running hotter than the modeled background, supporting local build-up"
          : "Lighter valley winds favor local build-up from urban and kiln emissions"
        : "Local emissions remain material inside the valley basin",
    score: localScoreAdjusted,
    imported: false
  });

  if (fireSignal.hotspotCount > 0 && fireScoreAdjusted >= 0.35) {
    contributors.push({
      name:
        fireSignal.inferredSourceName === "Probable agricultural burning, northern India"
          ? `${input.windDirection} crop-belt fire corridor`
          : `${input.windDirection} upwind hotspot corridor`,
      evidence: `${fireSignal.inferredEvidenceLabel} with ${input.windDirection} transport`,
      score: fireScoreAdjusted,
      imported: true
    });
  }

  if (transportScoreAdjusted >= 0.4) {
    contributors.push({
      name: `${input.windDirection} regional haze transport`,
      evidence:
        industrialRegistryBoost >= 0.25
          ? `${registryEvidence.upwindIndustrialCount} mapped industrial sites align with the upwind corridor`
          : aerosolSupport >= 0.4
            ? `${Math.round(pm25Stats.avg12h || input.pm25)} ug/m3 recent mean with elevated modeled aerosol loading`
            : `${Math.round(pm25Stats.avg12h || input.pm25)} ug/m3 recent mean with sustained upwind transport`,
      score: transportScoreAdjusted,
      imported: true
    });
  }

  if (dustScoreAdjusted >= 0.28 || input.windSpeedKph >= 11) {
    contributors.push({
      name:
        input.windSpeedKph >= 11
          ? `${input.windDirection} wind-driven dust`
          : "Surface dust resuspension",
      evidence:
        modelEvidence.modeledDust && modelEvidence.modeledDust >= 10
          ? `Modeled dust ${modelEvidence.modeledDust} ug/m3 aligns with surface resuspension risk`
          : `${input.windSpeedKph} kph surface winds support particulate lift and resuspension`,
      score: dustScoreAdjusted,
      imported: input.windSpeedKph >= 11
    });
  }

  const sources = normalizeContributors(
    contributors
      .sort((a, b) => b.score - a.score)
      .slice(0, 4)
  );

  const importedShare = sources
    .filter((source) => source.imported)
    .reduce((sum, source) => sum + source.share, 0);
  const localShare = 100 - importedShare;
  const topShare = sources[0]?.share ?? 0;
  const secondShare = sources[1]?.share ?? 0;
  const scoreSeparation = clamp((topShare - secondShare) / 35, 0, 1);
  const evidenceCoverage = clamp(pm25Stats.count12h / 10, 0, 1);
  const confidenceScore = clamp(
    freshness * 0.2 +
      windStats.windConsistency * 0.14 +
      trajectory.trajectoryConsistency * 0.16 +
      evidenceCoverage * 0.2 +
      scoreSeparation * 0.18 +
      clamp(windStats.avg12hSpeed / 18, 0, 1) * 0.08 +
      persistence * 0.06 +
      stationQuality * 0.12 +
      agreementScore * 0.1 +
      modelEvidence.agreementScore * 0.12 +
      clamp(stationCount / 4, 0, 1) * 0.04 +
      clamp(fireSignal.hotspotCount / 40, 0, 1) * 0.04 +
      clamp(pm25Stats.count24h / 18, 0, 1) * 0.06 +
      regime.confidence * 0.06,
    0,
    1
  );
  const confidence = buildConfidenceLevel(confidenceScore);
  const confidenceBreakdown: ConfidenceBreakdown = {
    freshness: Math.round(freshness * 100),
    stationAgreement: Math.round(
      ((stationQuality * 0.55 +
        agreementScore * 0.35 +
        clamp(stationCount / 4, 0, 1) * 0.1) /
        1) *
        100
    ),
    modelAgreement: Math.round(modelEvidence.agreementScore * 100),
    transport: Math.round(
      ((windStats.windConsistency * 0.45 +
        trajectory.trajectoryConsistency * 0.4 +
        (trajectory.provider === "hysplit" ? 0.12 : 0) +
        clamp(windStats.avg12hSpeed / 18, 0, 1) * 0.15) /
        1.12) *
        100
    ),
    history: Math.round(
      ((evidenceCoverage * 0.6 +
        persistence * 0.15 +
        clamp(pm25Stats.count24h / 18, 0, 1) * 0.25) /
        1) *
        100
    ),
    hotspotSupport: Math.round(
      ((clamp(fireSignal.hotspotCount / 40, 0, 1) * 0.6 +
        clamp(fireSignal.croplandMatchCount / 8, 0, 1) * 0.4) /
        1) *
        100
    ),
    overall: Math.round(confidenceScore * 100)
  };
  const dominantSource = sources[0]?.name ?? "Regional transport";
  const summary = buildAttributionSummary({
    dominantSource,
    importedShare,
    windDirection: input.windDirection,
    regime
  });

  return {
    summary,
    importedShare,
    localShare,
    confidence,
    confidenceBreakdown,
    sources,
    regime
  };
}

async function fetchJson<T>(url: string, init?: RequestInit) {
  const response = await fetch(url, {
    ...init,
    cache: "no-store",
    next: { revalidate: 0 }
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

async function fetchNearestPm25Location() {
  const apiKey = process.env.OPENAQ_API_KEY;
  const cacheKey = "openaq:locations:np:pm25:v1";
  const cached = getSignalCacheEntry(cacheKey);

  if (cached && isSignalCacheFresh(cached.checkedAt, OPENAQ_LOCATION_CACHE_TTL_HOURS)) {
    try {
      const cachedLocations = JSON.parse(cached.payloadJson) as OpenAQLocation[];
      if (Array.isArray(cachedLocations) && cachedLocations.length > 0) {
        return cachedLocations;
      }
    } catch {
      // Ignore malformed cache payloads and refetch.
    }
  }

  if (!apiKey) {
    throw new SnapshotError("OPENAQ_API_KEY is not set", "env:openaq");
  }

  const query = new URLSearchParams({
    iso: "NP",
    limit: "500",
    parameters_id: `${PM25_PARAMETER_ID}`
  });

  let data: OpenAQListResponse<OpenAQLocation>;
  try {
    data = await fetchJson<OpenAQListResponse<OpenAQLocation>>(
      `${OPENAQ_BASE_URL}/locations?${query.toString()}`,
      {
        headers: {
          "X-API-Key": apiKey
        }
      }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown OpenAQ locations failure";

    if (
      message.includes("429") &&
      cached
    ) {
      try {
        const cachedLocations = JSON.parse(cached.payloadJson) as OpenAQLocation[];
        if (Array.isArray(cachedLocations) && cachedLocations.length > 0) {
          return cachedLocations;
        }
      } catch {
        // Ignore malformed cache payloads and rethrow the live failure.
      }
    }

    throw new SnapshotError(message, "openaq:locations");
  }

  const nepalLocations = data.results ?? [];
  const locations = [...nepalLocations].sort((a, b) => {
    const aScore = scoreLocationQuality(a);
    const bScore = scoreLocationQuality(b);

    return bScore.quality - aScore.quality || aScore.distanceKm - bScore.distanceKm;
  });

  if (locations.length === 0) {
    throw new SnapshotError("No OpenAQ locations found in Nepal", "openaq:locations-empty");
  }

  setSignalCacheEntry({
    cacheKey,
    signalKind: "openaq:locations",
    payloadJson: JSON.stringify(locations),
    checkedAt: new Date().toISOString()
  });

  return locations;
}

async function fetchLatestPm25(locationId: number) {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new SnapshotError("OPENAQ_API_KEY is not set", "env:openaq");
  }

  let data: OpenAQListResponse<OpenAQLatestMeasurement>;
  try {
    data = await fetchJson<OpenAQListResponse<OpenAQLatestMeasurement>>(
      `${OPENAQ_BASE_URL}/locations/${locationId}/latest?limit=100`,
      {
        headers: {
          "X-API-Key": apiKey
        }
      }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown OpenAQ latest failure";
    throw new SnapshotError(message, "openaq:latest");
  }

  const measurements = data.results ?? [];
  const measurement = await findPm25LatestMeasurement(measurements);

  if (!measurement) {
    throw new SnapshotError(
      `No latest PM2.5 measurement for location ${locationId}`,
      "openaq:latest-empty"
    );
  }

  return measurement;
}

async function fetchSensor(sensorId: number) {
  const cacheKey = `openaq:sensor:${sensorId}`;
  const cached = getSignalCacheEntry(cacheKey);

  if (cached && isSignalCacheFresh(cached.checkedAt, 24 * 30)) {
    try {
      return JSON.parse(cached.payloadJson) as OpenAQSensor | null;
    } catch {
      // Ignore malformed cache payloads and refetch.
    }
  }

  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new SnapshotError("OPENAQ_API_KEY is not set", "env:openaq");
  }

  const response = await fetchJson<OpenAQListResponse<OpenAQSensor>>(
    `${OPENAQ_BASE_URL}/sensors/${sensorId}`,
    {
      headers: {
        "X-API-Key": apiKey
      }
    }
  );

  const sensor = response.results?.[0] ?? null;

  setSignalCacheEntry({
    cacheKey,
    signalKind: "openaq:sensor",
    payloadJson: JSON.stringify(sensor),
    checkedAt: new Date().toISOString()
  });

  return sensor;
}

async function findPm25LatestMeasurement(measurements: OpenAQLatestMeasurement[]) {
  const numericMeasurements = measurements.filter(
    (measurement) =>
      typeof measurement.value === "number" && typeof measurement.sensorsId === "number"
  );

  for (const measurement of numericMeasurements) {
    try {
      const sensor = await fetchSensor(measurement.sensorsId!);
      if (sensor?.parameter?.id === PM25_PARAMETER_ID) {
        return measurement;
      }
    } catch {
      // Ignore sensor metadata failures and keep checking other sensors.
    }
  }

  return null;
}

function isFreshMeasurement(measurementTime: string, now = new Date()) {
  const measuredAt = new Date(measurementTime);
  const ageMs = now.getTime() - measuredAt.getTime();

  return ageMs >= 0 && ageMs <= MAX_MEASUREMENT_AGE_HOURS * 60 * 60 * 1000;
}

async function fetchBestLatestPm25() {
  const locations = await fetchNearestPm25Location();
  const failures: string[] = [];
  const candidates: Array<{
    location: OpenAQLocation;
    measurement: OpenAQLatestMeasurement;
    stationQuality: number;
    distanceKm: number;
  }> = [];

  for (const location of locations) {
    if (!location?.id) continue;

    try {
      const measurement = await fetchLatestPm25(location.id);
      const value = measurement.value;
      const timestamp = measurement.datetime?.utc;

      if (!Number.isFinite(value) || value < 0) {
        failures.push(`location ${location.id}: invalid value ${value}`);
        continue;
      }

      if (!timestamp) {
        failures.push(`location ${location.id}: missing timestamp`);
        continue;
      }

      if (!isFreshMeasurement(timestamp)) {
        failures.push(`location ${location.id}: stale timestamp ${timestamp}`);
        continue;
      }

      const station = scoreLocationQuality(location);

      candidates.push({
        location,
        measurement,
        stationQuality: station.quality,
        distanceKm: station.distanceKm
      });

      if (candidates.length >= 6) {
        break;
      }
    } catch (error) {
      failures.push(
        `location ${location.id}: ${error instanceof Error ? error.message : "unknown error"}`
      );
    }
  }

  if (candidates.length > 0) {
    const weighted = candidates.map((candidate) => {
      const freshnessHours =
        (Date.now() - new Date(candidate.measurement.datetime?.utc ?? 0).getTime()) /
        (60 * 60 * 1000);
      const freshnessScore =
        freshnessHours <= 3 ? 1 : freshnessHours <= 12 ? 0.8 : 0.55;
      const weight = clamp(
        candidate.stationQuality * 0.7 + freshnessScore * 0.3,
        0.1,
        1
      );

      return {
        ...candidate,
        freshnessHours,
        baseWeight: weight
      };
    });
    const candidateValues = weighted.map((candidate) => candidate.measurement.value);
    const medianValue = median(candidateValues);
    const absoluteDeviations = candidateValues.map((value) => Math.abs(value - medianValue));
    const mad = median(absoluteDeviations);
    const robustScale = Math.max(mad * 1.4826, 4);
    const robustWeighted = weighted.map((candidate) => {
      const consensusDelta = candidate.measurement.value - medianValue;
      const zScore = Math.abs(consensusDelta) / robustScale;
      const outlierPenalty =
        zScore <= 1 ? 1 : zScore <= 2.5 ? clamp(1 - (zScore - 1) / 1.5 * 0.55, 0.45, 1) : 0.18;
      const adjustedWeight = clamp(candidate.baseWeight * outlierPenalty, 0.05, 1);

      return {
        ...candidate,
        consensusDelta,
        outlierPenalty,
        weight: adjustedWeight
      };
    });
    const weights = robustWeighted.map((candidate) => candidate.weight);
    const consensusValue = weightedAverage(candidateValues, weights);
    const disagreement = weightedStandardDeviation(candidateValues, weights, consensusValue);
    const spreadUgM3 = Math.round(disagreement * 10) / 10;
    const primary = [...robustWeighted].sort(
      (a, b) => b.weight - a.weight || a.distanceKm - b.distanceKm
    )[0];
    const outlierCount = robustWeighted.filter((candidate) => candidate.outlierPenalty < 0.7).length;
    const agreementScore = clamp(1 - disagreement / 16 - outlierCount * 0.08, 0, 1);
    const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);

    return {
      location: primary.location,
      measurement: {
        ...primary.measurement,
        value: consensusValue
      },
      stationQuality:
        robustWeighted.reduce(
          (sum, candidate) => sum + candidate.stationQuality * candidate.weight,
          0
        ) / totalWeight,
      distanceKm:
        robustWeighted.reduce((sum, candidate) => sum + candidate.distanceKm * candidate.weight, 0) /
        totalWeight,
      agreementScore,
      spreadUgM3,
      outlierCount,
      stationCount: robustWeighted.length,
      stations: robustWeighted.map((candidate) => ({
        locationId: candidate.location.id,
        sensorId: candidate.measurement.sensorsId ?? null,
        label:
          candidate.location.locality ??
          candidate.location.name ??
          `Location ${candidate.location.id}`,
        pm25: Math.round(candidate.measurement.value),
        updatedAt: candidate.measurement.datetime?.utc ?? new Date().toISOString(),
        latitude: candidate.location.coordinates?.latitude ?? null,
        longitude: candidate.location.coordinates?.longitude ?? null,
        distanceKm: Math.round(candidate.distanceKm),
        stationQuality: Math.round(candidate.stationQuality * 100),
        weight: Math.round(candidate.weight * 100),
        freshnessHours: Math.round(candidate.freshnessHours * 10) / 10,
        consensusDelta: Math.round(candidate.consensusDelta * 10) / 10,
        outlierPenalty: Math.round(candidate.outlierPenalty * 100)
      }))
    };
  }

  throw new SnapshotError(
    `No valid Kathmandu PM2.5 measurement found. ${failures.join(" | ")}`,
    "openaq:latest-invalid"
  );
}

async function fetchHourlyPm25Range(
  sensorId: number,
  datetimeFrom: string,
  datetimeTo: string
) {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new SnapshotError("OPENAQ_API_KEY is not set", "env:openaq");
  }

  const query = new URLSearchParams({
    datetime_from: datetimeFrom,
    datetime_to: datetimeTo,
    limit: "1000"
  });

  let data: OpenAQListResponse<OpenAQHourlyMeasurement>;
  try {
    data = await fetchJson<OpenAQListResponse<OpenAQHourlyMeasurement>>(
      `${OPENAQ_BASE_URL}/sensors/${sensorId}/hours?${query.toString()}`,
      {
        headers: {
          "X-API-Key": apiKey
        }
      }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown OpenAQ hourly failure";
    throw new SnapshotError(message, "openaq:hours");
  }

  return data.results ?? [];
}

async function fetchHourlyPm25(sensorId: number, datetimeTo: string) {
  const end = new Date(datetimeTo);
  const start = new Date(end.getTime() - 48 * 60 * 60 * 1000);
  return fetchHourlyPm25Range(sensorId, start.toISOString(), end.toISOString());
}

async function fetchWindData() {
  const query = new URLSearchParams({
    latitude: `${kathmandu.coordinates.lat}`,
    longitude: `${kathmandu.coordinates.lng}`,
    current: "wind_speed_10m,wind_direction_10m",
    hourly: "wind_speed_10m,wind_direction_10m",
    wind_speed_unit: "kmh",
    timezone: "auto",
    forecast_days: "3",
    past_days: "2"
  });

  try {
    return await fetchJson<OpenMeteoResponse>(
      `${OPEN_METEO_BASE_URL}/v1/forecast?${query.toString()}`
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Open-Meteo failure";
    throw new SnapshotError(message, "open-meteo:forecast");
  }
}

async function fetchAirQualityData() {
  const query = new URLSearchParams({
    latitude: `${kathmandu.coordinates.lat}`,
    longitude: `${kathmandu.coordinates.lng}`,
    current: "pm2_5,us_aqi,dust,aerosol_optical_depth",
    hourly: "pm2_5,us_aqi,dust,aerosol_optical_depth",
    timezone: "auto",
    forecast_days: "2",
    past_days: "2"
  });

  try {
    return await fetchJson<OpenMeteoAirQualityResponse>(
      `${OPEN_METEO_AIR_QUALITY_BASE_URL}/v1/air-quality?${query.toString()}`
    );
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown Open-Meteo air quality failure";
    throw new SnapshotError(message, "open-meteo:air-quality");
  }
}

async function fetchHistoricalWindData(startDate: string, endDate: string) {
  const query = new URLSearchParams({
    latitude: `${kathmandu.coordinates.lat}`,
    longitude: `${kathmandu.coordinates.lng}`,
    hourly: "wind_speed_10m,wind_direction_10m",
    wind_speed_unit: "kmh",
    timezone: "GMT",
    start_date: startDate,
    end_date: endDate
  });

  try {
    return await fetchJson<OpenMeteoResponse>(
      `${OPEN_METEO_ARCHIVE_BASE_URL}/v1/archive?${query.toString()}`
    );
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown Open-Meteo archive failure";
    throw new SnapshotError(message, "open-meteo:archive");
  }
}

async function fetchHistoricalAirQualityData(startDate: string, endDate: string) {
  const query = new URLSearchParams({
    latitude: `${kathmandu.coordinates.lat}`,
    longitude: `${kathmandu.coordinates.lng}`,
    hourly: "pm2_5,us_aqi,dust,aerosol_optical_depth",
    timezone: "GMT",
    start_date: startDate,
    end_date: endDate
  });

  try {
    return await fetchJson<OpenMeteoAirQualityResponse>(
      `${OPEN_METEO_AIR_QUALITY_BASE_URL}/v1/air-quality?${query.toString()}`
    );
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Unknown Open-Meteo historical air quality failure";
    throw new SnapshotError(message, "open-meteo:air-quality-history");
  }
}

function getHotspotCacheKey(hotspot: FireHotspot) {
  return `${hotspot.lat.toFixed(3)},${hotspot.lng.toFixed(3)}`;
}

function isCroplandCacheFresh(checkedAt: string) {
  const ageMs = Date.now() - new Date(checkedAt).getTime();
  return ageMs >= 0 && ageMs <= CROPLAND_CACHE_TTL_HOURS * 60 * 60 * 1000;
}

async function hotspotHasNearbyCropland(hotspot: FireHotspot) {
  const query = `
[out:json][timeout:12];
(
  way["landuse"="farmland"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  relation["landuse"="farmland"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  way["landuse"="orchard"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  relation["landuse"="orchard"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  way["landuse"="vineyard"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  relation["landuse"="vineyard"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  way["landuse"="plant_nursery"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
  relation["landuse"="plant_nursery"](around:${CROPLAND_PROXIMITY_METERS},${hotspot.lat},${hotspot.lng});
);
out ids 1;
`.trim();

  const response = await fetch(OVERPASS_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    },
    body: new URLSearchParams({ data: query }).toString(),
    cache: "no-store",
    next: { revalidate: 0 }
  });

  if (!response.ok) {
    throw new Error(`Overpass request failed: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as { elements?: unknown[] };
  return Array.isArray(payload.elements) && payload.elements.length > 0;
}

async function annotateCroplandMatchedHotspots(hotspots: FireHotspot[]) {
  if (hotspots.length === 0) {
    return {
      matchCount: 0,
      hotspots
    };
  }

  const sampledHotspots = [...hotspots]
    .sort((a, b) => b.frp - a.frp)
    .slice(0, MAX_CROPLAND_HOTSPOT_SAMPLES);
  const annotatedHotspots: FireHotspot[] = hotspots.map((hotspot) => ({
    ...hotspot,
    nearCropland: null
  }));
  let matches = 0;

  for (const hotspot of sampledHotspots) {
    const hotspotKey = getHotspotCacheKey(hotspot);
    const cached = getCroplandCacheEntry(hotspotKey);

    if (cached && isCroplandCacheFresh(cached.checkedAt)) {
      const nearCropland = cached.nearCropland === 1;
      for (const candidate of annotatedHotspots) {
        if (getHotspotCacheKey(candidate) === hotspotKey) {
          candidate.nearCropland = nearCropland;
        }
      }
      if (cached.nearCropland === 1) {
        matches += 1;
      }
      continue;
    }

    try {
      const nearCropland = await hotspotHasNearbyCropland(hotspot);

      setCroplandCacheEntry({
        hotspotKey,
        latitude: hotspot.lat,
        longitude: hotspot.lng,
        nearCropland,
        checkedAt: new Date().toISOString()
      });

      for (const candidate of annotatedHotspots) {
        if (getHotspotCacheKey(candidate) === hotspotKey) {
          candidate.nearCropland = nearCropland;
        }
      }

      if (nearCropland) {
        matches += 1;
      }
    } catch {
      return {
        matchCount: 0,
        hotspots: annotatedHotspots
      };
    }
  }

  return {
    matchCount: matches,
    hotspots: annotatedHotspots
  };
}

function getRegistryMatchLabel(
  kind: "industrial" | "kiln",
  tags?: Record<string, string>
) {
  const explicitName = tags?.name?.trim();
  if (explicitName) {
    return explicitName;
  }

  if (kind === "kiln") {
    return "Mapped kiln";
  }

  return "Mapped industrial site";
}

function extractOverpassPoint(
  element: {
    lat?: number;
    lon?: number;
    center?: {
      lat?: number;
      lon?: number;
    };
    tags?: Record<string, string>;
  },
  kind: "industrial" | "kiln"
) : RegistryMatch | null {
  const lat = element.lat ?? element.center?.lat;
  const lng = element.lon ?? element.center?.lon;

  if (typeof lat !== "number" || typeof lng !== "number") {
    return null;
  }

  return {
    lat,
    lng,
    label: getRegistryMatchLabel(kind, element.tags)
  };
}

async function fetchRegistryMatches(
  kind: "industrial" | "kiln",
  center: [number, number],
  radiusMeters: number
) {
  const cacheKey = getRegistryCacheKey(kind, center, radiusMeters);
  const cached = getSignalCacheEntry(cacheKey);

  if (cached && isSignalCacheFresh(cached.checkedAt)) {
    try {
      const payload = JSON.parse(cached.payloadJson) as {
        count?: number;
        points?: RegistryMatch[];
      };
      if (typeof payload.count === "number" && Array.isArray(payload.points)) {
        return {
          count: payload.count,
          points: payload.points
        };
      }
    } catch {
      // Ignore malformed cache payloads and refresh.
    }
  }

  const [lat, lng] = center;
  const query =
    kind === "industrial"
      ? `
[out:json][timeout:18];
(
  way["landuse"="industrial"](around:${radiusMeters},${lat},${lng});
  relation["landuse"="industrial"](around:${radiusMeters},${lat},${lng});
  node["industrial"](around:${radiusMeters},${lat},${lng});
  way["industrial"](around:${radiusMeters},${lat},${lng});
  relation["industrial"](around:${radiusMeters},${lat},${lng});
);
out center;
`.trim()
      : `
[out:json][timeout:18];
(
  node["man_made"="kiln"]["product"~"brick|bricks",i](around:${radiusMeters},${lat},${lng});
  way["man_made"="kiln"]["product"~"brick|bricks",i](around:${radiusMeters},${lat},${lng});
  relation["man_made"="kiln"]["product"~"brick|bricks",i](around:${radiusMeters},${lat},${lng});
  node["industrial"="brickworks"](around:${radiusMeters},${lat},${lng});
  way["industrial"="brickworks"](around:${radiusMeters},${lat},${lng});
  relation["industrial"="brickworks"](around:${radiusMeters},${lat},${lng});
);
out center;
`.trim();

  const response = await fetch(OVERPASS_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    },
    body: new URLSearchParams({ data: query }).toString(),
    cache: "no-store",
    next: { revalidate: 0 }
  });

  if (!response.ok) {
    throw new Error(`Overpass request failed: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as {
    elements?: Array<{
      lat?: number;
      lon?: number;
      center?: {
        lat?: number;
        lon?: number;
      };
      tags?: Record<string, string>;
    }>;
  };
  const elements = Array.isArray(payload.elements) ? payload.elements : [];
  const uniquePoints = new Map<string, RegistryMatch>();

  for (const element of elements) {
    const point = extractOverpassPoint(element, kind);

    if (!point) {
      continue;
    }

    const key = `${point.lat.toFixed(4)},${point.lng.toFixed(4)}`;
    if (!uniquePoints.has(key) && uniquePoints.size < MAX_REGISTRY_POINTS) {
      uniquePoints.set(key, point);
    }
  }

  const count = elements.length;
  const points = [...uniquePoints.values()];

  setSignalCacheEntry({
    cacheKey,
    signalKind: `registry:${kind}`,
    payloadJson: JSON.stringify({ count, points }),
    checkedAt: new Date().toISOString()
  });

  return {
    count,
    points
  };
}

async function fetchSourceRegistryEvidence(
  upwindBearingDegrees: number
): Promise<RegistryEvidenceInternal> {
  const localCenter: [number, number] = [
    kathmandu.coordinates.lat,
    kathmandu.coordinates.lng
  ];
  const upwindCenter = projectFromKathmandu(upwindBearingDegrees, 120);

  try {
    const [
      localKilnCount,
      localIndustrialCount,
      upwindIndustrialCount,
      upwindKilnCount
    ] = await Promise.all([
      fetchRegistryMatches("kiln", localCenter, LOCAL_REGISTRY_RADIUS_METERS),
      fetchRegistryMatches("industrial", localCenter, LOCAL_REGISTRY_RADIUS_METERS),
      fetchRegistryMatches("industrial", upwindCenter, UPWIND_REGISTRY_RADIUS_METERS),
      fetchRegistryMatches("kiln", upwindCenter, UPWIND_REGISTRY_RADIUS_METERS)
    ]);

    return {
      provider: "osm",
      localKilnCount: localKilnCount.count,
      localIndustrialCount: localIndustrialCount.count,
      upwindIndustrialCount: upwindIndustrialCount.count,
      upwindKilnCount: upwindKilnCount.count,
      localKilns: localKilnCount.points,
      localIndustrialSites: localIndustrialCount.points,
      upwindIndustrialSites: upwindIndustrialCount.points,
      upwindKilns: upwindKilnCount.points
    };
  } catch {
    return {
      provider: "osm",
      localKilnCount: 0,
      localIndustrialCount: 0,
      upwindIndustrialCount: 0,
      upwindKilnCount: 0,
      localKilns: [],
      localIndustrialSites: [],
      upwindIndustrialSites: [],
      upwindKilns: []
    };
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function buildTrajectoryEvidenceFromPoints(points: TrajectoryPoint[]): TrajectoryEvidence {
  if (points.length === 0) {
    return {
      provider: "wind-model",
      available: false,
      originBearing: null,
      originDistanceKm: null,
      pathPoints: [],
      agriTransport: 0.35,
      industrialTransport: 0.35,
      localRetention: 0.5,
      dustTransport: 0.3,
      trajectoryConsistency: 0.5
    };
  }

  const farthestPoint = [...points].sort((a, b) => b.distanceKm - a.distanceKm)[0];
  const anchorBearing = farthestPoint?.bearing ?? points[points.length - 1].bearing;
  let totalWeight = 0;
  let agriTransport = 0;
  let industrialTransport = 0;
  let localRetention = 0;
  let dustTransport = 0;
  let alignedWeight = 0;
  const pathPoints = [...points]
    .sort((a, b) => b.distanceKm - a.distanceKm)
    .map((point) => [point.latitude, point.longitude] as [number, number]);

  pathPoints.push([kathmandu.coordinates.lat, kathmandu.coordinates.lng]);

  for (const point of points) {
    const recencyWeight = clamp(1 - Math.abs(point.ageHours) / 78, 0.12, 1);
    const distanceWeight = clamp(point.distanceKm / 160, 0.1, 1);
    const weight = recencyWeight * (0.55 + distanceWeight * 0.45);
    const agri = corridorStrength(point.bearing, 225, 70);
    const industrial = corridorStrength(point.bearing, 255, 80);
    const local = clamp(1 - point.distanceKm / 140, 0, 1);
    const dust = corridorStrength(point.bearing, 240, 95) * clamp(point.distanceKm / 90, 0, 1);

    totalWeight += weight;
    agriTransport += agri * weight;
    industrialTransport += industrial * weight;
    localRetention += local * weight;
    dustTransport += dust * weight;

    if (angularDistance(point.bearing, anchorBearing) <= 45) {
      alignedWeight += weight;
    }
  }

  return {
    provider: "hysplit",
    available: true,
    originBearing: Math.round(anchorBearing),
    originDistanceKm: Math.round(farthestPoint.distanceKm),
    pathPoints,
    agriTransport: totalWeight > 0 ? agriTransport / totalWeight : 0.35,
    industrialTransport: totalWeight > 0 ? industrialTransport / totalWeight : 0.35,
    localRetention: totalWeight > 0 ? localRetention / totalWeight : 0.5,
    dustTransport: totalWeight > 0 ? dustTransport / totalWeight : 0.3,
    trajectoryConsistency: totalWeight > 0 ? alignedWeight / totalWeight : 0.5
  };
}

function parseHysplitTdump(contents: string) {
  const lines = contents
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 6) {
    return [] as TrajectoryPoint[];
  }

  let index = 0;
  const numGrids = Number.parseInt(lines[index].split(/\s+/)[0] ?? "0", 10);
  if (!Number.isFinite(numGrids)) {
    return [] as TrajectoryPoint[];
  }
  index += 1 + numGrids;

  const numTraj = Number.parseInt(lines[index]?.split(/\s+/)[0] ?? "0", 10);
  if (!Number.isFinite(numTraj)) {
    return [] as TrajectoryPoint[];
  }
  index += 1 + numTraj;

  const numDiag = Number.parseInt(lines[index]?.split(/\s+/)[0] ?? "0", 10);
  if (!Number.isFinite(numDiag)) {
    return [] as TrajectoryPoint[];
  }
  index += 1;

  const points: TrajectoryPoint[] = [];

  for (const line of lines.slice(index)) {
    const tokens = line.split(/\s+/);
    if (tokens.length < 12 + Math.max(numDiag, 0)) {
      continue;
    }

    const ageHours = Number(tokens[8]);
    const latitude = Number(tokens[9]);
    const longitude = Number(tokens[10]);
    const heightM = Number(tokens[11]);

    if (
      !Number.isFinite(ageHours) ||
      !Number.isFinite(latitude) ||
      !Number.isFinite(longitude) ||
      !Number.isFinite(heightM)
    ) {
      continue;
    }

    points.push({
      ageHours,
      latitude,
      longitude,
      heightM,
      bearing: bearingDegrees(
        kathmandu.coordinates.lat,
        kathmandu.coordinates.lng,
        latitude,
        longitude
      ),
      distanceKm: haversineKm(
        kathmandu.coordinates.lat,
        kathmandu.coordinates.lng,
        latitude,
        longitude
      )
    });
  }

  return points;
}

async function fetchHysplitTrajectory(
  measurementTimestamp: string
): Promise<TrajectoryEvidence | null> {
  const authHeaderName = process.env.HYSPLIT_AUTH_HEADER_NAME;
  const authHeaderValue = process.env.HYSPLIT_AUTH_HEADER_VALUE;

  if (!authHeaderName || !authHeaderValue) {
    return null;
  }

  const cacheKey = getTrajectoryCacheKey(measurementTimestamp);
  const cached = getSignalCacheEntry(cacheKey);
  if (cached && isSignalCacheFresh(cached.checkedAt, 24 * 30)) {
    try {
      return JSON.parse(cached.payloadJson) as TrajectoryEvidence;
    } catch {
      // Ignore malformed cache payloads and refetch.
    }
  }

  const startDate = new Date(measurementTimestamp);
  const requestBody = {
    meteorologicalData: process.env.HYSPLIT_METEO_DATA ?? "GFS",
    direction: "backward",
    duration: 48,
    motion: "actual",
    latitude: kathmandu.coordinates.lat,
    longitude: kathmandu.coordinates.lng,
    levels: [500, 1000, 1500],
    startDate: startDate.toISOString().slice(0, 10),
    startHour: startDate.getUTCHours(),
    endpointInterval: 60,
    graphic: ["none"]
  };
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
    [authHeaderName]: authHeaderValue
  };

  try {
    const createResponse = await fetch(`${HYSPLIT_BASE_URL}/api/v1/traj`, {
      method: "POST",
      headers,
      body: JSON.stringify(requestBody),
      cache: "no-store",
      next: { revalidate: 0 }
    });

    if (!createResponse.ok) {
      return null;
    }

    const createPayload = (await createResponse.json()) as {
      uuid?: string;
      id?: string;
      result?: { uuid?: string };
    };
    const uuid = createPayload.uuid ?? createPayload.id ?? createPayload.result?.uuid;
    if (!uuid) {
      return null;
    }

    let completed = false;
    for (let attempt = 0; attempt < 12; attempt += 1) {
      const statusResponse = await fetch(`${HYSPLIT_BASE_URL}/api/v1/traj/status/${uuid}`, {
        headers,
        cache: "no-store",
        next: { revalidate: 0 }
      });

      if (!statusResponse.ok) {
        break;
      }

      const statusPayload = (await statusResponse.json()) as {
        status?: string;
        state?: string;
      };
      const status = statusPayload.status ?? statusPayload.state ?? "";
      if (status.toUpperCase() === "COMPLETED") {
        completed = true;
        break;
      }

      if (status.toUpperCase() === "FAILED") {
        break;
      }

      await sleep(750);
    }

    if (!completed) {
      return null;
    }

    const downloadResponse = await fetch(`${HYSPLIT_BASE_URL}/api/v1/traj/download/${uuid}`, {
      headers,
      cache: "no-store",
      next: { revalidate: 0 }
    });

    if (!downloadResponse.ok) {
      return null;
    }

    const zipBuffer = await downloadResponse.arrayBuffer();
    const zip = await JSZip.loadAsync(zipBuffer);
    const tdumpFile = Object.values(zip.files).find((file) =>
      file.name.toLowerCase().includes("tdump")
    );

    if (!tdumpFile) {
      return null;
    }

    const tdumpContents = await tdumpFile.async("string");
    const trajectory = buildTrajectoryEvidenceFromPoints(parseHysplitTdump(tdumpContents));

    setSignalCacheEntry({
      cacheKey,
      signalKind: "trajectory:hysplit",
      payloadJson: JSON.stringify(trajectory),
      checkedAt: new Date().toISOString()
    });

    return trajectory;
  } catch {
    return null;
  }
}

async function fetchFireSignal(): Promise<FireSignal | null> {
  const mapKey = process.env.FIRMS_MAP_KEY;

  if (!mapKey) {
    return null;
  }

  const url = `${FIRMS_BASE_URL}/${mapKey}/${FIRMS_SOURCE}/${FIRMS_UPWIND_BBOX}/2`;

  let response: Response;
  try {
    response = await fetch(url, {
      cache: "no-store",
      next: { revalidate: 0 }
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown FIRMS failure";
    throw new SnapshotError(message, "firms:fetch");
  }

  if (!response.ok) {
    const text = await response.text();
    throw new SnapshotError(
      `FIRMS request failed: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
      "firms:fetch"
    );
  }

  const csv = await response.text();
  const lines = csv
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length <= 1) {
    return {
      hotspotCount: 0,
      hotspotDensity: 0,
      meanFrp: 0,
      cropBeltHotspotCount: 0,
      croplandMatchCount: 0,
      inferredSourceName: "Upwind fire activity",
      inferredEvidenceLabel: "No FIRMS hotspot evidence",
      hotspots: []
    };
  }

  const headers = parseCsvLine(lines[0]).map((header) => header.trim().toLowerCase());
  const confidenceIndex = headers.indexOf("confidence");
  const frpIndex = headers.indexOf("frp");
  const latIndex = headers.indexOf("latitude");
  const lngIndex = headers.indexOf("longitude");
  const dateIndex = headers.indexOf("acq_date");
  let hotspotCount = 0;
  let frpSum = 0;
  let cropBeltHotspotCount = 0;
  let latestDate = "";
  const hotspots: FireHotspot[] = [];

  for (const line of lines.slice(1)) {
    const values = parseCsvLine(line);
    const confidence = confidenceIndex >= 0 ? values[confidenceIndex]?.toLowerCase() ?? "" : "";
    const frp = frpIndex >= 0 ? Number(values[frpIndex]) : 0;
    const lat = latIndex >= 0 ? Number(values[latIndex]) : Number.NaN;
    const lng = lngIndex >= 0 ? Number(values[lngIndex]) : Number.NaN;
    const acqDate = dateIndex >= 0 ? values[dateIndex] ?? "" : "";

    if (confidence && confidence.startsWith("l")) {
      continue;
    }

    hotspotCount += 1;
    if (acqDate && acqDate > latestDate) {
      latestDate = acqDate;
    }
    if (Number.isFinite(frp)) {
      frpSum += frp;
    }
    if (
      Number.isFinite(lat) &&
      Number.isFinite(lng) &&
      isWithinBounds(lat, lng, NORTH_INDIA_CROP_BELT_BBOX)
    ) {
      cropBeltHotspotCount += 1;
    }
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      hotspots.push({
        lat,
        lng,
        frp: Number.isFinite(frp) ? frp : 0,
        date: acqDate
      });
    }
  }

  const { matchCount: croplandMatchCount, hotspots: annotatedHotspots } =
    await annotateCroplandMatchedHotspots(hotspots);
  const cropBeltDensity = clamp(cropBeltHotspotCount / 40, 0, 1);
  const croplandMatchRatio =
    hotspots.length > 0
      ? croplandMatchCount / Math.min(hotspots.length, MAX_CROPLAND_HOTSPOT_SAMPLES)
      : 0;
  const cropBurningWindow = latestDate
    ? isLikelyCropBurningWindow(`${latestDate}T00:00:00Z`)
    : false;
  const probableAgriculturalBurning =
    cropBurningWindow &&
    cropBeltHotspotCount >= 10 &&
    cropBeltDensity >= 0.2 &&
    croplandMatchRatio >= 0.35;

  return {
    hotspotCount,
    hotspotDensity: clamp(hotspotCount / 80, 0, 1),
    meanFrp: hotspotCount > 0 ? frpSum / hotspotCount : 0,
    cropBeltHotspotCount,
    croplandMatchCount,
    inferredSourceName: probableAgriculturalBurning
      ? "Probable agricultural burning, northern India"
      : "Upwind fire activity",
    inferredEvidenceLabel: probableAgriculturalBurning
      ? `${cropBeltHotspotCount} FIRMS hotspots with ${croplandMatchCount} cropland-aligned matches`
      : croplandMatchCount > 0
        ? `${hotspotCount} FIRMS hotspots, ${croplandMatchCount} near mapped cropland`
        : `${hotspotCount} FIRMS hotspots in the upwind corridor`,
    hotspots: annotatedHotspots
      .sort((a, b) => b.frp - a.frp)
      .slice(0, MAX_HOTSPOTS_FOR_MAP)
      .map((hotspot) => ({
        lat: hotspot.lat,
        lng: hotspot.lng,
        frp: hotspot.frp,
        date: hotspot.date,
        nearCropland: hotspot.nearCropland ?? null
      }))
  };
}

async function fetchHistoricalFireSignals(days: number) {
  const mapKey = process.env.FIRMS_MAP_KEY;

  if (!mapKey) {
    return new Map<string, FireSignal>();
  }

  const url = `${FIRMS_BASE_URL}/${mapKey}/${FIRMS_SOURCE}/${FIRMS_UPWIND_BBOX}/${days}`;

  try {
    const response = await fetch(url, {
      cache: "no-store",
      next: { revalidate: 0 }
    });

    if (!response.ok) {
      return new Map<string, FireSignal>();
    }

    const csv = await response.text();
    const lines = csv
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    if (lines.length <= 1) {
      return new Map<string, FireSignal>();
    }

    const headers = parseCsvLine(lines[0]).map((header) => header.trim().toLowerCase());
    const confidenceIndex = headers.indexOf("confidence");
    const frpIndex = headers.indexOf("frp");
    const latIndex = headers.indexOf("latitude");
    const lngIndex = headers.indexOf("longitude");
    const dateIndex = headers.indexOf("acq_date");
    const grouped = new Map<string, FireHotspot[]>();

    for (const line of lines.slice(1)) {
      const values = parseCsvLine(line);
      const confidence = confidenceIndex >= 0 ? values[confidenceIndex]?.toLowerCase() ?? "" : "";
      const frp = frpIndex >= 0 ? Number(values[frpIndex]) : 0;
      const lat = latIndex >= 0 ? Number(values[latIndex]) : Number.NaN;
      const lng = lngIndex >= 0 ? Number(values[lngIndex]) : Number.NaN;
      const acqDate = dateIndex >= 0 ? values[dateIndex] ?? "" : "";

      if (confidence && confidence.startsWith("l")) {
        continue;
      }

      if (!acqDate || !Number.isFinite(lat) || !Number.isFinite(lng)) {
        continue;
      }

      const bucket = grouped.get(acqDate) ?? [];
      bucket.push({
        lat,
        lng,
        frp: Number.isFinite(frp) ? frp : 0,
        date: acqDate
      });
      grouped.set(acqDate, bucket);
    }

    const dailySignals = new Map<string, FireSignal>();

    for (const [date, hotspots] of grouped.entries()) {
      const hotspotCount = hotspots.length;
      const frpSum = hotspots.reduce((sum, hotspot) => sum + hotspot.frp, 0);
      const cropBeltHotspotCount = hotspots.filter((hotspot) =>
        isWithinBounds(hotspot.lat, hotspot.lng, NORTH_INDIA_CROP_BELT_BBOX)
      ).length;
      const { matchCount: croplandMatchCount } = await annotateCroplandMatchedHotspots(hotspots);
      const cropBeltDensity = clamp(cropBeltHotspotCount / 40, 0, 1);
      const croplandMatchRatio =
        hotspots.length > 0
          ? croplandMatchCount / Math.min(hotspots.length, MAX_CROPLAND_HOTSPOT_SAMPLES)
          : 0;
      const probableAgriculturalBurning =
        isLikelyCropBurningWindow(`${date}T00:00:00Z`) &&
        cropBeltHotspotCount >= 10 &&
        cropBeltDensity >= 0.2 &&
        croplandMatchRatio >= 0.35;

      dailySignals.set(date, {
        hotspotCount,
        hotspotDensity: clamp(hotspotCount / 80, 0, 1),
        meanFrp: hotspotCount > 0 ? frpSum / hotspotCount : 0,
        cropBeltHotspotCount,
        croplandMatchCount,
        inferredSourceName: probableAgriculturalBurning
          ? "Probable agricultural burning, northern India"
          : "Upwind fire activity",
        inferredEvidenceLabel: probableAgriculturalBurning
          ? `${cropBeltHotspotCount} FIRMS hotspots with ${croplandMatchCount} cropland-aligned matches`
          : croplandMatchCount > 0
            ? `${hotspotCount} FIRMS hotspots, ${croplandMatchCount} near mapped cropland`
            : `${hotspotCount} FIRMS hotspots in the upwind corridor`,
        hotspots: []
      });
    }

    return dailySignals;
  } catch {
    return new Map<string, FireSignal>();
  }
}

function buildFeedHeadline(entry: {
  pm25: number;
  importedShare: number;
  windDirection: string;
}) {
  if (entry.importedShare >= 60) {
    return `Imported pollution dominated this six-hour window with ${entry.windDirection} transport into Kathmandu.`;
  }

  if (entry.pm25 >= 120) {
    return `Heavy PM2.5 persisted in this six-hour window as local accumulation combined with regional inflow.`;
  }

  if (entry.pm25 >= 80) {
    return `Elevated PM2.5 continued with mixed local emissions and transported haze.`;
  }

  return `Moderate particulate load with weaker transport forcing in this six-hour window.`;
}

function build24HourTimeline(
  hourlyMeasurements: OpenAQHourlyMeasurement[],
  weather: OpenMeteoResponse,
  airQuality: OpenMeteoAirQualityResponse | null,
  stationQuality: number,
  agreementScore: number,
  stationCount: number,
  fireSignal: FireSignal | null
): TimelineFrame[] {
  const sortedMeasurements = [...hourlyMeasurements]
    .filter(
      (item) =>
        typeof item.value === "number" &&
        item.value >= 0 &&
        getHourlyMeasurementTimestamp(item)
    )
    .sort(
      (a, b) =>
        new Date(getHourlyMeasurementTimestamp(a)!).getTime() -
        new Date(getHourlyMeasurementTimestamp(b)!).getTime()
    );

  if (sortedMeasurements.length === 0) {
    return [];
  }

  const endTime = new Date(
    getHourlyMeasurementTimestamp(sortedMeasurements[sortedMeasurements.length - 1])!
  ).getTime();
  const startTime = endTime - 24 * 60 * 60 * 1000;
  const hourlyTimes = weather.hourly?.time ?? [];
  const hourlySpeeds = weather.hourly?.wind_speed_10m ?? [];
  const hourlyDirections = weather.hourly?.wind_direction_10m ?? [];

  return sortedMeasurements
    .filter((measurement) => {
      const timestamp = new Date(getHourlyMeasurementTimestamp(measurement)!).getTime();
      return timestamp >= startTime;
    })
    .slice(-24)
    .map((measurement) => {
      const timestamp = getHourlyMeasurementTimestamp(measurement)!;
      const weatherIndex = findHourlyIndex(hourlyTimes, timestamp);
      const windSpeedKph = Math.round(
        weatherIndex >= 0
          ? hourlySpeeds[weatherIndex] ?? weather.current?.wind_speed_10m ?? 0
          : weather.current?.wind_speed_10m ?? 0
      );
      const windDirectionDegrees =
        weatherIndex >= 0
          ? hourlyDirections[weatherIndex] ?? weather.current?.wind_direction_10m ?? 0
          : weather.current?.wind_direction_10m ?? 0;
      const windDirection = directionFromDegrees(windDirectionDegrees);
      const pm25 = Math.round(measurement.value);
      const modelEvidence = buildModelEvidence(pm25, airQuality, timestamp);
      const attribution = computeAttribution({
        pm25,
        windDirection,
        windDirectionDegrees,
        windSpeedKph,
        measurementTimestamp: timestamp,
        hourlyMeasurements: sortedMeasurements.filter((item) => {
          const utc = getHourlyMeasurementTimestamp(item);
          return typeof utc === "string" && new Date(utc).getTime() <= new Date(timestamp).getTime();
        }),
        weather,
        stationQuality,
        agreementScore,
        stationCount,
        fireSignal,
        modelEvidence
      });

      return {
        timestamp,
        pm25,
        aqi: getAqiFromPm25(measurement.value),
        aqiCategory: getAqiCategory(getAqiFromPm25(measurement.value)),
        importedShare: attribution.importedShare,
        localShare: attribution.localShare,
        confidence: attribution.confidence,
        dominantSource: attribution.sources[0]?.name ?? "Unknown",
        windDirection,
        windSpeedKph,
        transportPath: buildWindTrajectoryPath(weather, timestamp),
        regime: attribution.regime
      };
    });
}

function buildSixHourFeed(
  hourlyMeasurements: OpenAQHourlyMeasurement[],
  weather: OpenMeteoResponse
): FeedEntry[] {
  const sortedMeasurements = [...hourlyMeasurements]
    .filter(
      (item) =>
        typeof item.value === "number" && getHourlyMeasurementTimestamp(item)
    )
    .sort((a, b) =>
      new Date(getHourlyMeasurementTimestamp(a)!).getTime() -
      new Date(getHourlyMeasurementTimestamp(b)!).getTime()
    );

  if (sortedMeasurements.length === 0) {
    return [];
  }

  const hourlyTimes = weather.hourly?.time ?? [];
  const hourlySpeeds = weather.hourly?.wind_speed_10m ?? [];
  const hourlyDirections = weather.hourly?.wind_direction_10m ?? [];

  const sixHourBuckets = new Map<
    string,
    {
      values: number[];
      weatherIndexes: number[];
      latestTimestamp: string;
    }
  >();

  for (const measurement of sortedMeasurements) {
    const utc = getHourlyMeasurementTimestamp(measurement);
    if (!utc) continue;
    const date = new Date(utc);
    const bucketStart = new Date(date);
    bucketStart.setUTCHours(Math.floor(date.getUTCHours() / 6) * 6, 0, 0, 0);
    const key = bucketStart.toISOString();
    const weatherIndex = findHourlyIndex(hourlyTimes, utc);
    const bucket = sixHourBuckets.get(key) ?? {
      values: [],
      weatherIndexes: [],
      latestTimestamp: utc
    };

    bucket.values.push(measurement.value);
    if (weatherIndex >= 0) {
      bucket.weatherIndexes.push(weatherIndex);
    }
    if (utc > bucket.latestTimestamp) {
      bucket.latestTimestamp = utc;
    }
    sixHourBuckets.set(key, bucket);
  }

  return [...sixHourBuckets.entries()]
    .sort((a, b) => new Date(b[0]).getTime() - new Date(a[0]).getTime())
    .slice(0, 8)
    .map(([timestamp, bucket]) => {
      const bucketTime = new Date(bucket.latestTimestamp).getTime();
      const pm25 = Math.round(
        bucket.values.reduce((sum, value) => sum + value, 0) / bucket.values.length
      );
      const speedAverage =
        bucket.weatherIndexes.length > 0
          ? bucket.weatherIndexes.reduce(
              (sum, index) => sum + (hourlySpeeds[index] ?? 0),
              0
            ) / bucket.weatherIndexes.length
          : weather.current?.wind_speed_10m ?? 0;
      const directionAverage =
        bucket.weatherIndexes.length > 0
          ? bucket.weatherIndexes.reduce(
              (sum, index) => sum + (hourlyDirections[index] ?? 0),
              0
          ) / bucket.weatherIndexes.length
          : weather.current?.wind_direction_10m ?? 0;
      const attribution = computeAttribution({
        pm25,
        windDirection: directionFromDegrees(directionAverage),
        windDirectionDegrees: directionAverage,
        windSpeedKph: Math.round(speedAverage),
        measurementTimestamp: bucket.latestTimestamp,
        hourlyMeasurements: sortedMeasurements.filter((measurement) => {
          const utc = getHourlyMeasurementTimestamp(measurement);
          return typeof utc === "string" && new Date(utc).getTime() <= bucketTime;
        }),
        weather
      });
      const importedShare = attribution.importedShare;
      const localShare = attribution.localShare;
      const windDirection = directionFromDegrees(directionAverage);
      const windSpeedKph = Math.round(speedAverage);

      return {
        timestamp,
        pm25,
        importedShare,
        localShare,
        windDirection,
        windSpeedKph,
        headline: buildFeedHeadline({
          pm25,
          importedShare,
          windDirection
        })
      };
    });
}

function buildMapEvidence(input: {
  stationEvidence: StationEvidence[];
  fireEvidence: FireEvidence | null;
  registryEvidence: RegistryEvidence;
  trajectoryEvidence: TrajectoryEvidence;
}): MapEvidence {
  const stations = input.stationEvidence
    .filter(
      (station) =>
        typeof station.latitude === "number" && typeof station.longitude === "number"
    )
    .map((station) => ({
      lat: station.latitude!,
      lng: station.longitude!,
      label: `${station.label} · PM2.5 ${station.pm25} ug/m3 · weight ${station.weight}%`,
      kind: "station" as const,
      weight: station.weight
    }));

  const hotspots =
    input.fireEvidence?.hotspots.map((hotspot) => ({
      lat: hotspot.lat,
      lng: hotspot.lng,
      label:
        hotspot.nearCropland === true
          ? `FIRMS hotspot · FRP ${Math.round(hotspot.frp)} · near cropland`
          : `FIRMS hotspot · FRP ${Math.round(hotspot.frp)}`,
      kind:
        hotspot.nearCropland === true
          ? ("cropland-hotspot" as const)
          : ("hotspot" as const),
      weight: clamp(hotspot.frp / 40, 0.25, 1)
    })) ?? [];

  const registry: MapEvidencePoint[] = [
    ...input.registryEvidence.localKilns.map((point) => ({
      lat: point.lat,
      lng: point.lng,
      label: point.label,
      kind: "kiln" as const
    })),
    ...input.registryEvidence.upwindKilns.map((point) => ({
      lat: point.lat,
      lng: point.lng,
      label: point.label,
      kind: "kiln" as const
    })),
    ...input.registryEvidence.localIndustrialSites.map((point) => ({
      lat: point.lat,
      lng: point.lng,
      label: point.label,
      kind: "industrial" as const
    })),
    ...input.registryEvidence.upwindIndustrialSites.map((point) => ({
      lat: point.lat,
      lng: point.lng,
      label: point.label,
      kind: "industrial" as const
    }))
  ];

  const lines =
    input.trajectoryEvidence.pathPoints.length >= 2
      ? [
          {
            label:
              input.trajectoryEvidence.provider === "hysplit"
                ? "NOAA HYSPLIT back trajectory"
                : "Wind-derived back trajectory",
            kind: "trajectory" as const,
            points: input.trajectoryEvidence.pathPoints
          }
        ]
      : [];

  return {
    stations,
    hotspots,
    registry,
    lines
  };
}

export async function getHistoricalTrend(
  days = 30,
  stationSummary?: StationSummary
): Promise<HistoricalTrendEntry[]> {
  noStore();

  const cacheKey = `trend:v2:${days}d`;
  const cached = getSignalCacheEntry(cacheKey);
  if (cached && isSignalCacheFresh(cached.checkedAt, HISTORICAL_TREND_CACHE_TTL_HOURS)) {
    try {
      return JSON.parse(cached.payloadJson) as HistoricalTrendEntry[];
    } catch {
      // Ignore malformed cache payloads and refetch.
    }
  }

  try {
    const latest = stationSummary ? null : await fetchBestLatestPm25();
    const sensorId = stationSummary?.primarySensorId ?? latest?.measurement.sensorsId;
    const measurementTimestamp = latest?.measurement.datetime?.utc ?? new Date().toISOString();

    if (typeof sensorId !== "number") {
      return getSevenDayTrend(days) as HistoricalTrendEntry[];
    }

    const end = new Date(measurementTimestamp);
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
    const startDate = toIsoDateString(start);
    const endDate = toIsoDateString(end);
    const [hourlyMeasurements, historicalWeather, historicalAirQuality, dailyFireSignals] =
      await Promise.all([
        fetchHourlyPm25Range(sensorId, start.toISOString(), end.toISOString()),
        fetchHistoricalWindData(startDate, endDate),
        fetchHistoricalAirQualityData(startDate, endDate),
        fetchHistoricalFireSignals(days)
      ]);

    const validMeasurements = [...hourlyMeasurements]
      .filter(
        (item) =>
          typeof item.value === "number" &&
          item.value >= 0 &&
          typeof getHourlyMeasurementTimestamp(item) === "string"
      )
      .sort(
        (a, b) =>
          new Date(getHourlyMeasurementTimestamp(a)!).getTime() -
          new Date(getHourlyMeasurementTimestamp(b)!).getTime()
      );

    if (validMeasurements.length === 0) {
      return getSevenDayTrend(days) as HistoricalTrendEntry[];
    }

    const dailyBuckets = new Map<string, OpenAQHourlyMeasurement[]>();
    for (const measurement of validMeasurements) {
      const timestamp = getHourlyMeasurementTimestamp(measurement);
      if (!timestamp) continue;
      const localDay = getKathmanduLocalDay(timestamp);
      const bucket = dailyBuckets.get(localDay) ?? [];
      bucket.push(measurement);
      dailyBuckets.set(localDay, bucket);
    }

    const registryPromises = new Map<string, Promise<RegistryEvidenceInternal>>();
    const entries = await Promise.all(
      [...dailyBuckets.entries()]
        .sort((a, b) => b[0].localeCompare(a[0]))
        .slice(0, days)
        .map(async ([localDay, measurements]) => {
          const measurement = measurements[measurements.length - 1];
          const timestamp = getHourlyMeasurementTimestamp(measurement)!;
          const bucketTime = new Date(timestamp).getTime();
          const pm25 = Math.round(measurement.value);
          const weatherIndex = findHourlyIndex(historicalWeather.hourly?.time ?? [], timestamp);
          const windSpeedKph = Math.round(
            weatherIndex >= 0
              ? historicalWeather.hourly?.wind_speed_10m?.[weatherIndex] ?? 0
              : 0
          );
          const windDirectionDegrees =
            weatherIndex >= 0
              ? historicalWeather.hourly?.wind_direction_10m?.[weatherIndex] ?? 0
              : 0;
          const windDirection = directionFromDegrees(windDirectionDegrees);
          const sectorKey = `${Math.round((((windDirectionDegrees % 360) + 360) % 360) / 22.5)}`;

          let registryPromise = registryPromises.get(sectorKey);
          if (!registryPromise) {
            registryPromise = fetchSourceRegistryEvidence(windDirectionDegrees);
            registryPromises.set(sectorKey, registryPromise);
          }

          const registryEvidence = await registryPromise.catch(() => ({
            provider: "osm" as const,
            localKilnCount: 0,
            localIndustrialCount: 0,
            upwindIndustrialCount: 0,
            upwindKilnCount: 0,
            localKilns: [],
            localIndustrialSites: [],
            upwindIndustrialSites: [],
            upwindKilns: []
          }));
          const trajectoryEvidence = {
            provider: "wind-model" as const,
            available: false,
            originBearing: null,
            originDistanceKm: null,
            pathPoints: buildWindTrajectoryPath(historicalWeather, timestamp),
            ...buildTrajectorySignals(historicalWeather, timestamp)
          };
          const attribution = computeAttribution({
            pm25,
            windDirection,
            windDirectionDegrees,
            windSpeedKph,
            measurementTimestamp: timestamp,
            hourlyMeasurements: validMeasurements.filter((item) => {
              const utc = getHourlyMeasurementTimestamp(item);
              return typeof utc === "string" && new Date(utc).getTime() <= bucketTime;
            }),
            weather: historicalWeather,
            stationQuality: stationSummary?.stationQuality ?? latest?.stationQuality ?? 0.75,
            agreementScore: stationSummary?.agreementScore ?? latest?.agreementScore ?? 0.7,
            stationCount: stationSummary?.stationCount ?? latest?.stationCount ?? 1,
            fireSignal: dailyFireSignals.get(toUtcDateString(timestamp)) ?? null,
            modelEvidence: buildModelEvidence(pm25, historicalAirQuality, timestamp),
            trajectoryEvidence,
            registryEvidence
          });
          const fireSignal = dailyFireSignals.get(toUtcDateString(timestamp));

          return {
            localDay,
            sourceUpdatedAt: timestamp,
            pm25,
            importedShare: attribution.importedShare,
            localShare: attribution.localShare,
            hotspotCount: fireSignal?.hotspotCount ?? 0,
            croplandMatchCount: fireSignal?.croplandMatchCount ?? 0,
            dominantSource: attribution.sources[0]?.name ?? "Unavailable"
          };
        })
    );

    const trend = entries.sort((a, b) => b.sourceUpdatedAt.localeCompare(a.sourceUpdatedAt));

    setSignalCacheEntry({
      cacheKey,
      signalKind: "trend",
      payloadJson: JSON.stringify(trend),
      checkedAt: new Date().toISOString()
    });

    return trend;
  } catch {
    return getSevenDayTrend(days) as HistoricalTrendEntry[];
  }
}

export async function getSnapshot(): Promise<CitySnapshot> {
  noStore();

  const generatedAt = new Date().toISOString();
  const [
    { measurement, stationQuality, agreementScore, spreadUgM3, outlierCount, stationCount, stations },
    weather,
    airQuality,
    fireSignal
  ] = await Promise.all([
    fetchBestLatestPm25(),
    fetchWindData(),
    fetchAirQualityData(),
    fetchFireSignal()
  ]);
  const pm25 = Math.round(measurement.value);
  const aqi = getAqiFromPm25(measurement.value);
  const aqiCategory = getAqiCategory(aqi);
  const category = getCategory(pm25);
  const windSpeedKph = Math.round(weather.current?.wind_speed_10m ?? 0);
  const windDirectionDegrees = weather.current?.wind_direction_10m ?? 0;
  const windDirection = directionFromDegrees(windDirectionDegrees);
  const windTrail = buildWindTrail(weather);
  const hourlyFeed =
    typeof measurement.sensorsId === "number"
      ? await fetchHourlyPm25(
          measurement.sensorsId,
          measurement.datetime?.utc ?? new Date().toISOString()
        )
      : [];
  const measurementTimestamp =
    measurement.datetime?.utc ?? weather.current?.time ?? new Date().toISOString();
  const trajectoryEvidence =
    (await fetchHysplitTrajectory(measurementTimestamp)) ?? {
      provider: "wind-model" as const,
      available: false,
      originBearing: null,
      originDistanceKm: null,
      pathPoints: buildWindTrajectoryPath(weather, measurementTimestamp),
      ...buildTrajectorySignals(weather, measurementTimestamp)
    };
  const registryEvidence = await fetchSourceRegistryEvidence(
    trajectoryEvidence.originBearing ?? windDirectionDegrees
  );
  const modelEvidence = buildModelEvidence(
    pm25,
    airQuality,
    measurementTimestamp
  );
  const attribution = computeAttribution({
    pm25,
    windDirection,
    windDirectionDegrees,
    windSpeedKph,
    measurementTimestamp,
    hourlyMeasurements: hourlyFeed,
    weather,
    stationQuality,
    agreementScore,
    stationCount,
    fireSignal,
    modelEvidence,
    trajectoryEvidence,
    registryEvidence
  });
  const smoothedAttribution = smoothAttribution(
    attribution,
    getRecentSnapshots(8),
    windDirection
  );
  const feed = buildSixHourFeed(hourlyFeed, weather);
  const timeline24h = build24HourTimeline(
    hourlyFeed,
    weather,
    airQuality,
    stationQuality,
    agreementScore,
    stationCount,
    fireSignal
  );

  const snapshot: CitySnapshot = {
    city: kathmandu.city,
    country: kathmandu.country,
    updatedAt:
      measurement.datetime?.utc ?? weather.current?.time ?? new Date().toISOString(),
    generatedAt,
    pm25,
    aqi,
    aqiCategory,
    category,
    importedShare: smoothedAttribution.importedShare,
    localShare: smoothedAttribution.localShare,
    confidence: smoothedAttribution.confidence,
    summary: smoothedAttribution.summary,
    coordinates: kathmandu.coordinates,
    sources: smoothedAttribution.sources.map(({ imported: _imported, ...source }) => source),
    windTrail,
    feed,
    timeline24h,
    trajectoryEvidence,
    registryEvidence,
    stationEvidence: stations,
    stationSummary: {
      primarySensorId: measurement.sensorsId ?? null,
      stationQuality,
      agreementScore,
      stationCount,
      spreadUgM3,
      outlierCount,
      consensusMethod: "robust-weighted-mean"
    },
    modelEvidence,
    confidenceBreakdown: smoothedAttribution.confidenceBreakdown,
    fireEvidence: fireSignal ?? {
      hotspotCount: 0,
      hotspotDensity: 0,
      meanFrp: 0,
      cropBeltHotspotCount: 0,
      croplandMatchCount: 0,
      inferredSourceName: "Upwind fire activity",
      inferredEvidenceLabel: "No FIRMS hotspot evidence",
      hotspots: []
    },
    mapEvidence: buildMapEvidence({
      stationEvidence: stations,
      fireEvidence: fireSignal,
      registryEvidence,
      trajectoryEvidence
    }),
    regime: smoothedAttribution.regime,
    dataMode: "live",
    interpretationMode: "heuristic"
  };

  try {
    persistSnapshot(snapshot);
  } catch (error) {
    console.error("Failed to persist snapshot", error);
  }

  return snapshot;
}
