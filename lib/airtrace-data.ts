import { unstable_noStore as noStore } from "next/cache";
import JSZip from "jszip";

import {
  getCroplandCacheEntry,
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
  overlay?: {
    center: [number, number];
    radius: number;
  };
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
};

export type TrajectoryEvidence = {
  provider: "hysplit" | "wind-model";
  available: boolean;
  originBearing: number | null;
  originDistanceKm: number | null;
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
  label: string;
  pm25: number;
  updatedAt: string;
  distanceKm: number;
  stationQuality: number;
  weight: number;
  freshnessHours: number;
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
};

export type RegistryEvidence = {
  provider: "osm";
  localKilnCount: number;
  localIndustrialCount: number;
  upwindIndustrialCount: number;
  upwindKilnCount: number;
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
  modelEvidence: ModelEvidence;
  confidenceBreakdown: ConfidenceBreakdown;
  fireEvidence: FireEvidence;
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
  sources: SourceContribution[];
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
  overlay: {
    center: [number, number];
    radius: number;
  };
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
const FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv";
const FIRMS_SOURCE = "VIIRS_SNPP_NRT";
const OVERPASS_API_URL = "https://overpass-api.de/api/interpreter";
const FIRMS_UPWIND_BBOX = "80,24,89,31";
const CROPLAND_PROXIMITY_METERS = 750;
const MAX_CROPLAND_HOTSPOT_SAMPLES = 12;
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

function projectFromKathmandu(bearingDegrees: number, distanceKm: number): [number, number] {
  const earthRadiusKm = 6371;
  const bearing = toRadians(bearingDegrees);
  const lat1 = toRadians(kathmandu.coordinates.lat);
  const lng1 = toRadians(kathmandu.coordinates.lng);
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
    overlay: item.overlay,
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

function buildConfidenceLevel(score: number): "Low" | "Medium" | "High" {
  if (score >= 0.72) return "High";
  if (score >= 0.48) return "Medium";
  return "Low";
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
    upwindKilnCount: 0
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
    inferredEvidenceLabel: "No FIRMS hotspot evidence"
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
  const contributors: ContributorCandidate[] = [];

  contributors.push({
    name:
      localSpike >= 0.28
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
    score: localScore,
    imported: false,
    overlay: {
      center: [kathmandu.coordinates.lat, kathmandu.coordinates.lng],
      radius: 18000
    }
  });

  if (fireSignal.hotspotCount > 0 && fireScore >= 0.35) {
    contributors.push({
      name:
        fireSignal.inferredSourceName === "Probable agricultural burning, northern India"
          ? `${input.windDirection} crop-belt fire corridor`
          : `${input.windDirection} upwind hotspot corridor`,
      evidence: `${fireSignal.inferredEvidenceLabel} with ${input.windDirection} transport`,
      score: fireScore,
      imported: true,
      overlay: {
        center: projectFromKathmandu(input.windDirectionDegrees, 170),
        radius: 42000
      }
    });
  }

  if (transportScore >= 0.4) {
    contributors.push({
      name: `${input.windDirection} regional haze transport`,
      evidence:
        industrialRegistryBoost >= 0.25
          ? `${registryEvidence.upwindIndustrialCount} mapped industrial sites align with the upwind corridor`
          : aerosolSupport >= 0.4
            ? `${Math.round(pm25Stats.avg12h || input.pm25)} ug/m3 recent mean with elevated modeled aerosol loading`
            : `${Math.round(pm25Stats.avg12h || input.pm25)} ug/m3 recent mean with sustained upwind transport`,
      score: transportScore,
      imported: true,
      overlay: {
        center: projectFromKathmandu(input.windDirectionDegrees, 110),
        radius: 34000
      }
    });
  }

  if (dustScore >= 0.28 || input.windSpeedKph >= 11) {
    contributors.push({
      name:
        input.windSpeedKph >= 11
          ? `${input.windDirection} wind-driven dust`
          : "Surface dust resuspension",
      evidence:
        modelEvidence.modeledDust && modelEvidence.modeledDust >= 10
          ? `Modeled dust ${modelEvidence.modeledDust} ug/m3 aligns with surface resuspension risk`
          : `${input.windSpeedKph} kph surface winds support particulate lift and resuspension`,
      score: dustScore,
      imported: input.windSpeedKph >= 11,
      overlay: {
        center: projectFromKathmandu(input.windDirectionDegrees, input.windSpeedKph >= 11 ? 70 : 35),
        radius: 24000
      }
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
      clamp(pm25Stats.count24h / 18, 0, 1) * 0.06,
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
  const summary =
    importedShare >= 55
      ? `${dominantSource} likely drove most of the current PM2.5 load, with ${input.windDirection} transport sustaining imported haze into Kathmandu.`
      : `${dominantSource} appears to be the leading contributor, but current conditions still point to a mixed balance between imported haze and local valley emissions.`;

  return {
    summary,
    importedShare,
    localShare,
    confidence,
    confidenceBreakdown,
    sources: sources.map(({ imported: _imported, ...source }) => source)
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

      if (candidates.length >= 5) {
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
        weight
      };
    });
    const totalWeight = weighted.reduce((sum, candidate) => sum + candidate.weight, 0);
    const consensusValue =
      totalWeight > 0
        ? weighted.reduce(
            (sum, candidate) => sum + candidate.measurement.value * candidate.weight,
            0
          ) / totalWeight
        : weighted[0].measurement.value;
    const disagreement =
      totalWeight > 0
        ? Math.sqrt(
            weighted.reduce((sum, candidate) => {
              const diff = candidate.measurement.value - consensusValue;
              return sum + diff * diff * candidate.weight;
            }, 0) / totalWeight
          )
        : 0;
    const primary = weighted[0];

    return {
      location: primary.location,
      measurement: {
        ...primary.measurement,
        value: consensusValue
      },
      stationQuality:
        weighted.reduce(
          (sum, candidate) => sum + candidate.stationQuality * candidate.weight,
          0
        ) / totalWeight,
      distanceKm:
        weighted.reduce((sum, candidate) => sum + candidate.distanceKm * candidate.weight, 0) /
        totalWeight,
      agreementScore: clamp(1 - disagreement / 18, 0, 1),
      stationCount: weighted.length,
      stations: weighted.map((candidate) => ({
        locationId: candidate.location.id,
        label:
          candidate.location.locality ??
          candidate.location.name ??
          `Location ${candidate.location.id}`,
        pm25: Math.round(candidate.measurement.value),
        updatedAt: candidate.measurement.datetime?.utc ?? new Date().toISOString(),
        distanceKm: Math.round(candidate.distanceKm),
        stationQuality: Math.round(candidate.stationQuality * 100),
        weight: Math.round(candidate.weight * 100),
        freshnessHours: Math.round(candidate.freshnessHours * 10) / 10
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

async function countCroplandMatchedHotspots(hotspots: FireHotspot[]) {
  if (hotspots.length === 0) {
    return 0;
  }

  const sampledHotspots = [...hotspots]
    .sort((a, b) => b.frp - a.frp)
    .slice(0, MAX_CROPLAND_HOTSPOT_SAMPLES);
  let matches = 0;

  for (const hotspot of sampledHotspots) {
    const hotspotKey = getHotspotCacheKey(hotspot);
    const cached = getCroplandCacheEntry(hotspotKey);

    if (cached && isCroplandCacheFresh(cached.checkedAt)) {
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

      if (nearCropland) {
        matches += 1;
      }
    } catch {
      return 0;
    }
  }

  return matches;
}

async function fetchRegistryCount(
  kind: "industrial" | "kiln",
  center: [number, number],
  radiusMeters: number
) {
  const cacheKey = getRegistryCacheKey(kind, center, radiusMeters);
  const cached = getSignalCacheEntry(cacheKey);

  if (cached && isSignalCacheFresh(cached.checkedAt)) {
    try {
      const payload = JSON.parse(cached.payloadJson) as { count?: number };
      if (typeof payload.count === "number") {
        return payload.count;
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
out ids;
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
out ids;
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
  const count = Array.isArray(payload.elements) ? payload.elements.length : 0;

  setSignalCacheEntry({
    cacheKey,
    signalKind: `registry:${kind}`,
    payloadJson: JSON.stringify({ count }),
    checkedAt: new Date().toISOString()
  });

  return count;
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
      fetchRegistryCount("kiln", localCenter, LOCAL_REGISTRY_RADIUS_METERS),
      fetchRegistryCount("industrial", localCenter, LOCAL_REGISTRY_RADIUS_METERS),
      fetchRegistryCount("industrial", upwindCenter, UPWIND_REGISTRY_RADIUS_METERS),
      fetchRegistryCount("kiln", upwindCenter, UPWIND_REGISTRY_RADIUS_METERS)
    ]);

    return {
      provider: "osm",
      localKilnCount,
      localIndustrialCount,
      upwindIndustrialCount,
      upwindKilnCount
    };
  } catch {
    return {
      provider: "osm",
      localKilnCount: 0,
      localIndustrialCount: 0,
      upwindIndustrialCount: 0,
      upwindKilnCount: 0
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
      inferredEvidenceLabel: "No FIRMS hotspot evidence"
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

  const croplandMatchCount = await countCroplandMatchedHotspots(hotspots);
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
        : `${hotspotCount} FIRMS hotspots in the upwind corridor`
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
      const croplandMatchCount = await countCroplandMatchedHotspots(hotspots);
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
            : `${hotspotCount} FIRMS hotspots in the upwind corridor`
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
      const weatherIndex = hourlyTimes.findIndex((time) => time === timestamp);
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
        windSpeedKph
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
    const weatherIndex = hourlyTimes.findIndex((time) => time === utc);
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

export async function getHistoricalTrend(days = 30): Promise<HistoricalTrendEntry[]> {
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
    const latest = await fetchBestLatestPm25();
    const sensorId = latest.measurement.sensorsId;
    const measurementTimestamp =
      latest.measurement.datetime?.utc ?? new Date().toISOString();

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
            upwindKilnCount: 0
          }));
          const trajectoryEvidence = {
            provider: "wind-model" as const,
            available: false,
            originBearing: null,
            originDistanceKm: null,
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
            stationQuality: latest.stationQuality,
            agreementScore: latest.agreementScore,
            stationCount: latest.stationCount,
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
  const [{ measurement, stationQuality, agreementScore, stationCount, stations }, weather, airQuality, fireSignal] = await Promise.all([
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
    importedShare: attribution.importedShare,
    localShare: attribution.localShare,
    confidence: attribution.confidence,
    summary: attribution.summary,
    coordinates: kathmandu.coordinates,
    sources: attribution.sources,
    windTrail,
    feed,
    timeline24h,
    trajectoryEvidence,
    registryEvidence,
    stationEvidence: stations,
    modelEvidence,
    confidenceBreakdown: attribution.confidenceBreakdown,
    fireEvidence: fireSignal ?? {
      hotspotCount: 0,
      hotspotDensity: 0,
      meanFrp: 0,
      cropBeltHotspotCount: 0,
      croplandMatchCount: 0,
      inferredSourceName: "Upwind fire activity",
      inferredEvidenceLabel: "No FIRMS hotspot evidence"
    },
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
