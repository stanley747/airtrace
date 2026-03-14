import { unstable_noStore as noStore } from "next/cache";

export type SourceContribution = {
  name: string;
  share: number;
  evidence: string;
};

export type WindPoint = {
  hour: string;
  direction: string;
  speedKph: number;
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

type AiAttributionResponse = {
  summary: string;
  importedShare: number;
  localShare: number;
  confidence: "Low" | "Medium" | "High";
  sources: SourceContribution[];
};

type FireSignal = {
  hotspotCount: number;
  hotspotDensity: number;
  meanFrp: number;
  cropBeltHotspotCount: number;
  inferredSourceName: string;
  inferredEvidenceLabel: string;
};

const PM25_PARAMETER_ID = 2;
const OPENAQ_BASE_URL = "https://api.openaq.org/v3";
const OPEN_METEO_BASE_URL =
  process.env.OPEN_METEO_BASE_URL ?? "https://api.open-meteo.com";
const MAX_MEASUREMENT_AGE_HOURS = 24;
const FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv";
const FIRMS_SOURCE = "VIIRS_SNPP_NRT";
const FIRMS_UPWIND_BBOX = "80,24,89,31";
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
        item.datetime?.utc
    )
    .map((item) => ({
      value: item.value,
      time: new Date(item.datetime!.utc!).getTime()
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

function normalizeShares(scores: Array<Omit<SourceContribution, "share"> & { score: number }>) {
  const total = scores.reduce((sum, item) => sum + item.score, 0);
  const normalized = scores.map((item) => ({
    name: item.name,
    evidence: item.evidence,
    share: total > 0 ? Math.round((item.score / total) * 100) : 25
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
  const trajectory = buildTrajectorySignals(input.weather, input.measurementTimestamp);
  const seasonalPriors = getSeasonalPriors(input.measurementTimestamp);
  const stationQuality = input.stationQuality ?? 0.75;
  const agreementScore = input.agreementScore ?? 0.7;
  const stationCount = input.stationCount ?? 1;
  const fireSignal = input.fireSignal ?? {
    hotspotCount: 0,
    hotspotDensity: 0,
    meanFrp: 0,
    cropBeltHotspotCount: 0,
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

  const sources = normalizeShares([
    {
      name: fireSignal.inferredSourceName,
      score:
        0.7 +
        (agriCorridor * 0.55 + trajectory.agriTransport * 0.45) * 1.3 +
        transportStrength * 0.55 +
        persistence * 0.35 +
        severity * 0.22 +
        fireSignal.hotspotDensity * 0.95 * fireSourceBoost +
        clamp(fireSignal.meanFrp / 40, 0, 1) * 0.22 * fireSourceBoost +
        seasonalPriors.agricultural,
      evidence:
        fireSignal.hotspotCount > 0
          ? `${fireSignal.inferredEvidenceLabel} with ${input.windDirection} transport`
          : `${input.windDirection} corridor with ${input.windSpeedKph} kph transport from upwind plains`
    },
    {
      name: "Industrial belt across the Indo-Gangetic Plain",
      score:
        0.78 +
        (industrialCorridor * 0.52 + trajectory.industrialTransport * 0.48) * 1.18 +
        transportStrength * 0.45 +
        persistence * 0.5 +
        severity * 0.28 +
        seasonalPriors.industrial,
      evidence: `${Math.round(pm25Stats.avg12h || input.pm25)} ug/m3 recent mean with sustained regional haze transport`
    },
    {
      name: "Kathmandu traffic and brick kilns",
      score:
        0.92 +
        stagnation * 1.1 +
        (localShelterFlow * 0.45 + trajectory.localRetention * 0.35) +
        severity * 0.4 +
        localSpike * 0.55 +
        seasonalPriors.local,
      evidence:
        input.windSpeedKph < 10
          ? "Lighter valley winds favor local build-up from traffic and kilns"
          : "Mixed local emissions remain material inside the valley basin"
    },
    {
      name: "Dust resuspension",
      score:
        0.4 +
        (dustPotential * 0.65 + trajectory.dustTransport * 0.35) * 1.05 +
        severity * 0.16 +
        transportStrength * 0.18 +
        seasonalPriors.dust,
      evidence: `${input.windSpeedKph} kph surface winds support some particulate resuspension`
    }
  ]);

  const importedShare = sources
    .filter((source) =>
      source.name === "Probable agricultural burning, northern India" ||
      source.name === "Upwind fire activity" ||
      source.name === "Industrial belt across the Indo-Gangetic Plain"
    )
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
      clamp(stationCount / 4, 0, 1) * 0.04 +
      clamp(fireSignal.hotspotCount / 40, 0, 1) * 0.04 +
      clamp(pm25Stats.count24h / 18, 0, 1) * 0.06,
    0,
    1
  );
  const confidence = buildConfidenceLevel(confidenceScore);
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
    sources
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

  const measurement = data.results?.find(
    (result) => typeof result.value === "number"
  );

  if (!measurement) {
    throw new SnapshotError(
      `No latest PM2.5 measurement for location ${locationId}`,
      "openaq:latest-empty"
    );
  }

  return measurement;
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
      stationCount: weighted.length
    };
  }

  throw new SnapshotError(
    `No valid Kathmandu PM2.5 measurement found. ${failures.join(" | ")}`,
    "openaq:latest-invalid"
  );
}

async function fetchHourlyPm25(sensorId: number, datetimeTo: string) {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new SnapshotError("OPENAQ_API_KEY is not set", "env:openaq");
  }

  const end = new Date(datetimeTo);
  const start = new Date(end.getTime() - 48 * 60 * 60 * 1000);
  const query = new URLSearchParams({
    datetime_from: start.toISOString(),
    datetime_to: end.toISOString(),
    limit: "100"
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
  }

  const cropBeltDensity = clamp(cropBeltHotspotCount / 40, 0, 1);
  const cropBurningWindow = latestDate
    ? isLikelyCropBurningWindow(`${latestDate}T00:00:00Z`)
    : false;
  const probableAgriculturalBurning =
    cropBurningWindow && cropBeltHotspotCount >= 10 && cropBeltDensity >= 0.2;

  return {
    hotspotCount,
    hotspotDensity: clamp(hotspotCount / 80, 0, 1),
    meanFrp: hotspotCount > 0 ? frpSum / hotspotCount : 0,
    cropBeltHotspotCount,
    inferredSourceName: probableAgriculturalBurning
      ? "Probable agricultural burning, northern India"
      : "Upwind fire activity",
    inferredEvidenceLabel: probableAgriculturalBurning
      ? `${cropBeltHotspotCount} FIRMS hotspots in crop-belt geometry`
      : `${hotspotCount} FIRMS hotspots in the upwind corridor`
  };
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

function buildSixHourFeed(
  hourlyMeasurements: OpenAQHourlyMeasurement[],
  weather: OpenMeteoResponse
): FeedEntry[] {
  const sortedMeasurements = [...hourlyMeasurements]
    .filter((item) => item.datetime?.utc && typeof item.value === "number")
    .sort((a, b) =>
      new Date(a.datetime!.utc!).getTime() - new Date(b.datetime!.utc!).getTime()
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
    const utc = measurement.datetime?.utc;
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
          const utc = measurement.datetime?.utc;
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

export async function getSnapshot(): Promise<CitySnapshot> {
  noStore();

  const generatedAt = new Date().toISOString();
  const [{ measurement, stationQuality, agreementScore, stationCount }, weather, fireSignal] = await Promise.all([
    fetchBestLatestPm25(),
    fetchWindData(),
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
  const attribution = computeAttribution({
    pm25,
    windDirection,
    windDirectionDegrees,
    windSpeedKph,
    measurementTimestamp:
      measurement.datetime?.utc ?? weather.current?.time ?? new Date().toISOString(),
    hourlyMeasurements: hourlyFeed,
    weather,
    stationQuality,
    agreementScore,
    stationCount,
    fireSignal
  });
  const feed = buildSixHourFeed(hourlyFeed, weather);

  return {
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
    dataMode: "live",
    interpretationMode: "heuristic"
  };
}
