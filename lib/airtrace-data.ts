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
  pm25: number;
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
  dataMode: "live" | "fallback";
  interpretationMode: "ai" | "heuristic";
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

type OpenAQLocation = {
  id: number;
  locality?: string | null;
  name?: string | null;
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

const PM25_PARAMETER_ID = 2;
const OPENAQ_BASE_URL = "https://api.openaq.org/v3";
const OPEN_METEO_BASE_URL =
  process.env.OPEN_METEO_BASE_URL ?? "https://api.open-meteo.com";
const OPENAI_MODEL = process.env.OPENAI_MODEL ?? "gpt-5-nano";

const kathmandu = {
  city: "Kathmandu",
  country: "Nepal",
  coordinates: {
    lat: 27.7172,
    lng: 85.324
  },
  localityHints: ["Kathmandu", "Lalitpur", "Bhaktapur"]
};

const fallbackSnapshot: CitySnapshot = {
  city: "Kathmandu",
  country: "Nepal",
  updatedAt: "2026-03-12T14:30:00Z",
  pm25: 142,
  category: "Hazardous",
  importedShare: 68,
  localShare: 32,
  confidence: "Medium",
  summary:
    "Transport conditions suggest most of today's PM2.5 arrived from upwind agricultural and industrial corridors south-west of the valley, with local traffic and brick kilns amplifying the peak.",
  coordinates: kathmandu.coordinates,
  sources: [
    {
      name: "Agricultural burning, northern India",
      share: 36,
      evidence: "Upwind smoke indicators and south-westerly transport"
    },
    {
      name: "Industrial belt across the Indo-Gangetic Plain",
      share: 32,
      evidence: "Persistent regional haze aligned with multi-hour wind path"
    },
    {
      name: "Kathmandu traffic and brick kilns",
      share: 20,
      evidence: "Local morning build-up inside valley inversion"
    },
    {
      name: "Dust resuspension",
      share: 12,
      evidence: "Dry conditions and elevated surface winds"
    }
  ],
  windTrail: [
    { hour: "Now", direction: "SW", speedKph: 14 },
    { hour: "-6h", direction: "SW", speedKph: 18 },
    { hour: "-12h", direction: "WSW", speedKph: 22 },
    { hour: "-24h", direction: "W", speedKph: 19 },
    { hour: "-48h", direction: "WNW", speedKph: 15 }
  ],
  feed: [
    {
      timestamp: "2026-03-12T12:00:00Z",
      pm25: 142,
      importedShare: 68,
      localShare: 32,
      windDirection: "SW",
      windSpeedKph: 14,
      headline: "Transport-heavy six-hour window with strong upwind contribution into the valley."
    },
    {
      timestamp: "2026-03-12T06:00:00Z",
      pm25: 129,
      importedShare: 64,
      localShare: 36,
      windDirection: "SW",
      windSpeedKph: 18,
      headline: "Regional inflow remained elevated while local morning accumulation intensified exposure."
    },
    {
      timestamp: "2026-03-12T00:00:00Z",
      pm25: 121,
      importedShare: 62,
      localShare: 38,
      windDirection: "WSW",
      windSpeedKph: 22,
      headline: "Western and south-western transport corridors dominated the overnight pollution load."
    }
  ],
  dataMode: "fallback",
  interpretationMode: "heuristic"
};

function getCategory(pm25: number) {
  if (pm25 <= 12) return "Good";
  if (pm25 <= 35.4) return "Moderate";
  if (pm25 <= 55.4) return "Unhealthy for Sensitive Groups";
  if (pm25 <= 150.4) return "Very Unhealthy";
  if (pm25 <= 250.4) return "Hazardous";
  return "Severe";
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

async function fetchJson<T>(url: string, init?: RequestInit) {
  const response = await fetch(url, {
    ...init,
    cache: "no-store"
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

async function fetchNearestPm25Location() {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new Error("OPENAQ_API_KEY is not set");
  }

  const query = new URLSearchParams({
    coordinates: `${kathmandu.coordinates.lat},${kathmandu.coordinates.lng}`,
    radius: "25000",
    limit: "100",
    parameters_id: `${PM25_PARAMETER_ID}`
  });

  const data = await fetchJson<OpenAQListResponse<OpenAQLocation>>(
    `${OPENAQ_BASE_URL}/locations?${query.toString()}`,
    {
      headers: {
        "X-API-Key": apiKey
      }
    }
  );

  const matches =
    data.results?.filter((location) => {
      const locality = `${location.locality ?? ""} ${location.name ?? ""}`.toLowerCase();
      return kathmandu.localityHints.some((hint) =>
        locality.includes(hint.toLowerCase())
      );
    }) ?? [];

  return matches[0] ?? data.results?.[0];
}

async function fetchLatestPm25(locationId: number) {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new Error("OPENAQ_API_KEY is not set");
  }

  const data = await fetchJson<OpenAQListResponse<OpenAQLatestMeasurement>>(
    `${OPENAQ_BASE_URL}/locations/${locationId}/latest?limit=100`,
    {
      headers: {
        "X-API-Key": apiKey
      }
    }
  );

  const measurement = data.results?.find(
    (result) => typeof result.value === "number"
  );

  if (!measurement) {
    throw new Error(`No latest PM2.5 measurement for location ${locationId}`);
  }

  return measurement;
}

async function fetchHourlyPm25(sensorId: number, datetimeTo: string) {
  const apiKey = process.env.OPENAQ_API_KEY;

  if (!apiKey) {
    throw new Error("OPENAQ_API_KEY is not set");
  }

  const end = new Date(datetimeTo);
  const start = new Date(end.getTime() - 48 * 60 * 60 * 1000);
  const query = new URLSearchParams({
    datetime_from: start.toISOString(),
    datetime_to: end.toISOString(),
    limit: "100"
  });

  const data = await fetchJson<OpenAQListResponse<OpenAQHourlyMeasurement>>(
    `${OPENAQ_BASE_URL}/sensors/${sensorId}/hours?${query.toString()}`,
    {
      headers: {
        "X-API-Key": apiKey
      }
    }
  );

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

  return fetchJson<OpenMeteoResponse>(
    `${OPEN_METEO_BASE_URL}/v1/forecast?${query.toString()}`
  );
}

function extractJsonObject(content: string) {
  const start = content.indexOf("{");
  const end = content.lastIndexOf("}");

  if (start === -1 || end === -1 || end <= start) {
    throw new Error("No JSON object found in model response");
  }

  return content.slice(start, end + 1);
}

function normalizeAiResponse(data: AiAttributionResponse): AiAttributionResponse {
  const importedShare = Math.max(0, Math.min(100, Math.round(data.importedShare)));
  const localShare = 100 - importedShare;
  const sources = (data.sources ?? [])
    .slice(0, 4)
    .map((source) => ({
      name: source.name.trim(),
      share: Math.max(0, Math.min(100, Math.round(source.share))),
      evidence: source.evidence.trim()
    }))
    .filter((source) => source.name && source.evidence);

  const total = sources.reduce((sum, source) => sum + source.share, 0);

  if (sources.length > 0 && total !== 100) {
    sources[0] = {
      ...sources[0],
      share: Math.max(0, Math.min(100, sources[0].share + (100 - total)))
    };
  }

  return {
    summary: data.summary.trim(),
    importedShare,
    localShare,
    confidence: data.confidence,
    sources
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
    return fallbackSnapshot.feed;
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
      const importedShare = Math.max(
        20,
        Math.min(78, estimateImportedShare(pm25, Math.round(speedAverage), directionAverage))
      );
      const localShare = 100 - importedShare;
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

async function generateAiAttribution(input: {
  pm25: number;
  category: string;
  windDirection: string;
  windDirectionDegrees: number;
  windSpeedKph: number;
  windTrail: WindPoint[];
}): Promise<AiAttributionResponse> {
  const apiKey = process.env.OPENAI_API_KEY;

  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not set");
  }

  const prompt = `You are generating a cautious air pollution source attribution for Kathmandu, Nepal.
Use only the provided data plus general domain knowledge about Kathmandu valley pollution.
Do not claim certainty. Keep attribution probabilistic.
Return ONLY minified JSON with this exact shape:
{"summary":"string","importedShare":number,"localShare":number,"confidence":"Low|Medium|High","sources":[{"name":"string","share":number,"evidence":"string"}]}

Rules:
- Use exactly 4 sources.
- Shares must sum to 100.
- Kathmandu-specific plausible sources include imported agricultural burning, Indo-Gangetic industrial haze, traffic and brick kilns, and dust resuspension.
- Keep summary under 220 characters.
- Evidence strings should be short and data-grounded.
- Do not mention AI or the model.

Data:
- city: Kathmandu
- current_pm25: ${input.pm25}
- category: ${input.category}
- current_wind_direction_cardinal: ${input.windDirection}
- current_wind_direction_degrees: ${input.windDirectionDegrees}
- current_wind_speed_kph: ${input.windSpeedKph}
- wind_trail: ${JSON.stringify(input.windTrail)}`;

  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      messages: [
        {
          role: "system",
          content:
            "You produce compact, cautious JSON for environmental attribution."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0.2,
      max_completion_tokens: 500
    }),
    cache: "no-store"
  });

  if (!response.ok) {
    throw new Error(`OpenAI request failed: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as {
    choices?: Array<{
      message?: {
        content?: string;
      };
    }>;
  };
  const content = data.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("OpenAI response was empty");
  }

  return normalizeAiResponse(JSON.parse(extractJsonObject(content)));
}

export async function getSnapshot(): Promise<CitySnapshot> {
  try {
    const [location, weather] = await Promise.all([
      fetchNearestPm25Location(),
      fetchWindData()
    ]);

    if (!location?.id) {
      throw new Error("No OpenAQ PM2.5 location found near Kathmandu");
    }

    const measurement = await fetchLatestPm25(location.id);
    const pm25 = Math.round(measurement.value);
    const category = getCategory(pm25);
    const windSpeedKph = Math.round(weather.current?.wind_speed_10m ?? 0);
    const windDirectionDegrees = weather.current?.wind_direction_10m ?? 0;
    const windDirection = directionFromDegrees(windDirectionDegrees);
    const windTrail = buildWindTrail(weather);
    const aiAttribution = await generateAiAttribution({
      pm25,
      category,
      windDirection,
      windDirectionDegrees,
      windSpeedKph,
      windTrail
    });
    const hourlyFeed =
      typeof measurement.sensorsId === "number"
        ? await fetchHourlyPm25(measurement.sensorsId, measurement.datetime?.utc ?? new Date().toISOString())
        : [];
    const feed = buildSixHourFeed(hourlyFeed, weather);

    return {
      city: kathmandu.city,
      country: kathmandu.country,
      updatedAt:
        measurement.datetime?.utc ?? weather.current?.time ?? new Date().toISOString(),
      pm25,
      category,
      importedShare: aiAttribution.importedShare,
      localShare: aiAttribution.localShare,
      confidence: aiAttribution.confidence,
      summary: aiAttribution.summary,
      coordinates: kathmandu.coordinates,
      sources: aiAttribution.sources,
      windTrail,
      feed,
      dataMode: "live",
      interpretationMode: "ai"
    };
  } catch {
    return fallbackSnapshot;
  }
}
