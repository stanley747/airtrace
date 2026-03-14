"use client";

import type { CitySnapshot } from "@/lib/airtrace-data";
import { AttributionMapClient } from "@/components/attribution-map-client";

type AttributionMapProps = {
  city: CitySnapshot;
};

export function AttributionMap({ city }: AttributionMapProps) {
  return <AttributionMapClient city={city} />;
}
