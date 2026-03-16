"use client";

import type { CitySnapshot } from "@/lib/airtrace-data";
import dynamic from "next/dynamic";

type AttributionMapProps = {
  city: CitySnapshot;
};

const AttributionMapClient = dynamic(
  () =>
    import("@/components/attribution-map-client").then(
      (module) => module.AttributionMapClient
    ),
  {
    ssr: false,
    loading: () => <div className="leaflet-shell" />
  }
);

export function AttributionMap({ city }: AttributionMapProps) {
  return <AttributionMapClient city={city} />;
}
