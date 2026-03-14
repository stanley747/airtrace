"use client";

import "leaflet/dist/leaflet.css";
import {
  Circle,
  MapContainer,
  Marker,
  Pane,
  Polyline,
  Popup,
  TileLayer,
  Tooltip
} from "react-leaflet";
import { DivIcon, type LatLngExpression } from "leaflet";
import type { CitySnapshot } from "@/lib/airtrace-data";

type AttributionMapProps = {
  city: CitySnapshot;
};

type SourceRegion = {
  center: LatLngExpression;
  label: string;
  radius: number;
};

function getSourceRegion(sourceName: string): SourceRegion | null {
  if (sourceName === "Kathmandu traffic and brick kilns") {
    return {
      center: [27.7172, 85.324],
      label: sourceName,
      radius: 18000
    };
  }

  if (sourceName === "Upwind fire activity" || sourceName === "Probable agricultural burning, northern India") {
    return {
      center: [27.1, 83.6],
      label: sourceName,
      radius: 42000
    };
  }

  if (sourceName === "Industrial belt across the Indo-Gangetic Plain") {
    return {
      center: [27.9, 84.25],
      label: sourceName,
      radius: 36000
    };
  }

  if (sourceName === "Dust resuspension") {
    return {
      center: [27.42, 84.55],
      label: sourceName,
      radius: 26000
    };
  }

  return null;
}

function getShareColor(share: number) {
  if (share >= 40) return "#cf4b3c";
  if (share >= 30) return "#de6a3c";
  if (share >= 20) return "#e39b46";
  return "#d7ba65";
}

const cityIcon = new DivIcon({
  className: "leaflet-city-pin",
  html: "<span></span>",
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

export function AttributionMapClient({ city }: AttributionMapProps) {
  const center: LatLngExpression = [city.coordinates.lat, city.coordinates.lng];
  const sourceRegions = [...city.sources]
    .sort((a, b) => b.share - a.share)
    .map((source) => {
      const region = getSourceRegion(source.name);

      if (!region) {
        return null;
      }

      return {
        ...region,
        radius: Math.round(region.radius * (0.72 + source.share / 100)),
        label: `${source.name} (${source.share}%)`,
        color: getShareColor(source.share)
      };
    })
    .filter((region): region is SourceRegion & { color: string } => region !== null);
  const corridorPath: LatLngExpression[] = sourceRegions.map((region) => region.center);
  corridorPath.push(center);

  return (
    <div className="leaflet-shell">
      <MapContainer
        key={city.city}
        center={center}
        zoom={8}
        scrollWheelZoom={false}
        className="leaflet-canvas"
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        <Pane name="corridor" style={{ zIndex: 410 }}>
          <Polyline
            pathOptions={{
              color: "#15664f",
              weight: 4,
              opacity: 0.75,
              dashArray: "8 12"
            }}
            positions={corridorPath}
          />
        </Pane>

        {sourceRegions.map((region) => (
          <Circle
            key={region.label}
            center={region.center}
            radius={region.radius}
            pathOptions={{
              color: region.color,
              fillColor: region.color,
              fillOpacity: 0.25,
              weight: 2
            }}
          >
            <Tooltip>{region.label}</Tooltip>
          </Circle>
        ))}

        <Marker icon={cityIcon} position={center}>
          <Popup>
            <strong>{city.city}</strong>
            <div>PM2.5: {city.pm25} ug/m3</div>
            <div>Imported share: {city.importedShare}%</div>
          </Popup>
        </Marker>
      </MapContainer>
      <div className="map-legend">
        <span>
          <i className="legend-swatch legend-swatch-green" />
          Inferred transport path
        </span>
        <span>
          <i className="legend-swatch legend-swatch-orange" />
          Contributor region
        </span>
      </div>
    </div>
  );
}
