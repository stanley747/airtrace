"use client";

import "leaflet/dist/leaflet.css";
import {
  CircleMarker,
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
  center: [number, number];
  label: string;
  color: string;
  radius: number;
};

function getEvidenceColor(kind: SourceRegion["color"]) {
  return kind;
}

function getPointStyle(kind: string) {
  switch (kind) {
    case "station":
      return { color: "#3368f6", fill: "#3368f6" };
    case "cropland-hotspot":
      return { color: "#cf4b3c", fill: "#cf4b3c" };
    case "hotspot":
      return { color: "#de6a3c", fill: "#de6a3c" };
    case "kiln":
      return { color: "#9c6b30", fill: "#9c6b30" };
    case "industrial":
      return { color: "#4b5563", fill: "#4b5563" };
    default:
      return { color: "#6b7280", fill: "#6b7280" };
  }
}

const cityIcon = new DivIcon({
  className: "leaflet-city-pin",
  html: "<span></span>",
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

export function AttributionMapClient({ city }: AttributionMapProps) {
  const center: LatLngExpression = [city.coordinates.lat, city.coordinates.lng];
  const hotspotPoints: SourceRegion[] = city.mapEvidence.hotspots.map((hotspot) => ({
    center: [hotspot.lat, hotspot.lng],
    label: hotspot.label,
    color: getPointStyle(hotspot.kind).color,
    radius: hotspot.weight ? Math.max(4, 3 + hotspot.weight * 4) : 4
  }));
  const registryPoints: SourceRegion[] = city.mapEvidence.registry.map((point) => ({
    center: [point.lat, point.lng],
    label: point.label,
    color: getPointStyle(point.kind).color,
    radius: 4
  }));
  const stationPoints: SourceRegion[] = city.mapEvidence.stations.map((station) => ({
    center: [station.lat, station.lng],
    label: station.label,
    color: getPointStyle(station.kind).color,
    radius: station.weight ? Math.max(5, 4 + station.weight / 22) : 5
  }));

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
          {city.mapEvidence.lines.map((line) => (
            <Polyline
              key={line.label}
              pathOptions={{
                color: "#15664f",
                weight: 4,
                opacity: 0.75,
                dashArray: "8 12"
              }}
              positions={line.points}
            >
              <Tooltip>{line.label}</Tooltip>
            </Polyline>
          ))}
        </Pane>

        <Pane name="hotspots" style={{ zIndex: 420 }}>
          {hotspotPoints.map((region) => (
            <CircleMarker
              key={`${region.label}-${region.center[0]}-${region.center[1]}`}
              center={region.center}
              radius={region.radius}
              pathOptions={{
                color: getEvidenceColor(region.color),
                fillColor: getEvidenceColor(region.color),
                fillOpacity: 0.6,
                weight: 1.5
              }}
            >
              <Tooltip>{region.label}</Tooltip>
            </CircleMarker>
          ))}
        </Pane>

        <Pane name="registry" style={{ zIndex: 430 }}>
          {registryPoints.map((region) => (
            <CircleMarker
              key={`${region.label}-${region.center[0]}-${region.center[1]}`}
              center={region.center}
              radius={region.radius}
              pathOptions={{
                color: getEvidenceColor(region.color),
                fillColor: getEvidenceColor(region.color),
                fillOpacity: 0.7,
                weight: 1
              }}
            >
              <Tooltip>{region.label}</Tooltip>
            </CircleMarker>
          ))}
        </Pane>

        <Pane name="stations" style={{ zIndex: 440 }}>
          {stationPoints.map((region) => (
            <CircleMarker
              key={`${region.label}-${region.center[0]}-${region.center[1]}`}
              center={region.center}
              radius={region.radius}
              pathOptions={{
                color: getEvidenceColor(region.color),
                fillColor: getEvidenceColor(region.color),
                fillOpacity: 0.9,
                weight: 2
              }}
            >
              <Tooltip>{region.label}</Tooltip>
            </CircleMarker>
          ))}
        </Pane>

        <Marker icon={cityIcon} position={center}>
          <Popup>
            <strong>{city.city}</strong>
            <div>PM2.5: {city.pm25} ug/m3</div>
            <div>Imported share: {city.importedShare}%</div>
          </Popup>
        </Marker>
      </MapContainer>
    </div>
  );
}
