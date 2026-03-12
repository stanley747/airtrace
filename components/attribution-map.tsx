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
import { DivIcon, LatLngExpression } from "leaflet";
import type { CitySnapshot } from "@/lib/airtrace-data";

type AttributionMapProps = {
  city: CitySnapshot;
};

type SourceRegion = {
  center: LatLngExpression;
  label: string;
  color: string;
  radius: number;
};

const sourceRegionsByCity: Record<string, SourceRegion[]> = {
  Kathmandu: [
    {
      center: [27.1, 83.6],
      label: "Agricultural burning corridor",
      color: "#d46d34",
      radius: 42000
    },
    {
      center: [27.9, 84.25],
      label: "Indo-Gangetic haze belt",
      color: "#b88935",
      radius: 38000
    }
  ],
  Delhi: [
    {
      center: [29.0, 76.15],
      label: "Agricultural residue burning",
      color: "#d46d34",
      radius: 36000
    },
    {
      center: [28.9, 77.55],
      label: "Industrial transport corridor",
      color: "#b88935",
      radius: 28000
    }
  ],
  Lahore: [
    {
      center: [31.1, 73.65],
      label: "Punjab crop burning corridor",
      color: "#d46d34",
      radius: 40000
    },
    {
      center: [31.72, 74.9],
      label: "Regional industrial haze",
      color: "#b88935",
      radius: 32000
    }
  ],
  Dhaka: [
    {
      center: [23.55, 90.05],
      label: "Brick kiln belt",
      color: "#d46d34",
      radius: 28000
    },
    {
      center: [24.0, 89.85],
      label: "Cross-border haze corridor",
      color: "#b88935",
      radius: 46000
    }
  ],
  Karachi: [
    {
      center: [25.1, 66.65],
      label: "Arabian coastal transport",
      color: "#d46d34",
      radius: 42000
    },
    {
      center: [24.95, 67.5],
      label: "Industrial and dust corridor",
      color: "#b88935",
      radius: 34000
    }
  ],
  Kinshasa: [
    {
      center: [-4.2, 14.85],
      label: "Regional biomass transport",
      color: "#d46d34",
      radius: 52000
    },
    {
      center: [-4.62, 15.45],
      label: "Urban combustion cluster",
      color: "#b88935",
      radius: 26000
    }
  ]
};

const cityIcon = new DivIcon({
  className: "leaflet-city-pin",
  html: "<span></span>",
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

export function AttributionMap({ city }: AttributionMapProps) {
  const center: LatLngExpression = [city.coordinates.lat, city.coordinates.lng];
  const sourceRegions = sourceRegionsByCity[city.city] ?? [];
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
          Upwind source region
        </span>
      </div>
    </div>
  );
}
