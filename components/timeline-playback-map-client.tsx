"use client";

import "leaflet/dist/leaflet.css";
import {
  CircleMarker,
  MapContainer,
  Marker,
  Pane,
  Polyline,
  TileLayer,
  Tooltip
} from "react-leaflet";
import { DivIcon, type LatLngExpression } from "leaflet";

import type { TimelineFrame } from "@/lib/airtrace-data";

type TimelinePlaybackMapProps = {
  cityName: string;
  coordinates: {
    lat: number;
    lng: number;
  };
  frames: TimelineFrame[];
  activeIndex: number;
  isPlaying: boolean;
  onTogglePlayback: () => void;
};

function getTransportColor(windSpeedKph: number) {
  if (windSpeedKph >= 16) return "#a95639";
  if (windSpeedKph >= 11) return "#bf7347";
  if (windSpeedKph >= 6) return "#d59b58";
  return "#dec8a4";
}

function getConfidenceOpacity(confidence: TimelineFrame["confidence"]) {
  if (confidence === "High") return 0.86;
  if (confidence === "Medium") return 0.64;
  return 0.42;
}

const cityIcon = new DivIcon({
  className: "leaflet-city-pin",
  html: "<span></span>",
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

export function TimelinePlaybackMapClient({
  cityName,
  coordinates,
  frames,
  activeIndex,
  isPlaying,
  onTogglePlayback
}: TimelinePlaybackMapProps) {
  const safeActiveIndex = Math.min(activeIndex, Math.max(0, frames.length - 1));
  const activeFrame = frames[safeActiveIndex] ?? null;
  const center: LatLngExpression = [coordinates.lat, coordinates.lng];
  const activePath = activeFrame?.transportPath ?? [];
  const origin = activePath[0] ?? null;
  const recentOrigins = frames
    .slice(Math.max(0, safeActiveIndex - 5), safeActiveIndex + 1)
    .map((frame, index, slice) => {
      const point = frame.transportPath[0];

      if (!point) {
        return null;
      }

      return {
        point,
        timestamp: frame.timestamp,
        color: getTransportColor(frame.windSpeedKph),
        opacity:
          getConfidenceOpacity(frame.confidence) *
          (0.38 + (index / Math.max(1, slice.length - 1)) * 0.48)
      };
    })
    .filter(
      (item): item is {
        point: [number, number];
        timestamp: string;
        color: string;
        opacity: number;
      } => item !== null
    );
  const transportColor = getTransportColor(activeFrame?.windSpeedKph ?? 0);
  const transportOpacity = getConfidenceOpacity(activeFrame?.confidence ?? "Low");

  return (
    <div className="timeline-map-shell">
      <div className="timeline-map-overlay">
        <button
          type="button"
          className="timeline-map-play"
          onClick={onTogglePlayback}
        >
          {isPlaying ? "Pause" : "Play"}
        </button>
        {activeFrame ? (
          <div className="timeline-map-frame">
            <strong>
              {activeFrame.windDirection} {activeFrame.windSpeedKph} kph
            </strong>
            <span>
              {activeFrame.importedShare}% imported / {activeFrame.localShare}% local
            </span>
          </div>
        ) : null}
      </div>

      <MapContainer
        center={center}
        zoom={7}
        zoomControl={false}
        scrollWheelZoom={false}
        dragging={false}
        doubleClickZoom={false}
        boxZoom={false}
        keyboard={false}
        attributionControl={false}
        className="timeline-map-canvas"
      >
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        <Pane name="history-origins" style={{ zIndex: 410 }}>
          {recentOrigins.map((originPoint) => (
            <CircleMarker
              key={`${originPoint.timestamp}-${originPoint.point[0]}-${originPoint.point[1]}`}
              center={originPoint.point}
              radius={4}
              pathOptions={{
                color: originPoint.color,
                fillColor: originPoint.color,
                fillOpacity: originPoint.opacity,
                opacity: originPoint.opacity,
                weight: 1
              }}
            />
          ))}
        </Pane>

        <Pane name="active-transport" style={{ zIndex: 420 }}>
          {activePath.length >= 2 ? (
            <Polyline
              positions={activePath}
              pathOptions={{
                color: transportColor,
                weight: 5,
                opacity: transportOpacity,
                lineCap: "round",
                lineJoin: "round"
              }}
            >
              <Tooltip sticky>
                {activeFrame?.windDirection} transport at {activeFrame?.windSpeedKph ?? 0} kph
              </Tooltip>
            </Polyline>
          ) : null}
        </Pane>

        <Pane name="active-origin" style={{ zIndex: 430 }}>
          {origin ? (
            <CircleMarker
              center={origin}
              radius={8}
              pathOptions={{
                color: transportColor,
                fillColor: transportColor,
                fillOpacity: Math.max(0.2, transportOpacity * 0.42),
                weight: 2
              }}
            >
              <Tooltip sticky>Active upwind origin for this frame</Tooltip>
            </CircleMarker>
          ) : null}
        </Pane>

        <Marker icon={cityIcon} position={center}>
          <Tooltip direction="top" offset={[0, -10]} permanent>
            {cityName}
          </Tooltip>
        </Marker>
      </MapContainer>
    </div>
  );
}
