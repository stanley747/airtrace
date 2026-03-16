"use client";

import { useEffect, useState } from "react";

import type { TimelineFrame } from "@/lib/airtrace-data";
import { TimelinePlaybackMap } from "@/components/timeline-playback-map";

const FRAME_INTERVAL_MS = 850;

function formatFrameTime(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kathmandu",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    month: "short",
    day: "2-digit"
  })
    .format(new Date(value))
    .replace(",", "");
}

type TimelinePlayerProps = {
  cityName: string;
  coordinates: {
    lat: number;
    lng: number;
  };
  frames: TimelineFrame[];
};

export function TimelinePlayer({ cityName, coordinates, frames }: TimelinePlayerProps) {
  const [activeIndex, setActiveIndex] = useState(() =>
    frames.length > 0 ? frames.length - 1 : 0
  );
  const [isPlaying, setIsPlaying] = useState(false);

  useEffect(() => {
    if (!isPlaying || frames.length <= 1) {
      return undefined;
    }

    const intervalId = window.setInterval(() => {
      setActiveIndex((current) => {
        if (current >= frames.length - 1) {
          return 0;
        }

        return current + 1;
      });
    }, FRAME_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [frames.length, isPlaying]);

  if (frames.length === 0) {
    const placeholderHeights = [28, 34, 42, 56, 70, 64, 58, 52, 48, 44, 36, 30];

    return (
      <div className="timeline-player timeline-empty">
        <div className="timeline-head">
          <span className="note-label">24H PLAYBACK</span>
        </div>
        <div className="timeline-empty-copy">
          <strong>Hourly playback is still warming up.</strong>
          <p>
            AirTrace only shows this timeline when it has a recent sequence of hourly
            PM2.5 measurements to replay. The live snapshot above is still current.
          </p>
        </div>
        <div className="timeline-empty-track" aria-hidden="true">
          {placeholderHeights.map((height, index) => (
            <span
              key={height}
              className="timeline-empty-segment"
              style={{
                height: `${height}%`,
                opacity: 0.3 + index * 0.03
              }}
            />
          ))}
        </div>
        <div className="timeline-empty-badges">
          <span className="timeline-empty-badge">Live snapshot active</span>
          <span className="timeline-empty-badge">Waiting for hourly PM2.5 history</span>
          <span className="timeline-empty-badge">No fabricated frames</span>
        </div>
      </div>
    );
  }

  const safeActiveIndex = Math.min(activeIndex, frames.length - 1);
  const activeFrame = frames[safeActiveIndex] ?? frames[frames.length - 1];

  return (
    <div className="timeline-player">
      <TimelinePlaybackMap
        cityName={cityName}
        coordinates={coordinates}
        frames={frames}
        activeIndex={safeActiveIndex}
        isPlaying={isPlaying}
        onTogglePlayback={() => {
          setIsPlaying((current) => !current);
        }}
      />

      <div className="timeline-head">
        <span className="note-label">24H PLAYBACK</span>
        <span className="timeline-range">
          {formatFrameTime(frames[0].timestamp)} to{" "}
          {formatFrameTime(frames[frames.length - 1].timestamp)}
        </span>
      </div>

      <div className="timeline-controls">
        <div className="timeline-active">
          <strong>{formatFrameTime(activeFrame.timestamp)}</strong>
          <span>
            AQI {activeFrame.aqi} {activeFrame.aqiCategory}
          </span>
        </div>
      </div>

      <div
        className="timeline-track"
        aria-label="24 hour pollution playback"
        style={{
          gridTemplateColumns: `repeat(${frames.length}, minmax(0, 1fr))`
        }}
      >
        {frames.map((frame, index) => (
          <button
            key={frame.timestamp}
            type="button"
            className={`timeline-step${index === safeActiveIndex ? " is-active" : ""}`}
            onClick={() => {
              setActiveIndex(index);
              setIsPlaying(false);
            }}
            aria-label={`Show frame ${formatFrameTime(frame.timestamp)}`}
          >
            <span
              className="timeline-step-bar"
              style={{
                height: `${Math.max(20, Math.min(100, (frame.pm25 / 80) * 100))}%`
              }}
            />
          </button>
        ))}
      </div>

      <div className="timeline-stats">
        <div className="timeline-stat">
          <span>PM2.5</span>
          <strong>{activeFrame.pm25} ug/m3</strong>
        </div>
        <div className="timeline-stat">
          <span>Imported</span>
          <strong>{activeFrame.importedShare}%</strong>
        </div>
        <div className="timeline-stat">
          <span>Local</span>
          <strong>{activeFrame.localShare}%</strong>
        </div>
        <div className="timeline-stat">
          <span>Wind</span>
          <strong>
            {activeFrame.windDirection} {activeFrame.windSpeedKph} kph
          </strong>
        </div>
      </div>

      <div className="timeline-callout">
        <span className="note-label">FRAME READOUT</span>
        <p>
          {activeFrame.dominantSource} led this hour with {activeFrame.confidence.toLowerCase()}{" "}
          confidence.
        </p>
      </div>
    </div>
  );
}
