"use client";

import dynamic from "next/dynamic";

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

const TimelinePlaybackMapClient = dynamic(
  () =>
    import("@/components/timeline-playback-map-client").then(
      (module) => module.TimelinePlaybackMapClient
    ),
  {
    ssr: false,
    loading: () => <div className="timeline-map-shell" />
  }
);

export function TimelinePlaybackMap(props: TimelinePlaybackMapProps) {
  return <TimelinePlaybackMapClient {...props} />;
}
