"use client";

import { useRouter } from "next/navigation";
import { useTransition } from "react";

type RefreshControlsProps = {
  lastUpdated?: string;
};

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kathmandu",
    weekday: "short",
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  })
    .format(new Date(value))
    .replace(",", "") + " NPT";
}

export function RefreshControls({ lastUpdated }: RefreshControlsProps) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();

  return (
    <div className="refresh-controls">
      <button
        type="button"
        className="refresh-button"
        onClick={() => {
          startTransition(() => {
            router.refresh();
          });
        }}
        disabled={isPending}
      >
        {isPending ? "UPDATING..." : "UPDATE DATA"}
      </button>
      {lastUpdated ? (
        <div className="refresh-meta">
          <span className="status-label">LAST UPDATED</span>
          <strong>{formatTimestamp(lastUpdated)}</strong>
        </div>
      ) : null}
    </div>
  );
}
