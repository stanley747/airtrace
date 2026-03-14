"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

const TEN_MINUTES_MS = 10 * 60 * 1000;

export function AutoRefresh() {
  const router = useRouter();

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      router.refresh();
    }, TEN_MINUTES_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [router]);

  return null;
}
