"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

const progressEvents = [
  "task.started", "task.succeeded", "task.retry_scheduled", "task.capacity_deferred",
  "task.split", "record.quarantined", "job.sharded", "job.finalizing", "job.succeeded",
  "job.cancel_requested", "job.cancelled", "task.failed", "task.quarantined", "task.delivery_exhausted",
];

export function LiveJobRefresh({ jobIDs }: { jobIDs: string[] }) {
  const router = useRouter();
  const jobKey = jobIDs.join(",");

  useEffect(() => {
    let refreshTimer: ReturnType<typeof setTimeout> | undefined;
    const refresh = () => {
      clearTimeout(refreshTimer);
      refreshTimer = setTimeout(() => router.refresh(), 750);
    };
    const streams = (jobKey ? jobKey.split(",") : []).map((jobID) => {
      const stream = new EventSource(`/api/jobs/${encodeURIComponent(jobID)}/events`);
      for (const event of progressEvents) stream.addEventListener(event, refresh);
      return stream;
    });
    return () => {
      clearTimeout(refreshTimer);
      for (const stream of streams) stream.close();
    };
  }, [jobKey, router]);

  return null;
}
