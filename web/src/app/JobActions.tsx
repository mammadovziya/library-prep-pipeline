"use client";

import { RotateCcw, XCircle } from "lucide-react";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { DownloadButton } from "./DownloadButton";

function key(operation: string) {
  return `${operation}-${crypto.randomUUID()}`;
}

async function mutateWithRetry(url: string, headers: Record<string, string>) {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(url, { method: "POST", headers });
      if (![502, 503, 504].includes(response.status) || attempt === 2) return response;
      await response.body?.cancel();
    } catch (error) {
      lastError = error;
      if (attempt === 2) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** attempt));
  }
  throw lastError instanceof Error ? lastError : new Error("Job action failed after retry");
}

export function JobActions({ jobID, status, version, csrf }: { jobID: string; status: string; version: number; csrf: string }) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);

  async function mutate(operation: "cancel" | "rerun") {
    if (operation === "cancel" && !window.confirm("Cancel this job? The winning final commit may already be in progress.")) return;
    setBusy(true);
    try {
      const response = await mutateWithRetry(`/api/jobs/${jobID}/${operation}`, {
        "x-csrf-token": csrf,
        "idempotency-key": key(operation),
        ...(operation === "cancel" ? { "if-match": String(version) } : {}),
      });
      if (!response.ok) {
        const problem = await response.json().catch(() => ({}));
        throw new Error(problem.detail ?? `${operation} failed`);
      }
      router.refresh();
    } catch (caught) {
      window.alert(caught instanceof Error ? caught.message : "The job action failed");
    } finally {
      setBusy(false);
    }
  }

  if (status === "succeeded") return <span className="jobActions"><DownloadButton jobID={jobID} csrf={csrf} /><button className="downloadButton" disabled={busy} onClick={() => mutate("rerun")}><RotateCcw size={13} />Rerun</button></span>;
  if (status === "failed") return <button className="downloadButton" disabled={busy} onClick={() => mutate("rerun")}><RotateCcw size={13} />{busy ? "Queuing" : "Rerun"}</button>;
  if (status === "cancelled" || status === "expired" || status === "cancel_requested") return <span>—</span>;
  return <button className="downloadButton danger" disabled={busy} onClick={() => mutate("cancel")}><XCircle size={13} />{busy ? "Cancelling" : "Cancel"}</button>;
}
