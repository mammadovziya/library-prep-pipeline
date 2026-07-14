"use client";

import { Download, X } from "lucide-react";
import { useRef, useState } from "react";

type Artifact = {
  id: string;
  kind: string;
  size_bytes: number;
  media_type: string;
  checksum_sha256: string;
};

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let unit = -1;
  do {
    size /= 1024;
    unit += 1;
  } while (size >= 1024 && unit < units.length - 1);
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[unit]}`;
}

export function DownloadButton({ jobID, csrf }: { jobID: string; csrf: string }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [busy, setBusy] = useState(false);
  const [signing, setSigning] = useState<string | null>(null);
  const [error, setError] = useState("");

  async function openResults() {
    setBusy(true);
    setError("");
    try {
      const listed = await fetch(`/api/jobs/${jobID}/artifacts`, { cache: "no-store" });
      if (!listed.ok) throw new Error("Result list unavailable");
      const body = await listed.json() as { artifacts?: Artifact[] };
      if (!body.artifacts?.length) throw new Error("No result artifacts are available");
      setArtifacts(body.artifacts);
      dialog.current?.showModal();
    } catch (caught) {
      window.alert(caught instanceof Error ? caught.message : "Results are unavailable");
    } finally {
      setBusy(false);
    }
  }

  async function download(artifact: Artifact) {
    const target = window.open("about:blank", "_blank");
    if (target) target.opener = null;
    setSigning(artifact.id);
    setError("");
    try {
      const signed = await fetch(`/api/artifacts/${artifact.id}/download`, {
        method: "POST",
        headers: { "x-csrf-token": csrf },
      });
      if (!signed.ok) throw new Error("The signed URL could not be issued");
      const { url } = await signed.json() as { url?: string };
      if (!url) throw new Error("The signed URL response was invalid");
      if (target) target.location.replace(url);
      else window.location.assign(url);
    } catch (caught) {
      target?.close();
      setError(caught instanceof Error ? caught.message : "Download unavailable");
    } finally {
      setSigning(null);
    }
  }

  return <>
    <button className="downloadButton" disabled={busy} onClick={openResults} aria-label="Open result artifacts"><Download size={14} />{busy ? "Loading" : "Results"}</button>
    <dialog className="resultDialog" ref={dialog} onClose={() => setError("")}>
      <div className="dialogHead">
        <div><p className="eyebrow">Committed output</p><h2>Result artifacts</h2></div>
        <button className="iconButton" type="button" aria-label="Close results" onClick={() => dialog.current?.close()}><X size={17} /></button>
      </div>
      <div className="artifactList">
        {artifacts.map((artifact) => <article key={artifact.id}>
          <div><strong>{artifact.kind.replaceAll("_", " ")}</strong><small>{formatBytes(artifact.size_bytes)} · SHA-256 {artifact.checksum_sha256.slice(0, 12)}…</small></div>
          <button className="downloadButton" type="button" disabled={signing !== null} onClick={() => download(artifact)}><Download size={14} />{signing === artifact.id ? "Signing" : "Download"}</button>
        </article>)}
      </div>
      {error ? <p className="resultError" role="alert">{error}</p> : null}
    </dialog>
  </>;
}
