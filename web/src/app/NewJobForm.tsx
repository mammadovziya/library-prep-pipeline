"use client";

import { FormEvent, useRef, useState } from "react";
import { createSHA256 } from "hash-wasm";
import { UploadCloud, X } from "lucide-react";
import { useRouter } from "next/navigation";

type UploadContract = {
  id: string;
  part_size_bytes: number;
  max_parts: number;
};

function idempotencyKey(prefix: string) {
  return `${prefix}-${crypto.randomUUID()}`;
}

function bytesToBase64(bytes: Uint8Array) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

async function problem(response: Response) {
  const body = await response.json().catch(() => ({}));
  return body.detail ?? body.code ?? `Request failed (${response.status})`;
}

async function reliableFetch(input: RequestInfo | URL, init: RequestInit): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(input, init);
      if (![502, 503, 504].includes(response.status) || attempt === 2) return response;
      await response.body?.cancel();
    } catch (error) {
      lastError = error;
      if (attempt === 2) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** attempt));
  }
  throw lastError instanceof Error ? lastError : new Error("Request failed after retry");
}

export function NewJobForm({ csrf }: { csrf: string }) {
  const router = useRouter();
  const dialog = useRef<HTMLDialogElement>(null);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState("Ready");
  const [error, setError] = useState("");

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    const form = new FormData(event.currentTarget);
    const file = form.get("library") as File;
    const preset = String(form.get("preset"));
    const requestedConformers = Number(form.get("conformers"));
    try {
      if (!file || file.size < 1 || file.size > 20 * 1024 ** 3) throw new Error("Choose a file between 1 byte and 20 GB.");
      const hasher = await createSHA256();
      hasher.init();
      const hashChunk = 64 * 1024 ** 2;
      setStatus("Computing whole-file SHA-256");
      for (let offset = 0; offset < file.size; offset += hashChunk) {
        hasher.update(new Uint8Array(await file.slice(offset, Math.min(file.size, offset + hashChunk)).arrayBuffer()));
        setProgress(Math.round((Math.min(file.size, offset + hashChunk) / file.size) * 15));
      }
      const wholeChecksum = hasher.digest("hex");
      const uploadKey = idempotencyKey("upload");
      const initiate = await reliableFetch("/api/uploads", {
        method: "POST",
        headers: { "content-type": "application/json", "x-csrf-token": csrf, "idempotency-key": uploadKey },
        body: JSON.stringify({ expected_bytes: file.size, checksum_sha256: wholeChecksum, original_filename: file.name }),
      });
      if (!initiate.ok) throw new Error(await problem(initiate));
      const upload = (await initiate.json()) as UploadContract;
      const partCount = Math.ceil(file.size / upload.part_size_bytes);
      if (partCount > upload.max_parts) throw new Error("The server upload contract cannot fit this file.");
      const completed: Array<{ part_number: number; etag: string; checksum_sha256: string }> = [];
      for (let index = 0; index < partCount; index++) {
        const start = index * upload.part_size_bytes;
        const blob = file.slice(start, Math.min(file.size, start + upload.part_size_bytes));
        const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", await blob.arrayBuffer()));
        const checksum = bytesToBase64(digest);
        setStatus(`Uploading part ${index + 1} of ${partCount}`);
        const signed = await reliableFetch(`/api/uploads/${upload.id}/parts`, {
          method: "POST",
          headers: { "content-type": "application/json", "x-csrf-token": csrf },
          body: JSON.stringify({ part_number: index + 1, size_bytes: blob.size, checksum_sha256_base64: checksum }),
        });
        if (!signed.ok) throw new Error(await problem(signed));
        const { url } = (await signed.json()) as { url: string };
        const sent = await reliableFetch(url, { method: "PUT", body: blob, headers: { "x-amz-checksum-sha256": checksum } });
        if (!sent.ok) throw new Error(`Object upload failed (${sent.status}).`);
        const etag = sent.headers.get("etag");
        if (!etag) throw new Error("Object gateway did not expose the multipart ETag.");
        completed.push({ part_number: index + 1, etag, checksum_sha256: checksum });
        setProgress(15 + Math.round(((index + 1) / partCount) * 75));
      }
      setStatus("Verifying upload checksum");
      const completionKey = idempotencyKey("complete");
      const completedResponse = await reliableFetch(`/api/uploads/${upload.id}/complete`, {
        method: "POST",
        headers: { "content-type": "application/json", "x-csrf-token": csrf, "idempotency-key": completionKey },
        body: JSON.stringify({ parts: completed }),
      });
      if (!completedResponse.ok) throw new Error(await problem(completedResponse));
      setStatus("Reserving capacity and creating job");
      const jobKey = idempotencyKey("job");
      const job = await reliableFetch("/api/jobs", {
        method: "POST",
        headers: { "content-type": "application/json", "x-csrf-token": csrf, "idempotency-key": jobKey },
        body: JSON.stringify({
          input_upload_id: upload.id,
          preset,
          requested_conformers: requestedConformers,
          algorithm_version: "library-prep-alpha-1",
        }),
      });
      if (!job.ok) throw new Error(await problem(job));
      setProgress(100);
      setStatus("Queued");
      router.refresh();
      setTimeout(() => dialog.current?.close(), 500);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Upload failed.");
    } finally {
      setBusy(false);
    }
  }

  return <>
    <button className="primary" type="button" onClick={() => dialog.current?.showModal()}>New library</button>
    <dialog className="jobDialog" ref={dialog} onCancel={(event) => busy && event.preventDefault()}>
      <div className="dialogHead"><div><p className="eyebrow">New preparation</p><h2>Upload a molecular library</h2></div><button className="iconButton" disabled={busy} onClick={() => dialog.current?.close()} aria-label="Close"><X size={18} /></button></div>
      <form onSubmit={submit}>
        <label className="fileDrop"><UploadCloud /><span><strong>SMILES, CSV, TSV, or gzip</strong><small>20 GB compressed maximum · checksum verified</small></span><input name="library" type="file" required /></label>
        <div className="formGrid">
          <label><span>Preset</span><select name="preset" defaultValue="docking"><option value="docking">Docking</option><option value="enumerate">Enumerate variants</option></select></label>
          <label><span>Conformers</span><input name="conformers" type="number" min="1" max="10" defaultValue="1" required /></label>
        </div>
        <p className="policyNote">Alpha accepts trusted, non-confidential inputs only. Peak storage is estimated and reserved by the server.</p>
        {busy && <div className="progressTrack"><i style={{ width: `${progress}%` }} /></div>}
        <div className="dialogActions"><span className={error ? "formError" : "formStatus"}>{error || status}</span><button className="primary" disabled={busy} type="submit">{busy ? "Working…" : "Upload and queue"}</button></div>
      </form>
    </dialog>
  </>;
}
