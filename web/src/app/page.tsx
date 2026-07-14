import { Activity, FlaskConical, Gauge, HardDrive, LockKeyhole, Server, ShieldCheck } from "lucide-react";
import Link from "next/link";
import { apiRequest } from "@/lib/api";
import { getSession } from "@/lib/session";
import { JobActions } from "./JobActions";
import { LiveJobRefresh } from "./LiveJobRefresh";
import { NewJobForm } from "./NewJobForm";

type Job = {
  id: string;
  status: string;
  preset: string;
  requested_conformers: number;
  version: number;
  created_at: string;
};

function shortID(id: string) {
  return id.slice(0, 8);
}

export default async function Home() {
  const session = await getSession();
  let jobs: Job[] = [];
  let apiAvailable = true;
  if (session) {
    const result = await apiRequest("/v1/jobs");
    apiAvailable = result.status === 200;
    jobs = apiAvailable ? ((result.body as { jobs?: Job[] })?.jobs ?? []) : [];
  }

  return (
    <main>
      <header className="topbar">
        <Link className="brand" href="/" aria-label="Library Prep home">
          <span className="brandMark"><FlaskConical size={18} /></span>
          <span>Library Prep</span>
          <span className="alpha">CONTROLLED ALPHA</span>
        </Link>
        {session ? (
          <form action="/auth/logout" method="post"><input name="csrf" type="hidden" value={session.csrf} /><button className="secondary" type="submit">Sign out</button></form>
        ) : (
          <a className="primary small" href="/auth/login">Invitation sign in</a>
        )}
      </header>

      <section className="hero">
        <div>
          <p className="eyebrow"><ShieldCheck size={15} /> Trusted inputs only</p>
          <h1>Prepare screening libraries<br />without losing provenance.</h1>
          <p className="lede">A fenced, reproducible pipeline for profiling, filtering, variant generation and GPU conformers across one RTX 5090 and five RTX 4090 workers.</p>
        </div>
        <div className="systemCard">
          <div className="systemHead"><span>Fleet posture</span><span className={apiAvailable ? "online" : "offline"}>{apiAvailable ? "READY" : "DEGRADED"}</span></div>
          <div className="systemMetric"><span>Compute</span><strong>6 GPUs</strong></div>
          <div className="systemMetric"><span>Storage ceiling</span><strong>800 GB</strong></div>
          <div className="systemMetric"><span>Data copies</span><strong>2 hosts</strong></div>
          <div className="systemMetric"><span>Input policy</span><strong>Non-confidential</strong></div>
        </div>
      </section>

      {!session ? (
        <section className="accessPanel">
          <div className="accessIcon"><LockKeyhole size={23} /></div>
          <div><h2>Invitation-only access</h2><p>Accounts require manual approval. Confidential, regulated, HIPAA and GxP workloads are not accepted during alpha.</p></div>
          <a className="primary" href="/auth/login">Continue with Authentik</a>
        </section>
      ) : (
        <section className="workspace">
          <LiveJobRefresh jobIDs={jobs.filter((job) => !["succeeded", "failed", "cancelled", "expired"].includes(job.status)).map((job) => job.id)} />
          <div className="sectionTitle"><div><p className="eyebrow">Workspace</p><h2>Recent preparation jobs</h2></div><NewJobForm csrf={session.csrf} /></div>
          <div className="jobTable" role="table" aria-label="Recent preparation jobs">
            <div className="jobRow tableHead" role="row"><span>Job</span><span>Preset</span><span>Conformers</span><span>Status</span><span>Created</span><span>Result</span></div>
            {jobs.length ? jobs.map((job) => (
              <div className="jobRow" role="row" key={job.id}>
                <span className="mono">{shortID(job.id)}</span><span>{job.preset}</span><span>{job.requested_conformers}</span>
                <span><i className={`statusDot ${job.status}`} />{job.status.replaceAll("_", " ")}</span>
                <span>{new Intl.DateTimeFormat("en", { dateStyle: "medium", timeStyle: "short" }).format(new Date(job.created_at))}</span>
                <span><JobActions jobID={job.id} status={job.status} version={job.version} csrf={session.csrf} /></span>
              </div>
            )) : <div className="empty"><FlaskConical size={27} /><h3>No preparation jobs yet</h3><p>Your approved uploads and reproducible runs will appear here.</p></div>}
          </div>
        </section>
      )}

      <section className="principles">
        <article><Server /><h3>Fenced execution</h3><p>Every task runs under a short lease. Stale attempts cannot publish or replace a winning result.</p></article>
        <article><Gauge /><h3>Shared-GPU aware</h3><p>Three idle samples are required before work starts. Colleague processes trigger a safe deferral.</p></article>
        <article><HardDrive /><h3>Peak reserved</h3><p>Admission counts inputs, working data, finalization and retries—not only the final artifact.</p></article>
        <article><Activity /><h3>Reproducible</h3><p>Image digest, packages, seeds, GPU, driver, parameters and checksums travel with every manifest.</p></article>
      </section>

      <footer><span>Library Prep Platform</span><span>Alpha objective: RTO 4h · PostgreSQL RPO 5m</span></footer>
    </main>
  );
}
