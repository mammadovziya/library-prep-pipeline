import { NextRequest } from "next/server";
import { apiEventStream } from "@/lib/api";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest, context: { params: Promise<{ jobID: string }> }) {
  const { jobID } = await context.params;
  return apiEventStream(`/v1/jobs/${encodeURIComponent(jobID)}/events`, request.headers.get("last-event-id"));
}
