import { NextRequest, NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";
import { verifyMutation } from "@/lib/csrf";

export async function POST(request: NextRequest, context: { params: Promise<{ jobID: string }> }) {
  const rejected = await verifyMutation(request);
  if (rejected) return rejected;
  const { jobID } = await context.params;
  const result = await apiRequest(`/v1/jobs/${encodeURIComponent(jobID)}/rerun`, {
    method: "POST",
    body: "{}",
    headers: { "idempotency-key": request.headers.get("idempotency-key") ?? "" },
  });
  return NextResponse.json(result.body, { status: result.status });
}
