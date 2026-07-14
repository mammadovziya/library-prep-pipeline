import { NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";

export async function GET(_request: Request, context: { params: Promise<{ jobID: string }> }) {
  const { jobID } = await context.params;
  const result = await apiRequest(`/v1/jobs/${encodeURIComponent(jobID)}/artifacts`);
  return NextResponse.json(result.body, { status: result.status });
}
