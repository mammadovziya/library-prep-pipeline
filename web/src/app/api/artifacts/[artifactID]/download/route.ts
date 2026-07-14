import { NextRequest, NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";
import { verifyMutation } from "@/lib/csrf";

export async function POST(request: NextRequest, context: { params: Promise<{ artifactID: string }> }) {
  const rejected = await verifyMutation(request);
  if (rejected) return rejected;
  const { artifactID } = await context.params;
  const result = await apiRequest(`/v1/artifacts/${encodeURIComponent(artifactID)}/download`, { method: "POST", body: "{}" });
  return NextResponse.json(result.body, { status: result.status });
}
