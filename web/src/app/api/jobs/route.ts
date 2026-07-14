import { NextRequest, NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";
import { verifyMutation } from "@/lib/csrf";

export async function GET() {
  const result = await apiRequest("/v1/jobs");
  return NextResponse.json(result.body, { status: result.status });
}

export async function POST(request: NextRequest) {
  const rejected = await verifyMutation(request);
  if (rejected) return rejected;
  const body = await request.text();
  const result = await apiRequest("/v1/jobs", {
    method: "POST",
    body,
    headers: { "idempotency-key": request.headers.get("idempotency-key") ?? "" },
  });
  return NextResponse.json(result.body, { status: result.status });
}
