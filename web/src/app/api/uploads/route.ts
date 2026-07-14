import { NextRequest, NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";
import { verifyMutation } from "@/lib/csrf";

export async function POST(request: NextRequest) {
  const rejected = await verifyMutation(request);
  if (rejected) return rejected;
  const result = await apiRequest("/v1/uploads", {
    method: "POST",
    body: await request.text(),
    headers: { "idempotency-key": request.headers.get("idempotency-key") ?? "" },
  });
  return NextResponse.json(result.body, { status: result.status });
}
