import { NextRequest, NextResponse } from "next/server";
import { apiRequest } from "@/lib/api";
import { verifyMutation } from "@/lib/csrf";

export async function POST(request: NextRequest, context: { params: Promise<{ uploadID: string }> }) {
  const rejected = await verifyMutation(request);
  if (rejected) return rejected;
  const { uploadID } = await context.params;
  const result = await apiRequest(`/v1/uploads/${encodeURIComponent(uploadID)}/complete`, {
    method: "POST",
    body: await request.text(),
    headers: { "idempotency-key": request.headers.get("idempotency-key") ?? "" },
  });
  return NextResponse.json(result.body, { status: result.status });
}
