import { NextRequest, NextResponse } from "next/server";
import { getSession } from "./session";

export async function verifyMutation(request: NextRequest): Promise<NextResponse | null> {
  const session = await getSession();
  const origin = request.headers.get("origin");
  let sameOrigin = false;
  try {
    sameOrigin = Boolean(origin && new URL(origin).origin === request.nextUrl.origin);
  } catch {
    sameOrigin = false;
  }
  if (!session || !sameOrigin || request.headers.get("x-csrf-token") !== session.csrf) {
    return NextResponse.json({ code: "csrf_failed" }, { status: 403 });
  }
  return null;
}
