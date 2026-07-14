import { NextRequest, NextResponse } from "next/server";
import { clearSession, getSession } from "@/lib/session";

export async function POST(request: NextRequest) {
  const origin = request.headers.get("origin");
  let sameOrigin = false;
  try {
    sameOrigin = Boolean(origin && new URL(origin).origin === request.nextUrl.origin);
  } catch {
    sameOrigin = false;
  }
  const session = await getSession();
  const form = await request.formData().catch(() => null);
  if (!sameOrigin || !session || form?.get("csrf") !== session.csrf) {
    return new NextResponse("Forbidden", { status: 403 });
  }
  await clearSession();
  return NextResponse.redirect(new URL("/", request.url), 303);
}
