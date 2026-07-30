import { randomBytes } from "node:crypto";
import { NextResponse } from "next/server";
import { setSession } from "@/lib/session";

export async function GET() {
  if (process.env.NODE_ENV !== "development") {
    return new NextResponse("Not Found", { status: 404 });
  }

  await setSession({
    accessToken: "local-dashboard-preview",
    subject: "local-preview-user",
    roles: ["admin"],
    expiresAt: Math.floor(Date.now() / 1000) + 8 * 60 * 60,
    csrf: randomBytes(24).toString("base64url"),
  });

  return new NextResponse(null, {
    status: 303,
    headers: { location: "/" },
  });
}
