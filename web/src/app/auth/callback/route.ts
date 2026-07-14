import { randomBytes } from "node:crypto";
import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";
import { completeOIDC, decodeOIDCTransaction } from "@/lib/oidc";
import { setSession } from "@/lib/session";

export async function GET(request: NextRequest) {
  const code = request.nextUrl.searchParams.get("code");
  const state = request.nextUrl.searchParams.get("state");
  const encoded = (await cookies()).get("lp_oidc")?.value;
  if (!code || !state || !encoded) return NextResponse.redirect(new URL("/?auth=failed", request.url));
  try {
    const transaction = decodeOIDCTransaction(encoded);
    if (transaction.state !== state) throw new Error("OIDC state mismatch");
    const identity = await completeOIDC(code, transaction);
    await setSession({ ...identity, csrf: randomBytes(24).toString("base64url") });
    const response = NextResponse.redirect(new URL("/", request.url));
    response.cookies.delete("lp_oidc");
    return response;
  } catch {
    return NextResponse.redirect(new URL("/?auth=failed", request.url));
  }
}
