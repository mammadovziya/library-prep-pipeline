import { NextResponse } from "next/server";
import { beginOIDC, encodeOIDCTransaction } from "@/lib/oidc";

export async function GET() {
  const { transaction, url } = beginOIDC();
  const response = NextResponse.redirect(url);
  response.cookies.set("lp_oidc", encodeOIDCTransaction(transaction), {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/auth/callback",
    maxAge: 600,
  });
  return response;
}
