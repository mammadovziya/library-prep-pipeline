import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { createRemoteJWKSet, jwtVerify } from "jose";

export type OIDCTransaction = { state: string; nonce: string; verifier: string };

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export function encodeOIDCTransaction(transaction: OIDCTransaction): string {
  const payload = Buffer.from(JSON.stringify(transaction)).toString("base64url");
  const signature = createHmac("sha256", required("SESSION_SECRET")).update(payload).digest("base64url");
  return `${payload}.${signature}`;
}

export function decodeOIDCTransaction(value: string): OIDCTransaction {
  const [payload, encodedSignature, extra] = value.split(".");
  if (!payload || !encodedSignature || extra) throw new Error("invalid OIDC transaction cookie");
  const expected = createHmac("sha256", required("SESSION_SECRET")).update(payload).digest();
  const actual = Buffer.from(encodedSignature, "base64url");
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
    throw new Error("invalid OIDC transaction signature");
  }
  const transaction = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as Partial<OIDCTransaction>;
  if (typeof transaction.state !== "string" || typeof transaction.nonce !== "string" || typeof transaction.verifier !== "string") {
    throw new Error("invalid OIDC transaction payload");
  }
  return transaction as OIDCTransaction;
}

export function beginOIDC(): { transaction: OIDCTransaction; url: string } {
  const transaction = {
    state: randomBytes(24).toString("base64url"),
    nonce: randomBytes(24).toString("base64url"),
    verifier: randomBytes(48).toString("base64url"),
  };
  const challenge = createHash("sha256").update(transaction.verifier).digest("base64url");
  const params = new URLSearchParams({
    response_type: "code",
    client_id: required("OIDC_CLIENT_ID"),
    redirect_uri: required("OIDC_REDIRECT_URI"),
    scope: "openid profile email groups",
    state: transaction.state,
    nonce: transaction.nonce,
    code_challenge: challenge,
    code_challenge_method: "S256",
  });
  return { transaction, url: `${required("OIDC_AUTHORIZATION_ENDPOINT")}?${params}` };
}

export async function completeOIDC(code: string, transaction: OIDCTransaction) {
  const response = await fetch(required("OIDC_TOKEN_ENDPOINT"), {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code,
      client_id: required("OIDC_CLIENT_ID"),
      client_secret: required("OIDC_CLIENT_SECRET"),
      redirect_uri: required("OIDC_REDIRECT_URI"),
      code_verifier: transaction.verifier,
    }),
    cache: "no-store",
  });
  if (!response.ok) throw new Error("OIDC token exchange failed");
  const tokens = (await response.json()) as { access_token: string; id_token: string; expires_in: number };
  const issuer = required("OIDC_ISSUER");
  const verified = await jwtVerify(tokens.id_token, createRemoteJWKSet(new URL(required("OIDC_JWKS_URI"))), {
    issuer,
    audience: required("OIDC_CLIENT_ID"),
  });
  if (verified.payload.nonce !== transaction.nonce || typeof verified.payload.sub !== "string") {
    throw new Error("OIDC nonce or subject validation failed");
  }
  const groups = Array.isArray(verified.payload.groups) ? verified.payload.groups.filter((value): value is string => typeof value === "string") : [];
  const expiresAt = Math.min(
    typeof verified.payload.exp === "number" ? verified.payload.exp : Number.MAX_SAFE_INTEGER,
    Math.floor(Date.now() / 1000) + tokens.expires_in,
  );
  return { accessToken: tokens.access_token, subject: verified.payload.sub, roles: groups, expiresAt };
}
