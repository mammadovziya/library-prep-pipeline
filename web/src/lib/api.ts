import { readFileSync } from "node:fs";
import { Agent, request } from "undici";
import { getSession } from "./session";

let dispatcher: Agent | undefined;

function getDispatcher(): Agent {
  if (!dispatcher) {
    dispatcher = new Agent({
      connect: {
        ca: readFileSync(process.env.INTERNAL_CA_FILE!),
        cert: readFileSync(process.env.INTERNAL_CERT_FILE!),
        key: readFileSync(process.env.INTERNAL_KEY_FILE!),
        servername: process.env.INTERNAL_API_SERVER_NAME ?? "api.internal",
      },
    });
  }
  return dispatcher;
}

export async function apiRequest(path: string, init?: { method?: string; body?: string; headers?: Record<string, string> }) {
  const session = await getSession();
  if (!session) return { status: 401, body: null };
  try {
    const response = await request(`${process.env.INTERNAL_API_URL}${path}`, {
      dispatcher: getDispatcher(),
      method: init?.method ?? "GET",
      body: init?.body,
      headers: {
        authorization: `Bearer ${session.accessToken}`,
        "content-type": "application/json",
        ...init?.headers,
      },
      headersTimeout: 10_000,
      bodyTimeout: 30_000,
    });
    const body = await response.body.json().catch(() => null);
    return { status: response.statusCode, body };
  } catch {
    return { status: 503, body: { code: "control_plane_unavailable", detail: "The control plane is temporarily unavailable." } };
  }
}

export async function apiEventStream(path: string, lastEventID: string | null): Promise<Response> {
  const session = await getSession();
  if (!session) return Response.json({ code: "unauthorized" }, { status: 401 });
  try {
    const upstream = await request(`${process.env.INTERNAL_API_URL}${path}`, {
      dispatcher: getDispatcher(),
      method: "GET",
      headers: {
        authorization: `Bearer ${session.accessToken}`,
        accept: "text/event-stream",
        ...(lastEventID ? { "last-event-id": lastEventID } : {}),
      },
      headersTimeout: 10_000,
      bodyTimeout: 0,
    });
    if (upstream.statusCode !== 200) {
      const body = await upstream.body.json().catch(() => ({ code: "stream_unavailable" }));
      return Response.json(body, { status: upstream.statusCode });
    }
    const source = upstream.body;
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        try {
          for await (const chunk of source) {
            if (controller.desiredSize !== null && controller.desiredSize <= 0) {
              source.destroy(new Error("SSE client is too slow"));
              controller.error(new Error("SSE client is too slow"));
              return;
            }
            controller.enqueue(new Uint8Array(chunk));
          }
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
      cancel() {
        source.destroy();
      },
    });
    return new Response(stream, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        "x-accel-buffering": "no",
      },
    });
  } catch {
    return Response.json({ code: "control_plane_unavailable" }, { status: 503 });
  }
}
