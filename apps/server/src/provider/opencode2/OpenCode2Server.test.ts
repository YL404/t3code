import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  HostProcessEnvironment,
  HostProcessExecutablePath,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";
import { describe } from "vite-plus/test";

import * as OpenCodeRuntime from "../opencodeRuntime.ts";
import * as OpenCodeServerLedger from "../OpenCodeServerLedger.ts";
import * as OpenCode2Client from "./OpenCode2Client.ts";
import * as OpenCode2Server from "./OpenCode2Server.ts";

// Shapes and content types observed on `opencode serve` 2.0.18 (pid and port replaced). 1.x
// answers /api/info with its web UI: 200 text/html.
const INFO_BODY =
  '{"version":"2.0.18","pid":4242,"urls":["http://127.0.0.1:4096"],"paths":{"tmp":"/tmp/opencode"}}';
const UNAUTHORIZED_BODY = '{"_tag":"UnauthorizedError","message":"Authentication required"}';
const SPA_BODY = "<!doctype html><html><head><title>OpenCode</title></head></html>";

const serverReplying = (reply: {
  readonly status: number;
  readonly contentType?: string;
  readonly body: string;
}) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(reply.body, {
            status: reply.status,
            headers: reply.contentType === undefined ? {} : { "content-type": reply.contentType },
          }),
        ),
      ),
    ),
  );

const verify = (httpClient: Layer.Layer<HttpClient.HttpClient>, url = "http://127.0.0.1:4096") =>
  Effect.gen(function* () {
    const opencode = yield* OpenCode2Client.OpenCode2Client;
    const { client } = yield* opencode.connect({ baseUrl: url, password: "secret" });
    return yield* OpenCode2Server.verifyServer(client);
  }).pipe(Effect.provide(OpenCode2Client.layer.pipe(Layer.provide(httpClient))));

describe("OpenCode2Server.verifyServer", () => {
  it.effect("returns the version of an authenticated OpenCode 2 server", () =>
    Effect.gen(function* () {
      const version = yield* verify(
        serverReplying({ status: 200, contentType: "application/json", body: INFO_BODY }),
      );
      assert.strictEqual(version, "2.0.18");
    }),
  );

  it.effect("reports a rejected password, not a wrong server", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        serverReplying({ status: 401, contentType: "application/json", body: UNAUTHORIZED_BODY }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  // 1.x rejects a wrong password with an empty 401, which the client cannot decode.
  it.effect("reports an empty-body 401 as a rejected password", () =>
    Effect.gen(function* () {
      const error = yield* verify(serverReplying({ status: 401, body: "" })).pipe(Effect.flip);
      assert.include(error.detail, "rejected the server password");
    }),
  );

  it.effect("reports a server error as a server error, not as unreachable", () =>
    Effect.gen(function* () {
      for (const status of [500, 502]) {
        const error = yield* verify(
          serverReplying({ status, contentType: "text/plain", body: "upstream failed" }),
        ).pipe(Effect.flip);
        assert.include(error.detail, `returned HTTP ${status}`);
      }
    }),
  );

  // 1.x (and the 2.x web UI on unknown paths) answers /api/info with 200 HTML.
  it.effect("rejects a server that answers with the web UI's HTML", () =>
    Effect.gen(function* () {
      const error = yield* verify(
        serverReplying({ status: 200, contentType: "text/html", body: SPA_BODY }),
      ).pipe(Effect.flip);
      assert.include(error.detail, "is not an OpenCode 2 server");
    }),
  );

  it.effect("reports an unreachable server as unreachable", () =>
    Effect.gen(function* () {
      const error = yield* verify(FetchHttpClient.layer, "http://127.0.0.1:1").pipe(Effect.flip);
      assert.include(error.detail, "Could not reach the OpenCode server");
    }),
  );
});

describe("OpenCode2Server error details", () => {
  // `detail` reaches clients through the provider status message, and a
  // `serverUrl` can carry credentials in its userinfo or query.
  const serverUrl = "http://user:url-secret@127.0.0.1:1/?token=query-secret";
  const detailFor = (httpClient: Layer.Layer<HttpClient.HttpClient>) =>
    Effect.gen(function* () {
      const server = yield* OpenCode2Server.make({
        binaryPath: "opencode",
        serverUrl,
        serverPassword: "secret",
        directory: "/project",
        environment: {},
      });
      const error = yield* server.withConnection(() => Effect.void).pipe(Effect.flip);
      return error.detail;
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          OpenCode2Client.layer.pipe(Layer.provide(httpClient)),
          OpenCodeRuntime.OpenCodeRuntimeLive.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
        ).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
    );

  it.effect("never include the server URL", () =>
    Effect.gen(function* () {
      const hanging = Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.never),
      );
      const timedOut = yield* detailFor(hanging).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      const details = [
        yield* detailFor(FetchHttpClient.layer),
        yield* detailFor(serverReplying({ status: 401, body: "" })),
        yield* detailFor(serverReplying({ status: 502, contentType: "text/plain", body: "" })),
        yield* detailFor(serverReplying({ status: 200, contentType: "text/html", body: SPA_BODY })),
        yield* Fiber.join(timedOut),
      ];
      assert.deepStrictEqual(details, [
        "Could not reach the OpenCode server.",
        "The OpenCode server rejected the server password.",
        "The OpenCode server returned HTTP 502.",
        "The server is not an OpenCode 2 server.",
        "Timed out waiting for the OpenCode server.",
      ]);
      for (const detail of details) {
        assert.notInclude(detail, "url-secret");
        assert.notInclude(detail, "query-secret");
        assert.notInclude(detail, "127.0.0.1");
      }
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("OpenCode2Server external server", () => {
  // An external server was not started here, so charging startup a catalog wait
  // would only add latency: one /api/info check, no event subscription.
  it.effect("returns without waiting for a model catalog it did not build", () => {
    const requests: Array<string> = [];
    const httpClient = Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make((request) =>
        Effect.sync(() => {
          requests.push(String(request.url));
          return HttpClientResponse.fromWeb(
            request,
            new Response(INFO_BODY, {
              status: 200,
              headers: { "content-type": "application/json" },
            }),
          );
        }),
      ),
    );
    return Effect.gen(function* () {
      const server = yield* OpenCode2Server.make({
        binaryPath: "opencode",
        serverUrl: "http://127.0.0.1:4096",
        serverPassword: "secret",
        directory: "/project",
        environment: {},
      });
      const connection = yield* server.withConnection((value) => Effect.succeed(value));
      assert.isTrue(connection.external);
      assert.strictEqual(requests.length, 1);
      assert.include(requests[0], "/api/info");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          OpenCode2Client.layer.pipe(Layer.provide(httpClient)),
          OpenCodeRuntime.OpenCodeRuntimeLive.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
        ).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
    );
  });
});

describe("OpenCode2Server.settleModelCatalog", () => {
  const catalogEvent = (type: string, directory: string) =>
    ({ type, location: { directory } }) as unknown as OpenCode2Client.OpenCode2StreamEvent;

  const settle = <ReadE, StreamE, EventsE>(input: {
    readonly reads: Array<string>;
    readonly read?: Effect.Effect<void, ReadE>;
    readonly events: Effect.Effect<
      Stream.Stream<OpenCode2Client.OpenCode2StreamEvent, StreamE>,
      EventsE
    >;
  }) =>
    OpenCode2Server.settleModelCatalog({
      directory: "/project",
      read:
        input.read ??
        Effect.sync(() => {
          input.reads.push("read");
        }),
      events: input.events,
    });

  it.effect("reads once and settles on the directory's model.updated", () =>
    Effect.gen(function* () {
      const reads: Array<string> = [];
      const settling = yield* settle({
        reads,
        events: Effect.succeed(
          Stream.make(
            catalogEvent("model.updated", "/elsewhere"),
            catalogEvent("provider.updated", "/project"),
            catalogEvent("model.updated", "/project"),
          ).pipe(Stream.concat(Stream.never)),
        ),
      }).pipe(Effect.forkChild);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      // Settling on the matching event; waiting for the stream to end would hang.
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      assert.deepStrictEqual(reads, ["read"]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("stops waiting after the timeout when no matching event arrives", () =>
    Effect.gen(function* () {
      const reads: Array<string> = [];
      const settling = yield* settle({
        reads,
        events: Effect.succeed(
          Stream.make(catalogEvent("model.updated", "/elsewhere")).pipe(
            Stream.concat(Stream.never),
          ),
        ),
      }).pipe(Effect.forkChild);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.isUndefined(settling.pollUnsafe());
      yield* TestClock.adjust("5 seconds");
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      assert.deepStrictEqual(reads, ["read"]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("shares one deadline between the read and the wait", () =>
    Effect.gen(function* () {
      const reads: Array<string> = [];
      const settling = yield* settle({
        reads,
        read: Effect.sleep("4 seconds").pipe(
          Effect.andThen(
            Effect.sync(() => {
              reads.push("read");
            }),
          ),
        ),
        events: Effect.succeed(Stream.never),
      }).pipe(Effect.forkChild);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      yield* TestClock.adjust("4 seconds");
      // The read is done and the wait is running; both share one five-second budget.
      assert.isUndefined(settling.pollUnsafe());
      yield* TestClock.adjust("1 second");
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      assert.deepStrictEqual(reads, ["read"]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("lends the server when the event stream is unavailable", () =>
    Effect.gen(function* () {
      const reads: Array<string> = [];
      const settling = yield* settle({ reads, events: Effect.fail("no event stream") }).pipe(
        Effect.forkChild,
      );
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      assert.deepStrictEqual(reads, ["read"]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  const releasing = () => {
    let released = false;
    const stream = Stream.never.pipe(
      Stream.ensuring(
        Effect.sync(() => {
          released = true;
        }),
      ),
    );
    return { stream, released: () => released };
  };

  it.effect("releases the event subscription when the read fails", () =>
    Effect.gen(function* () {
      const { stream, released } = releasing();
      const settling = yield* settle({
        reads: [],
        read: Effect.fail("read failed"),
        events: Effect.succeed(stream),
      }).pipe(Effect.forkChild);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      // The child fiber is interrupted with the parent's scope; let that land.
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.isTrue(released());
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("releases the event subscription when the read outlives the deadline", () =>
    Effect.gen(function* () {
      const { stream, released } = releasing();
      const settling = yield* settle({
        reads: [],
        read: Effect.never,
        events: Effect.succeed(stream),
      }).pipe(Effect.forkChild);
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.isUndefined(settling.pollUnsafe());
      yield* TestClock.adjust("5 seconds");
      assert.strictEqual(settling.pollUnsafe()?._tag, "Success");
      // The child fiber is interrupted with the parent's scope; let that land.
      for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
      assert.isTrue(released());
    }).pipe(Effect.provide(TestClock.layer())),
  );
});

describe("OpenCode2Server passwords", () => {
  it.effect("generates a distinct 256-bit password per server", () =>
    Effect.gen(function* () {
      const first = yield* OpenCode2Server.generatePassword;
      const second = yield* OpenCode2Server.generatePassword;
      assert.match(Redacted.value(first), /^[A-Za-z0-9_-]{43}$/);
      assert.notStrictEqual(Redacted.value(first), Redacted.value(second));
      assert.notInclude(String(first), Redacted.value(first));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("hands the spawned server only the T3 password", () => {
    const password = Redacted.make("t3-generated");
    const environment = OpenCode2Server.serverEnvironment(
      { PATH: "/bin", OPENCODE_SERVER_PASSWORD: "ambient", OPENCODE_PASSWORD: "ambient" },
      password,
    );
    assert.deepStrictEqual(environment, { PATH: "/bin", OPENCODE_PASSWORD: "t3-generated" });
  });
});

// Serves /api/info only with the password from OPENCODE_PASSWORD, like 2.x, and
// prints the 2.x banner (plus the generated-password line 2.x prints when no
// password is set, which T3 must never need).
const FAKE_SERVER = `import { createServer } from "node:http";
const expected = "Basic " + Buffer.from("opencode:" + process.env.OPENCODE_PASSWORD).toString("base64");
const server = createServer((request, response) => {
  if (request.headers.authorization !== expected) {
    response.writeHead(401, { "content-type": "application/json", "www-authenticate": "Basic" });
    response.end(${JSON.stringify(UNAUTHORIZED_BODY)});
    return;
  }
  response.writeHead(200, { "content-type": "application/json" });
  response.end(JSON.stringify({ version: "2.0.18", pid: process.pid, urls: [], paths: { tmp: "/tmp" } }));
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write("server listening on http://127.0.0.1:" + server.address().port + "\\n");
  process.stdout.write("server password not-the-t3-password\\n");
});
`;

// Serves the endpoints `make` reads for a spawned server: /api/info, the
// /api/event subscription, and the /api/model catalog read. The catalog event
// goes out after a pause and is logged just before it does, so a log that
// already holds it when `withConnection` returns proves the lend waited for the
// event rather than returning on the read alone.
const CATALOG_SERVER = `import { createServer } from "node:http";
import { appendFileSync } from "node:fs";
const directory = process.env.T3_TEST_DIRECTORY;
const log = (line) => appendFileSync(process.env.T3_TEST_LOG, line + "\\n");
const expected = "Basic " + Buffer.from("opencode:" + process.env.OPENCODE_PASSWORD).toString("base64");
const server = createServer((request, response) => {
  if (request.headers.authorization !== expected) {
    response.writeHead(401, { "content-type": "application/json", "www-authenticate": "Basic" });
    response.end(${JSON.stringify(UNAUTHORIZED_BODY)});
    return;
  }
  const path = new URL(request.url, "http://127.0.0.1").pathname;
  if (path === "/api/info") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ version: "2.0.18", pid: process.pid, urls: [], paths: { tmp: "/tmp" } }));
    return;
  }
  if (path === "/api/event") {
    log("event-request");
    response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    response.flushHeaders();
    setTimeout(() => {
      log("event-sent:" + Date.now());
      response.write("data: " + JSON.stringify({
        id: "evt_catalog", created: 1, type: "model.updated", data: {}, location: { directory },
      }) + "\\n\\n");
    }, 250);
    return;
  }
  if (path === "/api/model") {
    log("model");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ location: { directory }, data: [] }));
    return;
  }
  response.writeHead(404);
  response.end();
});
server.listen(0, "127.0.0.1", () => {
  process.stdout.write("server listening on http://127.0.0.1:" + server.address().port + "\\n");
});
`;

const writeFakeBinary = (script: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const platform = yield* HostProcessPlatform;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-opencode2-fake-" });
    const isWindows = platform === "win32";
    const binaryPath = path.join(directory, isWindows ? "opencode.cmd" : "opencode");
    const scriptPath = path.join(directory, "opencode.mjs");
    yield* fs.writeFileString(scriptPath, script);
    yield* fs.writeFileString(
      binaryPath,
      isWindows
        ? `@echo off\r\n"${yield* HostProcessExecutablePath}" "${scriptPath}" %*\r\n`
        : `#!/bin/sh\nexec "${yield* HostProcessExecutablePath}" "${scriptPath}" "$@"\n`,
    );
    if (!isWindows) yield* fs.chmod(binaryPath, 0o755);
    return { directory, binaryPath };
  });

describe("OpenCode2Server spawned server", () => {
  it.live(
    "is ready once /api/info accepts the generated password",
    () =>
      Effect.gen(function* () {
        const { directory, binaryPath } = yield* writeFakeBinary(FAKE_SERVER);

        const server = yield* OpenCode2Server.make({
          binaryPath,
          serverUrl: "",
          serverPassword: "",
          directory,
          environment: { ...(yield* HostProcessEnvironment), OPENCODE_SERVER_PASSWORD: "ambient" },
        });
        const first = yield* server.withConnection((connection) => Effect.succeed(connection));
        const second = yield* server.withConnection((connection) => Effect.succeed(connection));
        assert.strictEqual(first.version, "2.0.18");
        assert.strictEqual(first.external, false);
        assert.strictEqual(second.client, first.client);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(
            OpenCode2Client.layer,
            OpenCodeRuntime.OpenCodeRuntimeLive.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
        ),
      ),
    15_000,
  );

  it.live(
    "is lent after the directory's catalog event",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { directory, binaryPath } = yield* writeFakeBinary(CATALOG_SERVER);
        const logPath = path.join(directory, "requests.log");

        const server = yield* OpenCode2Server.make({
          binaryPath,
          serverUrl: "",
          serverPassword: "",
          directory,
          environment: {
            ...(yield* HostProcessEnvironment),
            OPENCODE_SERVER_PASSWORD: "ambient",
            T3_TEST_DIRECTORY: directory,
            T3_TEST_LOG: logPath,
          },
        });
        const connection = yield* server.withConnection((value) => Effect.succeed(value));
        assert.strictEqual(connection.version, "2.0.18");
        assert.strictEqual(connection.external, false);
        const lines = (yield* fs.readFileString(logPath)).trim().split("\n");
        // Subscribed before the read, and still waiting when the event went out.
        assert.isTrue(lines.indexOf("event-request") < lines.indexOf("model"));
        const sentAt = lines
          .filter((line) => line.startsWith("event-sent:"))
          .map((line) => Number(line.slice("event-sent:".length)))[0];
        // The wait ended on the event, not on the five-second deadline.
        const returnedAt = yield* Clock.currentTimeMillis;
        assert.isTrue(sentAt !== undefined && returnedAt - sentAt < 2_000);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(
            OpenCode2Client.layer,
            OpenCodeRuntime.OpenCodeRuntimeLive.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer))),
        ),
      ),
    15_000,
  );
});
