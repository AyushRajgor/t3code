import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import * as Cache from "effect/Cache";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as ModelManifest from "./ModelManifest.ts";
import type { ProviderInstance } from "./ProviderDriver.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "./providerMaintenance.ts";

const claudeDriver = ProviderDriverKind.make("claudeAgent");

const makeCachedInstance = Effect.fn(function* (id: string, email: string, usedPercent: number) {
  const instanceId = ProviderInstanceId.make(id);
  const initialProvider = {
    instanceId,
    driver: claudeDriver,
    status: "ready",
    enabled: true,
    installed: true,
    auth: { status: "authenticated", email },
    checkedAt: "2026-10-07T00:00:00.000Z",
    version: "2.1.292",
    models: [],
    slashCommands: [],
    skills: [],
    usageLimits: {
      checkedAt: "2026-10-07T00:00:00.000Z",
      windows: [{ id: "five_hour", kind: "session", label: "Session", usedPercent }],
    },
  } satisfies ServerProvider;
  const reportedProvider = yield* Ref.make<ServerProvider>(initialProvider);
  const publishedProvider = yield* Ref.make<ServerProvider>(initialProvider);
  const probeCount = yield* Ref.make(0);
  const cache = yield* Cache.make({
    capacity: 1,
    timeToLive: "5 minutes",
    lookup: (_key: string) =>
      Ref.update(probeCount, (count) => count + 1).pipe(Effect.andThen(Ref.get(reportedProvider))),
  });
  const refresh = Cache.get(cache, "capabilities").pipe(
    Effect.tap((provider) => Ref.set(publishedProvider, provider)),
  );
  // Prime the same five-minute cache that subsequent status probes read.
  yield* refresh;
  const instance = {
    instanceId,
    driverKind: claudeDriver,
    continuationIdentity: {
      driverKind: claudeDriver,
      continuationKey: `${claudeDriver}:instance:${instanceId}`,
    },
    displayName: undefined,
    enabled: true,
    invalidateCaches: Cache.invalidateAll(cache),
    snapshot: {
      resolveMaintenance: () =>
        Effect.succeed(
          makeManualOnlyProviderMaintenanceCapabilities({
            provider: claudeDriver,
            packageName: null,
          }),
        ),
      getSnapshot: Ref.get(publishedProvider),
      refresh,
      streamChanges: Stream.empty,
      applyUsageLimits: () => Effect.void,
    },
    orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
    textGeneration: {} as ProviderInstance["textGeneration"],
  } satisfies ProviderInstance;
  return {
    instance,
    probeCount: Ref.get(probeCount),
    reportAccount: (nextEmail: string, nextUsedPercent: number) =>
      Ref.set(reportedProvider, {
        ...initialProvider,
        auth: { status: "authenticated", email: nextEmail },
        usageLimits: {
          ...initialProvider.usageLimits,
          windows: [{ ...initialProvider.usageLimits.windows[0]!, usedPercent: nextUsedPercent }],
        },
      }),
  };
});

const buildRegistry = Effect.fn(function* (instances: ReadonlyArray<ProviderInstance>) {
  const changes = yield* PubSub.unbounded<void>();
  const layerInstanceRegistry = Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
    getInstance: (instanceId) =>
      Effect.succeed(instances.find((instance) => instance.instanceId === instanceId)),
    listInstances: Effect.succeed(instances),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.fromPubSub(changes),
    subscribeChanges: PubSub.subscribe(changes),
  });
  const context = yield* Layer.build(
    ProviderRegistry.layer.pipe(
      Layer.provide(layerInstanceRegistry),
      Layer.provide(ModelManifest.layerTest),
      Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-provider-fresh-status-" })),
      Layer.provide(NodeServices.layer),
    ),
  );
  return yield* ProviderRegistry.ProviderRegistry.pipe(Effect.provide(context));
});

const accountUsage = (providers: ReadonlyArray<ServerProvider>) =>
  Object.fromEntries(
    providers.map((provider) => [
      provider.instanceId,
      {
        email: provider.auth.email,
        usedPercent: provider.usageLimits?.windows[0]?.usedPercent,
      },
    ]),
  );

describe("ProviderRegistry fresh status", () => {
  it.effect("retains cached account and quota probes until their five-minute expiry", () =>
    Effect.gen(function* () {
      const claude = yield* makeCachedInstance("claudeAgent", "personal@example.test", 10);
      const registry = yield* buildRegistry([claude.instance]);
      yield* claude.reportAccount("changed@example.test", 60);

      yield* TestClock.adjust("299 seconds");
      assert.deepStrictEqual(accountUsage(yield* registry.refresh()), {
        claudeAgent: { email: "personal@example.test", usedPercent: 10 },
      });
      assert.strictEqual(yield* claude.probeCount, 1);

      yield* TestClock.adjust("2 seconds");
      assert.deepStrictEqual(accountUsage(yield* registry.refresh()), {
        claudeAgent: { email: "changed@example.test", usedPercent: 60 },
      });
      assert.strictEqual(yield* claude.probeCount, 2);
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes the selected account immediately and leaves other instances cached", () =>
    Effect.gen(function* () {
      const personal = yield* makeCachedInstance("claudeAgent", "personal@example.test", 10);
      const work = yield* makeCachedInstance("claude_work", "work@example.test", 20);
      const registry = yield* buildRegistry([personal.instance, work.instance]);
      yield* personal.reportAccount("personal-new@example.test", 30);
      yield* work.reportAccount("work-new@example.test", 40);

      const fresh = yield* registry.refreshInstance(work.instance.instanceId, { fresh: true });
      assert.deepStrictEqual(accountUsage(fresh), {
        claudeAgent: { email: "personal@example.test", usedPercent: 10 },
        claude_work: { email: "work-new@example.test", usedPercent: 40 },
      });
      assert.strictEqual(yield* personal.probeCount, 1);
      assert.strictEqual(yield* work.probeCount, 2);

      yield* work.reportAccount("work-later@example.test", 50);
      const backgroundResult = yield* work.instance.snapshot.refresh;
      assert.strictEqual(backgroundResult.auth.email, "work-new@example.test");
      assert.strictEqual(backgroundResult.usageLimits?.windows[0]?.usedPercent, 40);
      assert.deepStrictEqual(accountUsage(yield* registry.refresh()), accountUsage(fresh));
      assert.strictEqual(yield* personal.probeCount, 1);
      assert.strictEqual(yield* work.probeCount, 2);

      assert.deepStrictEqual(accountUsage(yield* registry.refresh(undefined, { fresh: true })), {
        claudeAgent: { email: "personal-new@example.test", usedPercent: 30 },
        claude_work: { email: "work-later@example.test", usedPercent: 50 },
      });
      assert.strictEqual(yield* personal.probeCount, 2);
      assert.strictEqual(yield* work.probeCount, 3);
    }).pipe(Effect.scoped),
  );

  it.effect("fresh kind-scoped refresh only probes the default instance of that driver", () =>
    Effect.gen(function* () {
      const personal = yield* makeCachedInstance("claudeAgent", "personal@example.test", 10);
      const work = yield* makeCachedInstance("claude_work", "work@example.test", 20);
      const registry = yield* buildRegistry([personal.instance, work.instance]);
      yield* personal.reportAccount("personal-new@example.test", 30);
      yield* work.reportAccount("work-new@example.test", 40);

      assert.deepStrictEqual(accountUsage(yield* registry.refresh(claudeDriver, { fresh: true })), {
        claudeAgent: { email: "personal-new@example.test", usedPercent: 30 },
        claude_work: { email: "work@example.test", usedPercent: 20 },
      });
      assert.strictEqual(yield* personal.probeCount, 2);
      assert.strictEqual(yield* work.probeCount, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("fresh status after catalog invalidation probes each selected instance once", () =>
    Effect.gen(function* () {
      const personal = yield* makeCachedInstance("claudeAgent", "personal@example.test", 10);
      const work = yield* makeCachedInstance("claude_work", "work@example.test", 20);
      const registry = yield* buildRegistry([personal.instance, work.instance]);
      yield* personal.reportAccount("personal-new@example.test", 30);
      yield* work.reportAccount("work-new@example.test", 40);

      // Model refresh prepares the catalog and invalidates before reading status.
      yield* work.instance.invalidateCaches!;
      const targetedInput = {
        instanceId: work.instance.instanceId,
        fresh: true,
        refreshModels: true,
      };
      assert.deepStrictEqual(
        accountUsage(yield* registry.refreshInstance(targetedInput.instanceId, targetedInput)),
        {
          claudeAgent: { email: "personal@example.test", usedPercent: 10 },
          claude_work: { email: "work-new@example.test", usedPercent: 40 },
        },
      );
      assert.strictEqual(yield* personal.probeCount, 1);
      assert.strictEqual(yield* work.probeCount, 2);

      yield* work.reportAccount("work-later@example.test", 50);
      yield* personal.instance.invalidateCaches!;
      yield* work.instance.invalidateCaches!;
      const allInput = { fresh: true, refreshModels: true };
      assert.deepStrictEqual(accountUsage(yield* registry.refresh(undefined, allInput)), {
        claudeAgent: { email: "personal-new@example.test", usedPercent: 30 },
        claude_work: { email: "work-later@example.test", usedPercent: 50 },
      });
      assert.strictEqual(yield* personal.probeCount, 2);
      assert.strictEqual(yield* work.probeCount, 3);
    }).pipe(Effect.scoped),
  );
});
