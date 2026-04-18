#!/usr/bin/env node

import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

const HEADLESS = process.env.BURN_P2P_PLAYWRIGHT_HEADED === "1" ? false : true;

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePath(urlPath) {
  return urlPath.replace(/^\/+/, "");
}

function loadConfig() {
  const configPath = process.argv[2];
  if (!configPath) {
    throw new Error("usage: node mnist_browser_wasm_probe.mjs /abs/path/to/config.json");
  }
  return JSON.parse(fs.readFileSync(configPath, "utf8"));
}

async function loadPlaywright() {
  try {
    return await import("playwright");
  } catch {
    const npxRoot = path.join(os.homedir(), ".npm", "_npx");
    const candidates = fs
      .readdirSync(npxRoot)
      .map((entry) => path.join(npxRoot, entry, "node_modules", "playwright", "index.mjs"))
      .filter((candidate) => fs.existsSync(candidate))
      .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);
    if (candidates.length === 0) {
      throw new Error("playwright package not found");
    }
    return await import(pathToFileURL(candidates[0]).href);
  }
}

function resolveExecutable(envVar, candidates) {
  const explicit = process.env[envVar];
  if (explicit && fs.existsSync(explicit)) {
    return explicit;
  }
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function profileForPath(config, pathname) {
  const parts = pathname.split("/").filter(Boolean);
  if (parts[0] !== "profile") {
    return null;
  }
  const slug = parts[1];
  return config.profiles.find((profile) => profile.slug === slug) ?? null;
}

function comparableProfileTimeMs(profile, baselineProfile) {
  const fetchTimings = profile?.fetch_timings_ms ?? {};
  const fetchTotalMs =
    Number(fetchTimings.manifest ?? 0) +
    Number(fetchTimings.shards ?? 0) +
    Number(fetchTimings.eval ?? 0);
  const baselineFetchTimings = baselineProfile?.fetch_timings_ms ?? {};
  const baselineFetchTotalMs =
    Number(baselineFetchTimings.manifest ?? 0) +
    Number(baselineFetchTimings.shards ?? 0) +
    Number(baselineFetchTimings.eval ?? 0);
  const baselineHasLiveParticipant = !!baselineProfile?.live_participant;
  const profileHasLiveParticipant = !!profile?.live_participant;
  if (baselineHasLiveParticipant !== profileHasLiveParticipant) {
    return {
      profile: fetchTotalMs,
      baseline: baselineFetchTotalMs,
    };
  }
  return {
    profile: Number(profile?.total_page_time_ms ?? fetchTotalMs),
    baseline: Number(baselineProfile?.total_page_time_ms ?? baselineFetchTotalMs),
  };
}

function datasetPathForRequest(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  if (parts[0] !== "profile" || parts[2] !== "dataset") {
    return null;
  }
  return parts.slice(3).join("/");
}

async function writeProfiledBody(response, bytes, profile) {
  await sleep(profile.latency_ms);
  response.writeHead(200, {
    "content-type": bytes.path?.endsWith(".json") ? "application/json" : "application/octet-stream",
    "cache-control": "no-store",
    "content-length": bytes.data.length,
  });
  if (!profile.bandwidth_bytes_per_sec || profile.bandwidth_bytes_per_sec <= 0) {
    response.end(bytes.data);
    return;
  }

  const chunkSize = Math.max(16 * 1024, Math.floor(profile.bandwidth_bytes_per_sec / 8));
  const delayMs = Math.max(
    5,
    Math.round((chunkSize / profile.bandwidth_bytes_per_sec) * 1_000),
  );
  let offset = 0;
  while (offset < bytes.data.length) {
    response.write(bytes.data.subarray(offset, offset + chunkSize));
    offset += chunkSize;
    if (offset < bytes.data.length) {
      await sleep(delayMs);
    }
  }
  response.end();
}

async function startServer(config) {
  const requestLog = new Map(config.profiles.map((profile) => [profile.slug, []]));
  const peerDatasetRoot = config.peer_dataset_root || null;
  const peerHeadArtifactRoot = config.peer_head_artifact_root || null;
  const peerArtifactManifestRequests = [];
  const peerArtifactChunkRequests = [];
  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://127.0.0.1");
      const pathname = url.pathname;
      if (pathname.startsWith("/peer-swarm/manifest/")) {
        const artifactId = pathname.split("/").filter(Boolean)[2] ?? "";
        peerArtifactManifestRequests.push(artifactId);
        if (!peerHeadArtifactRoot) {
          response.writeHead(404);
          response.end("missing peer head artifact root");
          return;
        }
        const descriptorPath = path.join(
          peerHeadArtifactRoot,
          artifactId,
          "descriptor.json",
        );
        if (!fs.existsSync(descriptorPath)) {
          response.writeHead(404);
          response.end("missing peer artifact descriptor");
          return;
        }
        await writeProfiledBody(
          response,
          { path: "descriptor.json", data: fs.readFileSync(descriptorPath) },
          { latency_ms: 0, bandwidth_bytes_per_sec: 0 },
        );
        return;
      }
      if (pathname.startsWith("/peer-swarm/chunk/")) {
        const parts = pathname.split("/").filter(Boolean);
        const artifactId = parts[2] ?? "";
        const chunkId = parts[3] ?? "";
        peerArtifactChunkRequests.push({ artifact_id: artifactId, chunk_id: chunkId });
        if (!peerHeadArtifactRoot) {
          response.writeHead(404);
          response.end("missing peer head artifact root");
          return;
        }
        const chunkPath = path.join(
          peerHeadArtifactRoot,
          artifactId,
          "chunks",
          `${chunkId}.bin`,
        );
        if (!fs.existsSync(chunkPath)) {
          response.writeHead(404);
          response.end("missing peer artifact chunk");
          return;
        }
        await writeProfiledBody(
          response,
          { path: `${chunkId}.bin`, data: fs.readFileSync(chunkPath) },
          { latency_ms: 0, bandwidth_bytes_per_sec: 0 },
        );
        return;
      }
      const profile = profileForPath(config, pathname);
      if (profile) {
        const relativeDatasetPath = datasetPathForRequest(pathname);
        if (!relativeDatasetPath) {
          response.writeHead(404);
          response.end("missing dataset path");
          return;
        }
        requestLog.get(profile.slug).push(relativeDatasetPath);
        if (peerDatasetRoot) {
          const diskPath = path.join(peerDatasetRoot, relativeDatasetPath);
          if (!fs.existsSync(diskPath)) {
            response.writeHead(404);
            response.end("missing direct dataset file");
            return;
          }
          await writeProfiledBody(
            response,
            { path: relativeDatasetPath, data: fs.readFileSync(diskPath) },
            profile,
          );
        } else if (config.dataset_base_url) {
          const upstream = await fetch(
            `${config.dataset_base_url.replace(/\/+$/, "")}/${relativeDatasetPath}`,
          );
          if (!upstream.ok) {
            response.writeHead(upstream.status);
            response.end(`upstream dataset fetch failed: ${upstream.status}`);
            return;
          }
          await writeProfiledBody(
            response,
            {
              path: relativeDatasetPath,
              data: Buffer.from(await upstream.arrayBuffer()),
            },
            profile,
          );
        } else {
          const diskPath = path.join(config.dataset_root, relativeDatasetPath);
          if (!fs.existsSync(diskPath)) {
            response.writeHead(404);
            response.end("missing dataset file");
            return;
          }
          await writeProfiledBody(
            response,
            { path: relativeDatasetPath, data: fs.readFileSync(diskPath) },
            profile,
          );
        }
        return;
      }

      const relativeAssetPath = normalizePath(pathname || "index.html") || "index.html";
      const assetPath = path.join(
        config.asset_root,
        relativeAssetPath === "" ? "index.html" : relativeAssetPath,
      );
      if (!fs.existsSync(assetPath) || fs.statSync(assetPath).isDirectory()) {
        response.writeHead(404);
        response.end("missing asset");
        return;
      }
      response.writeHead(200, {
        "cache-control": "no-store",
        "content-type": assetPath.endsWith(".html")
          ? "text/html; charset=utf-8"
          : assetPath.endsWith(".js")
            ? "text/javascript; charset=utf-8"
            : assetPath.endsWith(".wasm")
              ? "application/wasm"
              : "application/octet-stream",
      });
      response.end(fs.readFileSync(assetPath));
    } catch (error) {
      response.writeHead(500);
      response.end(String(error));
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  return {
    peerArtifactChunkRequests,
    peerArtifactManifestRequests,
    requestLog,
    origin: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

async function runProfile(page, profile, config, requestLog, origin) {
  const datasetBaseUrl = `${origin}/profile/${profile.slug}/dataset`;
  const liveParticipant =
    profile.slug === "fast" && config.edge_base_url
      ? {
          edge_base_url: config.edge_base_url,
          network_id: config.network_id,
          selected_head_id: config.selected_head_id,
          release_train_hash: config.release_train_hash,
          target_artifact_id: config.target_artifact_id,
          target_artifact_hash: config.target_artifact_hash,
          principal_id: config.principal_id,
          training_plan: {
            study_id: "mnist-study",
            experiment_id: config.experiment_id,
            revision_id: config.revision_id,
            workload_id: config.workload_id,
            budget: {
              max_window_secs: 12,
              max_checkpoint_bytes: 16777216,
              max_shard_bytes: 8388608,
              requires_webgpu: true,
              max_batch_size: 4,
              precision: "Fp16",
            },
            lease: null,
          },
        }
      : null;
  const result = await Promise.race([
    page.evaluate(async (probeConfig) => {
      const startedAt = performance.now();
      const manifestStartedAt = performance.now();
      const manifest = await fetch(`${probeConfig.datasetBaseUrl}/fetch-manifest.json`).then((response) => {
        if (!response.ok) {
          throw new Error(`manifest fetch failed: ${response.status}`);
        }
        return response.json();
      });
      const manifestFetchTimeMs = performance.now() - manifestStartedAt;
      const requestedEntries = manifest.entries.filter((entry) =>
        probeConfig.leasedMicroshardIds.includes(entry.microshard_id),
      );
      const trainRecords = [];
      const requestedPaths = [];
      const shardStartedAt = performance.now();
      for (const entry of requestedEntries) {
        requestedPaths.push(entry.locator);
        const records = await fetch(`${probeConfig.datasetBaseUrl}/${entry.locator}`).then((response) => {
          if (!response.ok) {
            throw new Error(`shard fetch failed: ${response.status}`);
          }
          return response.json();
        });
        trainRecords.push(...records);
      }
      const shardFetchTimeMs = performance.now() - shardStartedAt;
      const evalStartedAt = performance.now();
      const evalRecords = await fetch(`${probeConfig.datasetBaseUrl}/eval-records.json`).then((response) => {
        if (!response.ok) {
          throw new Error(`eval fetch failed: ${response.status}`);
        }
        return response.json();
      });
      const evalFetchTimeMs = performance.now() - evalStartedAt;
      const wasmResult = await window.runBrowserMnistProbe({
        train_records: trainRecords,
        eval_records: evalRecords,
        batch_size: probeConfig.batchSize,
        learning_rate: probeConfig.learningRate,
        max_train_batches: probeConfig.maxTrainBatches,
        live_participant: probeConfig.liveParticipant,
      });
      return {
        manifestFetchTimeMs,
        shardFetchTimeMs,
        evalFetchTimeMs,
        requestedPaths,
        trainRecordCount: trainRecords.length,
        evalRecordCount: evalRecords.length,
        wasmResult,
        totalPageTimeMs: performance.now() - startedAt,
      };
    }, {
      datasetBaseUrl,
      leasedMicroshardIds: config.leased_microshards,
      batchSize: config.batch_size,
      learningRate: config.learning_rate,
      maxTrainBatches: config.max_train_batches,
      liveParticipant,
    }),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`browser mnist profile ${profile.slug} timed out`)), 45_000),
    ),
  ]);
  const loggedPaths = requestLog.get(profile.slug) ?? [];
  const shardRequests = loggedPaths.filter(
    (entry) => entry !== "fetch-manifest.json" && entry !== "eval-records.json",
  );
  return {
    slug: profile.slug,
    latency_ms: profile.latency_ms,
    bandwidth_bytes_per_sec: profile.bandwidth_bytes_per_sec,
    dataset_base_url: datasetBaseUrl,
    fetch_manifest_requested: loggedPaths.includes("fetch-manifest.json"),
    requested_paths: loggedPaths,
    fetched_only_leased_shards:
      JSON.stringify(shardRequests) === JSON.stringify(result.requestedPaths),
    fetch_timings_ms: {
      manifest: Number(result.manifestFetchTimeMs.toFixed(3)),
      shards: Number(result.shardFetchTimeMs.toFixed(3)),
      eval: Number(result.evalFetchTimeMs.toFixed(3)),
    },
    train_record_count: result.trainRecordCount,
    eval_record_count: result.evalRecordCount,
    wasm: result.wasmResult,
    live_participant: result.wasmResult.live_participant ?? null,
    total_page_time_ms: Number(result.totalPageTimeMs.toFixed(3)),
  };
}

async function main() {
  const config = loadConfig();
  const { chromium } = await loadPlaywright();
  const chromeExecutable = resolveExecutable("BURN_P2P_CHROME_BIN", [
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ]);
  const server = await startServer(config);
  const consoleMessages = [];
  const browser = await chromium.launch({
    headless: HEADLESS,
    executablePath: chromeExecutable ?? undefined,
    args: ["--enable-unsafe-webgpu", "--use-angle=swiftshader"],
  });
  let context = null;
  let page = null;
  try {
    context = await browser.newContext();
    if (config.artifact_root) {
      ensureDir(config.artifact_root);
      await context.tracing.start({ screenshots: true, snapshots: true });
    }
    page = await context.newPage();
    await page.addInitScript(({ peerSwarmBaseUrl }) => {
      window.__burnP2PArtifactSwarm = {
        async fetchArtifactManifest(request) {
          if (!peerSwarmBaseUrl || !request?.artifact_id) {
            return null;
          }
          const response = await fetch(
            `${peerSwarmBaseUrl.replace(/\/+$/, "")}/manifest/${encodeURIComponent(request.artifact_id)}`,
            { cache: "no-store" },
          );
          if (response.status === 404) {
            return null;
          }
          if (!response.ok) {
            throw new Error(`peer artifact manifest fetch failed: ${response.status}`);
          }
          return await response.json();
        },
        async fetchArtifactChunk(request) {
          if (!peerSwarmBaseUrl || !request?.artifact?.artifact_id || !request?.chunk_id) {
            return null;
          }
          const response = await fetch(
            `${peerSwarmBaseUrl.replace(/\/+$/, "")}/chunk/${encodeURIComponent(request.artifact.artifact_id)}/${encodeURIComponent(request.chunk_id)}`,
            { cache: "no-store" },
          );
          if (response.status === 404) {
            return null;
          }
          if (!response.ok) {
            throw new Error(`peer artifact chunk fetch failed: ${response.status}`);
          }
          return new Uint8Array(await response.arrayBuffer());
        },
      };
    }, {
      peerSwarmBaseUrl: config.peer_head_artifact_root
        ? `${server.origin}/peer-swarm`
        : null,
    });
    page.on("console", (message) => {
      consoleMessages.push({ type: message.type(), text: message.text() });
    });
    page.on("pageerror", (error) => {
      consoleMessages.push({ type: "pageerror", text: String(error?.message ?? error) });
    });
    page.on("requestfailed", (request) => {
      consoleMessages.push({
        type: "requestfailed",
        text: `${request.method()} ${request.url()} ${request.failure()?.errorText ?? "failed"}`,
      });
    });
    await page.goto(`${server.origin}/index.html`);
    await page.waitForFunction(() => typeof window.runBrowserMnistProbe === "function", {
      timeout: 15_000,
    });

    const browserRuntime = await page.evaluate(async () => {
      const hasGpu = !!navigator.gpu;
      let adapterAvailable = false;
      let adapterError = null;
      try {
        const adapter = hasGpu
          ? await Promise.race([
              navigator.gpu.requestAdapter(),
              new Promise((_, reject) =>
                setTimeout(() => reject(new Error("adapter-timeout")), 5_000),
              ),
            ])
          : null;
        adapterAvailable = !!adapter;
      } catch (error) {
        adapterError = String(error?.message ?? error);
      }
      return {
        userAgent: navigator.userAgent,
        hasGpu,
        adapterAvailable,
        adapterError,
      };
    });

    const profiles = [];
    for (const profile of config.profiles) {
      profiles.push(await runProfile(page, profile, config, server.requestLog, server.origin));
    }

    const liveParticipantSummary =
      profiles.find((profile) => profile.live_participant)?.live_participant ?? null;
    const baselineProfile = profiles[0] ?? null;
    const slowestProfile = profiles[profiles.length - 1] ?? null;
    const comparableTimes = comparableProfileTimeMs(slowestProfile, baselineProfile);
    const summary = {
      measured_at: new Date().toISOString(),
      origin: server.origin,
      run_context: {
        network_id: config.network_id ?? null,
        experiment_id: config.experiment_id ?? null,
        revision_id: config.revision_id ?? null,
        selected_head_id: config.selected_head_id ?? null,
        lease_id: config.lease_id ?? null,
      },
      leased_microshards: config.leased_microshards,
      browser_runtime: browserRuntime,
      profiles,
      browser_dataset_access: {
        upstream_mode:
          config.browser_dataset_transport ??
          (config.dataset_base_url ? "p2p-artifact-via-edge" : "http"),
        browser_http_base_url: profiles[0]?.dataset_base_url ?? "",
        fetch_manifest_requested: profiles.every((profile) => profile.fetch_manifest_requested),
        leased_microshards: config.leased_microshards,
        requested_paths: profiles[0]?.requested_paths ?? [],
        fetched_only_leased_shards: profiles.every(
          (profile) => profile.fetched_only_leased_shards,
        ),
        shards_distributed_over_p2p: !!config.shards_distributed_over_p2p,
        notes:
          config.browser_dataset_transport === "p2p-signed-peer-bundle"
            ? [
                "browser shard requests were served from a lease-scoped peer bundle materialized from the live p2p artifact",
                "the live browser edge remained in the control/auth path, but dataset bytes were not fetched back through the edge route",
              ]
            : config.shards_distributed_over_p2p
          ? [
              "browser shard requests were proxied through the local latency harness into the live browser edge dataset route",
              "the live browser edge served a bundle that had already been synced from a native peer over the artifact control plane",
            ]
          : [
              "browser shard requests used the prepared dataset http origin",
              "shard transport was not exercised over the peer overlay",
            ],
      },
      browser_execution: {
        live_browser_training:
          profiles.every((profile) => profile.wasm?.backend === "burn-webgpu-wasm") &&
          !!liveParticipantSummary?.receipt_submission_accepted,
        browser_latency_emulated: profiles.some((profile) => profile.latency_ms > 0),
        slower_profile_increased_total_time:
          profiles.length < 2
            ? true
            : comparableTimes.profile > comparableTimes.baseline,
        session_enrolled: !!liveParticipantSummary?.session_enrolled,
        receipt_submission_accepted:
          !!liveParticipantSummary?.receipt_submission_accepted,
        runtime_state: liveParticipantSummary?.runtime_state ?? null,
        transport: liveParticipantSummary?.transport ?? null,
        head_artifact_transport: liveParticipantSummary?.head_artifact_transport ?? null,
        active_assignment: !!liveParticipantSummary?.active_assignment,
        emitted_receipt_id: liveParticipantSummary?.emitted_receipt_id ?? null,
        accepted_receipt_ids: liveParticipantSummary?.accepted_receipt_ids ?? [],
      },
      browser_artifact_access: {
        upstream_mode:
          liveParticipantSummary?.head_artifact_transport ??
          config.browser_head_artifact_transport ??
          "unknown",
        requested_artifact_ids: [...new Set(server.peerArtifactManifestRequests)],
        requested_chunk_ids: server.peerArtifactChunkRequests.map(
          (request) => request.chunk_id,
        ),
        peer_swarm_enabled: !!config.peer_head_artifact_root,
      },
    };

    if (config.artifact_root) {
      const browserDir = path.join(config.artifact_root, "browser-wasm");
      ensureDir(browserDir);
      await page.screenshot({ path: path.join(browserDir, "screenshot.png"), fullPage: true });
      await context.tracing.stop({ path: path.join(browserDir, "trace.zip") });
      fs.writeFileSync(
        path.join(browserDir, "console.json"),
        `${JSON.stringify(consoleMessages, null, 2)}\n`,
      );
      fs.writeFileSync(
        path.join(browserDir, "summary.json"),
        `${JSON.stringify(summary, null, 2)}\n`,
      );
    }
    console.log(JSON.stringify(summary, null, 2));
  } catch (error) {
    if (config.artifact_root) {
      const browserDir = path.join(config.artifact_root, "browser-wasm");
      ensureDir(browserDir);
      if (page) {
        try {
          await page.screenshot({ path: path.join(browserDir, "failure.png"), fullPage: true });
        } catch {}
      }
      if (context) {
        try {
          await context.tracing.stop({ path: path.join(browserDir, "trace.zip") });
        } catch {}
      }
      fs.writeFileSync(
        path.join(browserDir, "console.json"),
        `${JSON.stringify(consoleMessages, null, 2)}\n`,
      );
      fs.writeFileSync(path.join(browserDir, "error.txt"), `${String(error?.stack ?? error)}\n`);
    }
    throw error;
  } finally {
    await browser.close();
    await server.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
