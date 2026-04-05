#!/usr/bin/env node

import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import process from "node:process";

const BROWSER_TARGET_WINDOW_MS = 45_000;
const CHECKPOINT_BYTES = 16 * 1024 * 1024;
const SHARD_BYTES = 8 * 1024 * 1024;
const SHARD_COUNT = Number.parseInt(process.env.BURN_P2P_BROWSER_PROBE_SHARD_COUNT ?? "4", 10);
const ITERATIONS = Number.parseInt(process.env.BURN_P2P_BROWSER_PROBE_ITERATIONS ?? "5", 10);
const HEADLESS = process.env.BURN_P2P_PLAYWRIGHT_HEADED === "1" ? false : true;
const ARTIFACT_DIR = process.env.BURN_P2P_BROWSER_PROBE_ARTIFACT_DIR ?? null;

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
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

async function loadPlaywright() {
  try {
    return await import("playwright");
  } catch {
    const npxRoot = path.join(os.homedir(), ".npm", "_npx");
    if (!fs.existsSync(npxRoot)) {
      throw new Error(
        "playwright package not found; run `npx --yes playwright --version` once to populate the cache",
      );
    }

    const candidates = fs
      .readdirSync(npxRoot)
      .map((entry) => path.join(npxRoot, entry, "node_modules", "playwright", "index.mjs"))
      .filter((candidate) => fs.existsSync(candidate))
      .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs);

    if (candidates.length === 0) {
      throw new Error(
        "playwright package not found in npx cache; run `npx --yes playwright --version` once",
      );
    }

    return await import(pathToFileURL(candidates[0]).href);
  }
}

function payload(bytes) {
  return new Uint8Array(bytes);
}

async function startProbeServer() {
  const server = http.createServer((_req, res) => {
    res.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    res.end("<!doctype html><html><body>burn_p2p browser probe</body></html>");
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  return {
    server,
    probeUrl: `http://127.0.0.1:${address.port}/probe`,
  };
}

function quantile(samples, q) {
  const sorted = [...samples].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
  return sorted[index];
}

function summarize(samples) {
  const total = samples.reduce((sum, value) => sum + value, 0);
  return {
    min_ms: Number(Math.min(...samples).toFixed(3)),
    avg_ms: Number((total / samples.length).toFixed(3)),
    p95_ms: Number(quantile(samples, 0.95).toFixed(3)),
    max_ms: Number(Math.max(...samples).toFixed(3)),
  };
}

function summarizeTimed(samples, timedOutCount) {
  return {
    ...summarize(samples),
    timed_out_samples: timedOutCount,
  };
}

function recommendedRole({ dedicatedWorker, pageGpu, workerGpu }) {
  if (dedicatedWorker && pageGpu && workerGpu) {
    return "BrowserTrainerWgpu";
  }
  if (dedicatedWorker) {
    return "BrowserVerifier";
  }
  return "BrowserObserver";
}

async function measureBrowser(browserName, launcher, launchOptions, pageUrl) {
  const browser = await launcher.launch(launchOptions);
  try {
    const context = await browser.newContext();
    if (ARTIFACT_DIR) {
      ensureDir(ARTIFACT_DIR);
      await context.tracing.start({ screenshots: true, snapshots: true });
    }
    const page = await context.newPage();
    page.setDefaultTimeout(30_000);
    const consoleMessages = [];
    page.on("console", (message) => {
      consoleMessages.push({
        type: message.type(),
        text: message.text(),
      });
    });
    await page.goto(pageUrl);
    const result = await page.evaluate(
      async ({ checkpointBytes, shardBytes, shardCount, iterations, browserTargetWindowMs }) => {
        const timeoutMs = 2_000;

        function makeWorker() {
          const source = `
            self.onmessage = async (event) => {
              if (event.data.kind === "caps") {
                self.postMessage({
                  dedicatedWorker: true,
                  workerGpu: typeof navigator !== "undefined" && !!navigator.gpu,
                });
                return;
              }
              if (event.data.kind === "roundtrip") {
                const buffer = event.data.buffer;
                self.postMessage({ buffer, bytes: buffer.byteLength }, [buffer]);
              }
            };
          `;
          return new Worker(URL.createObjectURL(new Blob([source], { type: "text/javascript" })));
        }

        async function probeWorkerCaps(worker) {
          return new Promise((resolve) => {
            worker.onmessage = (event) => resolve(event.data);
            worker.postMessage({ kind: "caps" });
          });
        }

        async function withTimeout(promiseFactory, fallbackValue) {
          return new Promise((resolve) => {
            let finished = false;
            const timer = setTimeout(() => {
              if (!finished) {
                finished = true;
                resolve(fallbackValue);
              }
            }, timeoutMs);
            promiseFactory()
              .then((value) => {
                if (!finished) {
                  finished = true;
                  clearTimeout(timer);
                  resolve(value);
                }
              })
              .catch(() => {
                if (!finished) {
                  finished = true;
                  clearTimeout(timer);
                  resolve(fallbackValue);
                }
              });
          });
        }

        async function roundtrip(worker, bytes) {
          const start = performance.now();
          const buffer = new ArrayBuffer(bytes);
          const elapsed = await withTimeout(
            () =>
              new Promise((resolve) => {
                worker.onmessage = () => resolve(performance.now() - start);
                worker.postMessage({ kind: "roundtrip", buffer }, [buffer]);
              }),
            timeoutMs,
          );
          return {
            elapsed,
            timedOut: elapsed >= timeoutMs,
          };
        }

        async function transferSummary(bytes, iterations) {
          const worker = makeWorker();
          const caps = await probeWorkerCaps(worker);
          const samples = [];
          let timedOut = 0;
          for (let index = 0; index < iterations; index += 1) {
            const measurement = await roundtrip(worker, bytes);
            samples.push(measurement.elapsed);
            if (measurement.timedOut) {
              timedOut += 1;
            }
          }
          worker.terminate();
          return { caps, samples, timedOut };
        }

        async function hydrationSummary(bytes, iterations) {
          const samples = [];
          let timedOut = 0;
          for (let index = 0; index < iterations; index += 1) {
            const start = performance.now();
            const buffer = await withTimeout(
              () => new Blob([new Uint8Array(bytes)]).arrayBuffer(),
              null,
            );
            if (buffer === null) {
              timedOut += 1;
              samples.push(timeoutMs);
              continue;
            }
            if (buffer.byteLength !== bytes) {
              throw new Error(`expected ${bytes} bytes, got ${buffer.byteLength}`);
            }
            samples.push(performance.now() - start);
          }
          return { samples, timedOut };
        }

        async function hydrationBudget(checkpointBytes, shardBytes, shardCount) {
          const worker = makeWorker();
          const caps = await probeWorkerCaps(worker);
          const start = performance.now();
          const checkpoint = await withTimeout(
            () => new Blob([new Uint8Array(checkpointBytes)]).arrayBuffer(),
            null,
          );
          if (checkpoint === null) {
            worker.terminate();
            return { caps, totalMs: browserTargetWindowMs + 1, timedOut: true };
          }
          const checkpointTransfer = await withTimeout(
            () =>
              new Promise((resolve) => {
                worker.onmessage = () => resolve(performance.now() - start);
                worker.postMessage({ kind: "roundtrip", buffer: checkpoint }, [checkpoint]);
              }),
            null,
          );
          if (checkpointTransfer === null) {
            worker.terminate();
            return { caps, totalMs: browserTargetWindowMs + 1, timedOut: true };
          }
          for (let index = 0; index < shardCount; index += 1) {
            const shard = await withTimeout(
              () => new Blob([new Uint8Array(shardBytes)]).arrayBuffer(),
              null,
            );
            if (shard === null) {
              worker.terminate();
              return { caps, totalMs: browserTargetWindowMs + 1, timedOut: true };
            }
            const shardTransfer = await withTimeout(
              () =>
                new Promise((resolve) => {
                  worker.onmessage = () => resolve(performance.now() - start);
                  worker.postMessage({ kind: "roundtrip", buffer: shard }, [shard]);
                }),
              null,
            );
            if (shardTransfer === null) {
              worker.terminate();
              return { caps, totalMs: browserTargetWindowMs + 1, timedOut: true };
            }
          }
          worker.terminate();
          return {
            caps,
            totalMs: performance.now() - start,
            timedOut: false,
          };
        }

        const pageGpu = !!navigator.gpu;
        const checkpointTransfer = await transferSummary(checkpointBytes, iterations);
        const shardTransfer = await transferSummary(shardBytes, iterations);
        const checkpointHydration = await hydrationSummary(checkpointBytes, iterations);
        const shardHydration = await hydrationSummary(shardBytes, iterations);
        const hydration = await hydrationBudget(checkpointBytes, shardBytes, shardCount);

        return {
          userAgent: navigator.userAgent,
          hardwareConcurrency: navigator.hardwareConcurrency ?? null,
          deviceMemoryGiB: navigator.deviceMemory ?? null,
          pageGpu,
          workerGpu: checkpointTransfer.caps.workerGpu,
          dedicatedWorker: checkpointTransfer.caps.dedicatedWorker,
          checkpointTransferMs: checkpointTransfer.samples,
          checkpointTransferTimedOut: checkpointTransfer.timedOut,
          shardTransferMs: shardTransfer.samples,
          shardTransferTimedOut: shardTransfer.timedOut,
          checkpointHydrationMs: checkpointHydration.samples,
          checkpointHydrationTimedOut: checkpointHydration.timedOut,
          shardHydrationMs: shardHydration.samples,
          shardHydrationTimedOut: shardHydration.timedOut,
          hydrationBudgetMs: hydration.totalMs,
          hydrationBudgetTimedOut: hydration.timedOut,
          meetsBrowserTargetWindow: hydration.totalMs < browserTargetWindowMs,
        };
      },
      {
        checkpointBytes: CHECKPOINT_BYTES,
        shardBytes: SHARD_BYTES,
        shardCount: SHARD_COUNT,
        iterations: ITERATIONS,
        browserTargetWindowMs: BROWSER_TARGET_WINDOW_MS,
      },
    );

    const summary = {
      browser: browserName,
      user_agent: result.userAgent,
      hardware_concurrency: result.hardwareConcurrency,
      device_memory_gib: result.deviceMemoryGiB,
      page_gpu: result.pageGpu,
      worker_gpu: result.workerGpu,
      dedicated_worker: result.dedicatedWorker,
      recommended_role: recommendedRole({
        dedicatedWorker: result.dedicatedWorker,
        pageGpu: result.pageGpu,
        workerGpu: result.workerGpu,
      }),
      shard_transfer: summarizeTimed(result.shardTransferMs, result.shardTransferTimedOut),
      checkpoint_transfer: summarizeTimed(
        result.checkpointTransferMs,
        result.checkpointTransferTimedOut,
      ),
      shard_hydration: summarizeTimed(result.shardHydrationMs, result.shardHydrationTimedOut),
      checkpoint_hydration: summarizeTimed(
        result.checkpointHydrationMs,
        result.checkpointHydrationTimedOut,
      ),
      hydration_budget_ms: Number(result.hydrationBudgetMs.toFixed(3)),
      hydration_budget_timed_out: result.hydrationBudgetTimedOut,
      browser_target_window_ms: BROWSER_TARGET_WINDOW_MS,
      checkpoint_budget_bytes: CHECKPOINT_BYTES,
      shard_budget_bytes: SHARD_BYTES,
      shards_per_validation_sample: SHARD_COUNT,
      meets_browser_target_window: result.meetsBrowserTargetWindow,
    };
    if (ARTIFACT_DIR) {
      const browserDir = path.join(ARTIFACT_DIR, browserName);
      ensureDir(browserDir);
      await page.screenshot({ path: path.join(browserDir, "screenshot.png"), fullPage: true });
      await context.tracing.stop({ path: path.join(browserDir, "trace.zip") });
      fs.writeFileSync(
        path.join(browserDir, "console.json"),
        `${JSON.stringify(consoleMessages, null, 2)}\n`,
      );
      summary.artifacts = {
        screenshot: path.join(browserDir, "screenshot.png"),
        trace: path.join(browserDir, "trace.zip"),
        console: path.join(browserDir, "console.json"),
      };
    }
    await context.close();
    return summary;
  } finally {
    await browser.close();
  }
}

async function main() {
  const { chromium, firefox } = await loadPlaywright();
  const { server, probeUrl } = await startProbeServer();
  const chromeExecutable = resolveExecutable("BURN_P2P_CHROME_BIN", [
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  ]);
  const firefoxExecutable = resolveExecutable("BURN_P2P_FIREFOX_BIN", [
    "/usr/bin/firefox",
    "/Applications/Firefox.app/Contents/MacOS/firefox",
  ]);
  try {
    const results = [];
    const requestedBrowsers = new Set(
      (process.env.BURN_P2P_BROWSER_PROBE_BROWSERS ?? "chrome-wgpu,chrome-no-gpu")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean),
    );
    for (const descriptor of [
      {
        browser: "chrome-wgpu",
        launcher: chromium,
        options: {
          headless: HEADLESS,
          args: ["--enable-unsafe-webgpu", "--use-angle=swiftshader"],
        },
        pageUrl: probeUrl,
      },
      {
        browser: "chrome-no-gpu",
        launcher: chromium,
        options: {
          headless: HEADLESS,
          args: [],
        },
        pageUrl: "about:blank",
      },
      {
        browser: "firefox",
        launcher: firefox,
        options: {
          headless: HEADLESS,
        },
        pageUrl: probeUrl,
      },
    ]) {
      if (descriptor.browser.startsWith("chrome") && chromeExecutable) {
        descriptor.options.executablePath = chromeExecutable;
      }
      if (descriptor.browser === "firefox" && firefoxExecutable) {
        descriptor.options.executablePath = firefoxExecutable;
      }
      if (!requestedBrowsers.has(descriptor.browser)) {
        continue;
      }
      results.push(await measureBrowser(
        descriptor.browser,
        descriptor.launcher,
        descriptor.options,
        descriptor.pageUrl,
      ));
    }

    const output = {
      measured_at: new Date().toISOString(),
      host: process.env.HOSTNAME ?? null,
      command: "node crates/burn_p2p_testkit/scripts/browser_real_device_probe.mjs",
      results,
    };
    console.log(JSON.stringify(output, null, 2));
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
