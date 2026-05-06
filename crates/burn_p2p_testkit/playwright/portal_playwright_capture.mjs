#!/usr/bin/env node

import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

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

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, payload) {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`);
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  if (filePath.endsWith(".js")) return "text/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".wasm")) return "application/wasm";
  if (filePath.endsWith(".png")) return "image/png";
  return "application/octet-stream";
}

function resolveChromeExecutable() {
  const explicit = process.env.BURN_P2P_PLAYWRIGHT_CHROME;
  if (explicit && fs.existsSync(explicit)) {
    return explicit;
  }
  for (const candidate of ["/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"]) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function slugFromPathname(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  if (parts[0] === "scenarios") {
    return parts[1] ?? null;
  }
  return parts[0] ?? null;
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function bodyContains(page, text) {
  return page.waitForFunction(
    (needle) => document.body && document.body.innerText.includes(needle),
    text,
    { timeout: 10_000 },
  );
}

async function readRequestBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function sendJson(res, statusCode, payload) {
  const body = `${JSON.stringify(payload, null, 2)}\n`;
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  res.end(body);
}

function sendText(res, statusCode, body, contentTypeValue = "text/plain; charset=utf-8") {
  res.writeHead(statusCode, {
    "content-type": contentTypeValue,
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  res.end(body);
}

function sendFile(res, filePath) {
  const stat = fs.statSync(filePath);
  res.writeHead(200, {
    "content-type": contentType(filePath),
    "content-length": stat.size,
    "cache-control": "no-store",
  });
  fs.createReadStream(filePath).pipe(res);
}

async function startScenarioServer(bundleRoot, manifest) {
  const scenarioMap = new Map();
  for (const scenario of manifest.scenarios) {
    const scenarioDir = path.join(bundleRoot, path.dirname(scenario.html_path));
    scenarioMap.set(scenario.slug, {
      scenario,
      scenarioDir,
      snapshot: readJson(path.join(bundleRoot, scenario.snapshot_path)),
      directory: readJson(path.join(scenarioDir, "directory.json")),
      heads: readJson(path.join(scenarioDir, "heads.json")),
      signedDirectory: readJson(path.join(scenarioDir, "signed-directory.json")),
      signedLeaderboard: readJson(path.join(scenarioDir, "signed-leaderboard.json")),
      metricsCatchup: readJson(path.join(scenarioDir, "metrics-catchup.json")),
    });
  }

  const server = http.createServer(async (req, res) => {
    try {
      const method = req.method ?? "GET";
      const url = new URL(req.url ?? "/", "http://127.0.0.1");
      const pathname = decodeURIComponent(url.pathname);
      const slug = slugFromPathname(pathname);
      const scenarioEntry = slug ? scenarioMap.get(slug) : null;

      if (method === "GET" && pathname === "/favicon.ico") {
        res.writeHead(204, { "cache-control": "no-store" });
        res.end();
        return;
      }

      if (scenarioEntry && method === "POST" && pathname === `/${slug}/artifacts/export`) {
        const payload = JSON.parse((await readRequestBody(req)) || "{}");
        sendJson(res, 200, {
          export_job_id: `export-${slug}-${payload.head_id ?? "artifact"}`,
          status: "Queued",
          accepted_at: new Date().toISOString(),
        });
        return;
      }

      if (scenarioEntry && method === "POST" && pathname === `/${slug}/artifacts/download-ticket`) {
        const payload = JSON.parse((await readRequestBody(req)) || "{}");
        const headId = payload.head_id ?? "artifact";
        sendJson(res, 200, {
          download_path: `/${slug}/downloads/${headId}.bin`,
          head_id: headId,
        });
        return;
      }

      if (scenarioEntry && method === "GET" && pathname.startsWith(`/${slug}/downloads/`)) {
        const filename = pathname.split("/").pop() ?? "artifact.bin";
        const bytes = Buffer.from(`burn_p2p capture payload for ${slug}/${filename}\n`, "utf8");
        res.writeHead(200, {
          "content-type": "application/octet-stream",
          "content-length": bytes.length,
          "content-disposition": `attachment; filename="${filename}"`,
          "cache-control": "no-store",
        });
        res.end(bytes);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/portal/snapshot`) {
        sendJson(res, 200, scenarioEntry.snapshot);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/directory`) {
        sendJson(res, 200, scenarioEntry.directory);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/heads`) {
        sendJson(res, 200, scenarioEntry.heads);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/directory/signed`) {
        sendJson(res, 200, scenarioEntry.signedDirectory);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/leaderboard/signed`) {
        sendJson(res, 200, scenarioEntry.signedLeaderboard);
        return;
      }

      if (
        scenarioEntry &&
        method === "GET" &&
        (pathname === `/${slug}/metrics/catchup` ||
          pathname.startsWith(`/${slug}/metrics/catchup/`))
      ) {
        sendJson(res, 200, scenarioEntry.metricsCatchup);
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/metrics/live/latest`) {
        const bundle = scenarioEntry.metricsCatchup[0] ?? null;
        sendJson(res, 200, {
          network_id: scenarioEntry.snapshot.network_id,
          kind: "CatchupRefresh",
          cursors: bundle
            ? [
                {
                  experiment_id: bundle.experiment_id,
                  revision_id: bundle.revision_id,
                  latest_snapshot_seq: bundle.snapshot.manifest.snapshot_seq ?? null,
                  latest_ledger_segment_seq: bundle.ledger_segments.at(-1)?.segment_seq ?? null,
                  latest_head_id: bundle.snapshot.manifest.covers_until_head_id ?? null,
                  latest_merge_window_id:
                    bundle.snapshot.manifest.covers_until_merge_window_id ?? null,
                },
              ]
            : [],
          generated_at: scenarioEntry.snapshot.captured_at,
        });
        return;
      }

      if (scenarioEntry && method === "GET" && pathname === `/${slug}/trust`) {
        sendJson(res, 200, scenarioEntry.snapshot.trust);
        return;
      }

      const safeRelative = pathname.replace(/^\/+/, "");
      const staticPath = path.join(bundleRoot, safeRelative);
      if (
        safeRelative &&
        fs.existsSync(staticPath) &&
        fs.statSync(staticPath).isFile() &&
        staticPath.startsWith(bundleRoot)
      ) {
        sendFile(res, staticPath);
        return;
      }

      sendText(res, 404, `not found: ${pathname}`);
    } catch (error) {
      sendText(res, 500, `${error.stack ?? error}`);
    }
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  return {
    server,
    baseUrl: `http://127.0.0.1:${address.port}`,
  };
}

async function runInteraction(page, interaction, scenarioDir) {
  const selector = interaction.selector ?? null;
  switch (interaction.action) {
    case "goto_anchor":
      if (selector) {
        await page.locator(selector).scrollIntoViewIfNeeded();
        await wait(250);
      }
      break;
    case "filter":
      if (!selector) {
        throw new Error("filter interaction requires selector");
      }
      await page.locator(selector).fill(interaction.value ?? "");
      break;
    case "click":
      if (!selector) {
        throw new Error("click interaction requires selector");
      }
      await page.locator(selector).first().click();
      break;
    case "download":
      if (!selector) {
        throw new Error("download interaction requires selector");
      }
      {
        const downloadPromise = page.waitForEvent("download", { timeout: 10_000 }).catch(() => null);
        await page.locator(selector).first().click();
        const download = await downloadPromise;
        if (download) {
          await download.saveAs(path.join(scenarioDir, "download.bin"));
        }
      }
      break;
    default:
      throw new Error(`unsupported interaction action: ${interaction.action}`);
  }

  if (interaction.wait_for_text) {
    await bodyContains(page, interaction.wait_for_text);
  } else {
    await wait(250);
  }
}

async function captureScenario(browserType, baseUrl, bundleRoot, scenario) {
  const scenarioDir = path.join(bundleRoot, path.dirname(scenario.html_path));
  ensureDir(scenarioDir);
  const viewport = scenario.viewport ?? { width: 1440, height: 1280 };

  const context = await browserType.newContext({
    acceptDownloads: true,
    viewport,
  });
  await context.tracing.start({ screenshots: true, snapshots: true });

  const page = await context.newPage();
  const consoleMessages = [];
  const networkEvents = [];

  page.on("console", (message) => {
    consoleMessages.push({
      type: message.type(),
      text: message.text(),
    });
  });

  page.on("requestfinished", async (request) => {
    const response = await request.response();
    networkEvents.push({
      type: "finished",
      method: request.method(),
      url: request.url(),
      status: response?.status() ?? null,
      resource_type: request.resourceType(),
    });
  });

  page.on("requestfailed", (request) => {
    networkEvents.push({
      type: "failed",
      method: request.method(),
      url: request.url(),
      status: null,
      resource_type: request.resourceType(),
      failure_text: request.failure()?.errorText ?? "unknown",
    });
  });

  const scenarioUrl = `${baseUrl}/${scenario.html_path}`;
  await page.goto(scenarioUrl, { waitUntil: "load" });
  await page.waitForLoadState("networkidle").catch(() => {});
  for (const interaction of scenario.interactions) {
    await runInteraction(page, interaction, scenarioDir);
  }

  const screenshotPath = path.join(scenarioDir, "screenshot.png");
  await page.screenshot({ path: screenshotPath, fullPage: false });
  const tracePath = path.join(scenarioDir, "trace.zip");
  await context.tracing.stop({ path: tracePath });

  const scenarioStatePath = path.join(bundleRoot, scenario.snapshot_path);
  const statePayload = readJson(scenarioStatePath);
  const stateOutputPath = path.join(scenarioDir, "state.json");
  writeJson(stateOutputPath, statePayload);

  const consolePath = path.join(scenarioDir, "console.json");
  const networkPath = path.join(scenarioDir, "network.json");
  writeJson(consolePath, consoleMessages);
  writeJson(networkPath, networkEvents);

  const summary = {
    slug: scenario.slug,
    title: scenario.title,
    description: scenario.description,
    page_url: scenarioUrl,
    peer_count: scenario.peer_count,
    experiment_count: scenario.experiment_count,
    runtime_states: scenario.runtime_states,
    estimated_network_size: scenario.estimated_network_size,
    viewport,
    output_files: {
      screenshot: path.relative(bundleRoot, screenshotPath),
      trace: path.relative(bundleRoot, tracePath),
      console: path.relative(bundleRoot, consolePath),
      network: path.relative(bundleRoot, networkPath),
      state: path.relative(bundleRoot, stateOutputPath),
    },
    console_error_count: consoleMessages.filter((message) => message.type === "error").length,
    network_failures: networkEvents.filter(
      (event) =>
        event.type === "failed" &&
        !(event.failure_text === "net::ERR_ABORTED" && event.url.includes("/downloads/")),
    ).length,
    classified_network_events: networkEvents.filter(
      (event) =>
        event.type === "failed" &&
        event.failure_text === "net::ERR_ABORTED" &&
        event.url.includes("/downloads/"),
    ),
    downloaded_file: fs.existsSync(path.join(scenarioDir, "download.bin"))
      ? path.relative(bundleRoot, path.join(scenarioDir, "download.bin"))
      : null,
  };
  const summaryPath = path.join(scenarioDir, "summary.json");
  writeJson(summaryPath, summary);

  await context.close();
  return {
    ...summary,
    output_files: {
      ...summary.output_files,
      summary: path.relative(bundleRoot, summaryPath),
    },
  };
}

async function main() {
  const manifestPath = process.argv[2];
  if (!manifestPath) {
    throw new Error("usage: node portal_playwright_capture.mjs /abs/path/to/manifest.json");
  }

  const manifest = readJson(manifestPath);
  const bundleRoot = path.dirname(path.resolve(manifestPath));
  const { chromium } = await loadPlaywright();
  const headless = process.env.BURN_P2P_PLAYWRIGHT_HEADED === "1" ? false : true;
  const launchOptions = {
    headless,
    args: ["--disable-dev-shm-usage"],
  };
  const chromeExecutable = resolveChromeExecutable();
  if (chromeExecutable) {
    launchOptions.executablePath = chromeExecutable;
  }
  const browser = await chromium.launch(launchOptions);
  const { server, baseUrl } = await startScenarioServer(bundleRoot, manifest);

  try {
    const scenarioResults = [];
    for (const scenario of manifest.scenarios) {
      scenarioResults.push(await captureScenario(browser, baseUrl, bundleRoot, scenario));
    }

    const summary = {
      generated_at: new Date().toISOString(),
      browser: chromeExecutable ? "chromium-system-chrome" : "chromium-playwright",
      base_url: baseUrl,
      bundle_root: bundleRoot,
      scenario_count: scenarioResults.length,
      scenarios: scenarioResults,
    };
    writeJson(path.join(bundleRoot, "summary.json"), summary);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
