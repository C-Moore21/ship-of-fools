#!/usr/bin/env node
// UI click-latency benchmark for the beta app.
//
//   node bench/latency.mjs --mode=record        # capture API fixtures from live Flask (:5000)
//   node bench/latency.mjs                      # replay fixtures -> deterministic frontend numbers
//   node bench/latency.mjs --mode=live          # measure against live Flask, network included
//   node bench/latency.mjs --label="memo pass"  # tag the run
//
// Replay is the loop's metric: fixed synthetic API latency means any delta is
// a real frontend change, not Atlas jitter.
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { execSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright'
import { startServer } from './server.mjs'
import { PROBE_SOURCE } from './probe.mjs'
import { SCENARIOS } from './scenarios.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const FIXTURES = path.join(HERE, 'fixtures')
const RESULTS = path.join(HERE, 'results')

const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/)
    return m ? [m[1], m[2] ?? 'true'] : [a, 'true']
  })
)

const MODE = argv.mode ?? 'replay'                 // record | replay | live
// 15 reps, and the headline is the MEDIAN. p75 of 7 samples is the 6th of 7 —
// effectively the max, and two runs of an identical build disagreed by 100% on
// the sub-100ms scenarios. Median of 15 holds those inside a few ms.
const REPS = Number(argv.reps ?? 15)
// Synthetic per-call API latency in replay. 250ms approximates what the
// recording showed against Render + Atlas (0.19s-0.98s per endpoint). It has to
// be realistic: too low and a serial request chain looks cheap, which would make
// the loop under-value the round-trip fixes that matter most in production.
const API_DELAY = Number(argv['api-delay'] ?? 250)
const QUIET_MS = Number(argv.quiet ?? 150)         // DOM still this long => settled
const MAX_WAIT = Number(argv['max-wait'] ?? 12000)
const HEADED = argv.headed === 'true'
const ONLY = argv.only ? new Set(argv.only.split(',')) : null
const LABEL = argv.label ?? ''
const VIEWPORT = { width: 1600, height: 950 }
// Where record/live modes source real API responses. Defaults to local Flask;
// point at the deployed app to record fixtures without running a server:
//   --api-origin=https://ship-of-fools.onrender.com
const ORIGIN_API = argv['api-origin'] ?? argv.flask ?? 'http://127.0.0.1:5000'
const FLASK = ORIGIN_API

function fixtureKey(method, url, body) {
  const u = new URL(url)
  const raw = method + ' ' + u.pathname + u.search + (body ? '|' + body : '')
  return crypto.createHash('sha1').update(raw).digest('hex').slice(0, 20)
}
const fixturePath = (key) => path.join(FIXTURES, key + '.json')

const median = (a) => {
  if (!a.length) return null
  const s = [...a].sort((x, y) => x - y)
  const m = s.length >> 1
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}
const pct = (a, p) => {
  if (!a.length) return null
  const s = [...a].sort((x, y) => x - y)
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)]
}
const r1 = (n) => (n == null || Number.isNaN(n) ? null : Math.round(n * 10) / 10)

async function installRouting(page) {
  // Deterministic fonts: fulfil with empty CSS instead of hitting Google.
  if (MODE !== 'live') {
    await page.route('**://fonts.googleapis.com/**', (route) =>
      route.fulfill({ status: 200, contentType: 'text/css', body: '' }))
    await page.route('**://fonts.gstatic.com/**', (route) => route.abort())
  }

  if (MODE === 'record') {
    const stats = { seen: 0, saved: 0, failed: 0 }
    await page.route('**/api/**', async (route) => {
      const req = route.request()
      const u = new URL(req.url())
      stats.seen++
      try {
        // route.fetch bypasses CORS, so ORIGIN may point at a different host
        // (production) than the page. Fulfilling keeps it same-origin for the app.
        const res = await route.fetch({ url: ORIGIN_API + u.pathname + u.search, timeout: 60000 })
        const body = await res.text()
        const contentType = res.headers()['content-type'] ?? 'application/json'
        const fp = fixturePath(fixtureKey(req.method(), req.url(), req.postData()))
        // A synthesized fixture models an endpoint shape the deployed app does
        // not serve yet. Overwriting it with the live response would silently
        // turn the benchmark back into a measurement of the old code path.
        if (fs.existsSync(fp)) {
          try {
            if (JSON.parse(fs.readFileSync(fp, 'utf8')).synthesized) {
              stats.skipped = (stats.skipped ?? 0) + 1
              return route.fulfill({ status: res.status(), contentType, body })
            }
          } catch (_) { /* unreadable fixture: fall through and rewrite it */ }
        }
        fs.writeFileSync(fp,
          JSON.stringify({
            url: u.pathname + u.search,
            method: req.method(),
            postData: req.postData() ?? null,
            status: res.status(),
            contentType,
            body,
          }))
        stats.saved++
        return route.fulfill({ status: res.status(), contentType, body })
      } catch (e) {
        stats.failed++
        return route.fulfill({ status: 200, contentType: 'application/json', body: '[]' })
      }
    })
    return () => stats
  }

  if (MODE === 'replay') {
    const misses = new Map()
    await page.route('**/api/**', async (route) => {
      const req = route.request()
      const key = fixtureKey(req.method(), req.url(), req.postData())
      const fp = fixturePath(key)
      if (API_DELAY) await new Promise((r) => setTimeout(r, API_DELAY))
      if (!fs.existsSync(fp)) {
        const p = new URL(req.url()).pathname
        misses.set(p, (misses.get(p) ?? 0) + 1)
        // Shape-neutral empty payload so a miss degrades instead of crashing.
        return route.fulfill({ status: 200, contentType: 'application/json', body: '[]' })
      }
      const f = JSON.parse(fs.readFileSync(fp, 'utf8'))
      return route.fulfill({ status: f.status, contentType: f.contentType, body: f.body })
    })
    return () => ({ misses: Object.fromEntries(misses) })
  }

  await page.route('**/api/**', async (route) => {
    const u = new URL(route.request().url())
    try {
      const res = await route.fetch({ url: ORIGIN_API + u.pathname + u.search, timeout: 60000 })
      return route.fulfill({ response: res })
    } catch (_) {
      return route.fulfill({ status: 200, contentType: 'application/json', body: '[]' })
    }
  })
  return () => ({})
}

async function measure(page, scenario) {
  await page.evaluate(() => window.__bench.begin())
  await scenario.act(page)

  const deadline = Date.now() + MAX_WAIT
  for (;;) {
    const q = await page.evaluate(() => ({
      quietFor: window.__bench.quietFor(),
      touched: window.__bench.batches > 0,
    }))
    if (q.touched && q.quietFor >= QUIET_MS) {
      if (!scenario.until || (await scenario.until(page))) break
    }
    if (Date.now() > deadline) {
      // The predicate never came true — the number would be meaningless.
      const m = await page.evaluate(() => window.__bench.finish())
      return { ...m, timedOut: true }
    }
    await page.waitForTimeout(25)
  }
  return page.evaluate(() => window.__bench.finish())
}

async function main() {
  fs.mkdirSync(FIXTURES, { recursive: true })
  fs.mkdirSync(RESULTS, { recursive: true })

  let staticServer = null
  let origin = FLASK
  const useStatic = MODE !== 'live' || argv.server === 'static'
  if (useStatic) {
    const s = await startServer(Number(argv.port ?? 5199))
    staticServer = s.server
    origin = s.origin
  }

  const browser = await chromium.launch({
    channel: 'chrome',
    headless: !HEADED,
    args: ['--disable-background-timer-throttling', '--disable-renderer-backgrounding'],
  })
  const context = await browser.newContext({ viewport: VIEWPORT, deviceScaleFactor: 1 })
  await context.addInitScript({ content: PROBE_SOURCE })
  const page = await context.newPage()
  const routeStats = await installRouting(page)

  const pageErrors = []
  page.on('pageerror', (e) => pageErrors.push(String(e).slice(0, 300)))

  await page.goto(origin + '/beta', { waitUntil: 'load', timeout: 30000 })
  await page.waitForSelector('nav[aria-label="Years"] button', { timeout: 30000 })
  await page.waitForTimeout(1200) // let first-paint work drain

  const scenarios = SCENARIOS.filter((s) => !ONLY || ONLY.has(s.id))
  const results = {}

  for (const sc of scenarios) {
    const runs = []
    for (let i = 0; i < REPS + 1; i++) {
      try {
        await sc.prepare(page)
        const m = await measure(page, sc)
        if (sc.cleanup) await sc.cleanup(page)
        if (i > 0) runs.push(m) // rep 0 is warm-up: lazy chunk fetch + JIT
      } catch (err) {
        if (i > 0) runs.push({ error: String(err).slice(0, 200) })
        try { if (sc.cleanup) await sc.cleanup(page) } catch (_) {}
      }
      await page.waitForTimeout(120)
    }
    // A timed-out rep never reached its completion predicate, so its settle
    // time is a floor, not a measurement. Excluded rather than averaged in.
    const ok = runs.filter((r) => !r.error && !r.timedOut && r.settleMs != null)
    const pick = (k) => ok.map((r) => r[k]).filter((v) => v != null)
    results[sc.id] = {
      description: sc.description,
      samples: ok.length,
      errors: runs.length - ok.length,
      timeouts: runs.filter((r) => r.timedOut).length,
      errorSamples: [...new Set(runs.filter((r) => r.error).map((r) => r.error))].slice(0, 3),
      settleMs: {
        median: r1(median(pick('settleMs'))),
        p75: r1(pct(pick('settleMs'), 75)),
        p25: r1(pct(pick('settleMs'), 25)),
        // Spread, so compare.mjs can refuse to call a within-noise delta a win.
        iqr: r1((pct(pick('settleMs'), 75) ?? 0) - (pct(pick('settleMs'), 25) ?? 0)),
        max: r1(pick('settleMs').length ? Math.max(...pick('settleMs')) : null),
      },
      firstPaintMs: { median: r1(median(pick('firstPaintMs'))), p75: r1(pct(pick('firstPaintMs'), 75)) },
      blockingMs: { median: r1(median(pick('blockingMs'))), p75: r1(pct(pick('blockingMs'), 75)) },
      handlerMs: { median: r1(median(pick('handlerMs'))) },
      mutations: { median: r1(median(pick('mutations'))) },
      batches: { median: r1(median(pick('batches'))) },
      raw: ok,
    }
    const s = results[sc.id]
    console.log(
      sc.id.padEnd(22) +
      ' settle ' + String(s.settleMs.median).padStart(7) + 'ms ' +
      ' iqr ' + String(s.settleMs.iqr).padStart(6) + 'ms ' +
      ' paint ' + String(s.firstPaintMs.median).padStart(6) + 'ms ' +
      ' block ' + String(s.blockingMs.median).padStart(6) + 'ms ' +
      ' commits ' + String(s.batches.median).padStart(4) +
      (s.errors ? '  (' + s.errors + ' err' + (s.timeouts ? ', ' + s.timeouts + ' timeout' : '') + ')' : '')
    )
    if (s.errorSamples.length) console.log('   ! ' + s.errorSamples[0])
  }

  const settleMedians = Object.values(results).map((r) => r.settleMs.median).filter((v) => v != null)
  let git = null
  try { git = execSync('git rev-parse --short HEAD').toString().trim() } catch (_) {}

  const report = {
    ts: new Date().toISOString(),
    mode: MODE,
    label: LABEL,
    // Guards comparisons: a run scored on a different metric is not comparable.
    metric: 'meanSettle',
    git,
    config: { reps: REPS, apiDelayMs: API_DELAY, quietMs: QUIET_MS, viewport: VIEWPORT },
    overall: {
      // Headline: mean of per-scenario MEDIAN settle. One number the loop drives down.
      meanSettle: r1(settleMedians.reduce((a, b) => a + b, 0) / (settleMedians.length || 1)),
      worstScenario: Object.entries(results).sort(
        (a, b) => (b[1].settleMs.median ?? 0) - (a[1].settleMs.median ?? 0)
      )[0]?.[0] ?? null,
    },
    routing: routeStats(),
    pageErrors,
    scenarios: results,
  }

  const out = argv.out ?? path.join(RESULTS, report.ts.replace(/[:.]/g, '-') + '.json')
  fs.writeFileSync(out, JSON.stringify(report, null, 2))
  fs.writeFileSync(path.join(RESULTS, 'latest.json'), JSON.stringify(report, null, 2))

  console.log('\nmean settle (median): ' + report.overall.meanSettle + 'ms   worst: ' + report.overall.worstScenario)
  if (report.routing.misses && Object.keys(report.routing.misses).length)
    console.log('fixture misses:', report.routing.misses)
  if (report.routing.saved != null)
    console.log('fixtures saved: ' + report.routing.saved + ' / ' + report.routing.seen + ' api responses')
  if (pageErrors.length)
    console.log('page errors: ' + pageErrors.length + '\n  ' + pageErrors.slice(0, 3).join('\n  '))
  console.log('-> ' + out)

  await browser.close()
  if (staticServer) staticServer.close()
}

main().catch((e) => { console.error(e); process.exit(1) })
