#!/usr/bin/env node
// Diff the latest bench run against the frozen baseline.
//
//   node bench/compare.mjs                  # latest vs baseline
//   node bench/compare.mjs --set-baseline   # freeze latest as the baseline
//
// Exit codes drive the self-improving loop:
//   0  goal met (mean settle p75 <= baseline * TARGET)
//   1  improved but not there yet
//   2  regressed vs the previous run (revert the last change)
//   3  broken run (page errors, missing samples)
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const RESULTS = path.join(HERE, 'results')
const BASELINE = path.join(HERE, 'baseline.json')
const PREV = path.join(HERE, 'results', 'previous.json')

const TARGET = 0.5 // the goal: half the baseline latency

// A delta only counts as real if it clears both bars. Repeat runs of an
// identical build moved the sub-100ms scenarios by ~100%, so a percentage
// alone will happily report noise as a win.
const MIN_PCT = 8
const MIN_MS = 5

const argv = new Set(process.argv.slice(2))
const load = (p) => (fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null)

const latest = load(path.join(RESULTS, 'latest.json'))
if (!latest) { console.error('No bench/results/latest.json — run: node bench/latency.mjs'); process.exit(3) }

if (argv.has('--set-baseline')) {
  fs.writeFileSync(BASELINE, JSON.stringify(latest, null, 2))
  console.log('baseline frozen at mean settle = ' + latest.overall.meanSettle + 'ms')
  console.log('goal: ' + (latest.overall.meanSettle * TARGET).toFixed(1) + 'ms or below')
  process.exit(0)
}

const base = load(BASELINE)
if (!base) { console.error('No baseline — run: node bench/compare.mjs --set-baseline'); process.exit(3) }
let prev = load(PREV)
// A run scored under an older metric would produce a meaningless delta.
if (prev && prev.metric !== latest.metric) prev = null

const pctDelta = (now, then) => (then ? ((now - then) / then) * 100 : 0)
const fmt = (d) => (d > 0 ? '+' : '') + d.toFixed(1) + '%'
const arrow = (d, absMs) => {
  if (Math.abs(d) < MIN_PCT || Math.abs(absMs) < MIN_MS) return 'noise '
  return d < 0 ? 'FASTER' : 'SLOWER'
}

console.log('scenario                  baseline    latest     delta            iqr  commits')
console.log('-'.repeat(80))

const rows = []
for (const [id, b] of Object.entries(base.scenarios)) {
  const l = latest.scenarios[id]
  if (!l) { console.log(id.padEnd(24) + '  (missing in latest)'); continue }
  const bv = b.settleMs.median, lv = l.settleMs.median
  const d = pctDelta(lv, bv)
  rows.push({ id, base: bv, now: lv, d })
  console.log(
    id.padEnd(24) +
    String(bv).padStart(8) + 'ms' +
    String(lv).padStart(8) + 'ms' +
    fmt(d).padStart(9) + '  ' + arrow(d, lv - bv) +
    '  +-' + String(l.settleMs.iqr ?? '?').padStart(5) +
    '  ' + b.batches.median + '->' + l.batches.median
  )
}

const bMean = base.overall.meanSettle ?? base.overall.meanSettleP75
const lMean = latest.overall.meanSettle ?? latest.overall.meanSettleP75
const goal = bMean * TARGET
const overall = pctDelta(lMean, bMean)

console.log('-'.repeat(80))
console.log('MEAN SETTLE   baseline ' + bMean + 'ms   latest ' + lMean + 'ms   ' + fmt(overall))
console.log('GOAL              ' + goal.toFixed(1) + 'ms (-50%)   ' +
  (lMean <= goal ? 'REACHED' : 'remaining ' + (lMean - goal).toFixed(1) + 'ms'))

if (prev) {
  const pd = pctDelta(lMean, prev.overall.meanSettle)
  console.log('vs previous run   ' + prev.overall.meanSettle + 'ms -> ' + lMean + 'ms   ' + fmt(pd) +
    (prev.label ? '   (prev: ' + prev.label + ')' : ''))
}

const worst = [...rows].sort((a, b) => b.now - a.now)[0]
if (worst) console.log('\nworst remaining:  ' + worst.id + ' @ ' + worst.now + 'ms p75')

// Roll latest -> previous so the next run can compare against this one.
fs.writeFileSync(PREV, JSON.stringify(latest, null, 2))

if (latest.pageErrors?.length) { console.log('\nBROKEN: page errors present'); process.exit(3) }
if (Object.values(latest.scenarios).some((s) => s.samples < 3)) { console.log('\nBROKEN: too few samples'); process.exit(3) }
if (lMean <= goal) { console.log('\nGOAL MET'); process.exit(0) }
if (prev && pctDelta(lMean, prev.overall.meanSettle) > MIN_PCT) { console.log('\nREGRESSED vs previous run'); process.exit(2) }
process.exit(1)
