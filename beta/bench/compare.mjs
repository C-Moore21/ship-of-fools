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

const argv = new Set(process.argv.slice(2))
const load = (p) => (fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, 'utf8')) : null)

const latest = load(path.join(RESULTS, 'latest.json'))
if (!latest) { console.error('No bench/results/latest.json — run: node bench/latency.mjs'); process.exit(3) }

if (argv.has('--set-baseline')) {
  fs.writeFileSync(BASELINE, JSON.stringify(latest, null, 2))
  console.log('baseline frozen at mean settle p75 = ' + latest.overall.meanSettleP75 + 'ms')
  console.log('goal: ' + (latest.overall.meanSettleP75 * TARGET).toFixed(1) + 'ms or below')
  process.exit(0)
}

const base = load(BASELINE)
if (!base) { console.error('No baseline — run: node bench/compare.mjs --set-baseline'); process.exit(3) }
const prev = load(PREV)

const pctDelta = (now, then) => (then ? ((now - then) / then) * 100 : 0)
const fmt = (d) => (d > 0 ? '+' : '') + d.toFixed(1) + '%'
const arrow = (d) => (d <= -3 ? 'FASTER' : d >= 3 ? 'SLOWER' : '  ~   ')

console.log('scenario                  baseline    latest     delta   commits(b->l)')
console.log('-'.repeat(74))

const rows = []
for (const [id, b] of Object.entries(base.scenarios)) {
  const l = latest.scenarios[id]
  if (!l) { console.log(id.padEnd(24) + '  (missing in latest)'); continue }
  const d = pctDelta(l.settleMs.p75, b.settleMs.p75)
  rows.push({ id, base: b.settleMs.p75, now: l.settleMs.p75, d })
  console.log(
    id.padEnd(24) +
    String(b.settleMs.p75).padStart(8) + 'ms' +
    String(l.settleMs.p75).padStart(8) + 'ms' +
    fmt(d).padStart(9) + '  ' + arrow(d) +
    '   ' + b.batches.median + ' -> ' + l.batches.median
  )
}

const bMean = base.overall.meanSettleP75
const lMean = latest.overall.meanSettleP75
const goal = bMean * TARGET
const overall = pctDelta(lMean, bMean)

console.log('-'.repeat(74))
console.log('MEAN SETTLE p75   baseline ' + bMean + 'ms   latest ' + lMean + 'ms   ' + fmt(overall))
console.log('GOAL              ' + goal.toFixed(1) + 'ms (-50%)   ' +
  (lMean <= goal ? 'REACHED' : 'remaining ' + (lMean - goal).toFixed(1) + 'ms'))

if (prev) {
  const pd = pctDelta(lMean, prev.overall.meanSettleP75)
  console.log('vs previous run   ' + prev.overall.meanSettleP75 + 'ms -> ' + lMean + 'ms   ' + fmt(pd) +
    (prev.label ? '   (prev: ' + prev.label + ')' : ''))
}

const worst = [...rows].sort((a, b) => b.now - a.now)[0]
if (worst) console.log('\nworst remaining:  ' + worst.id + ' @ ' + worst.now + 'ms p75')

// Roll latest -> previous so the next run can compare against this one.
fs.writeFileSync(PREV, JSON.stringify(latest, null, 2))

if (latest.pageErrors?.length) { console.log('\nBROKEN: page errors present'); process.exit(3) }
if (Object.values(latest.scenarios).some((s) => s.samples < 3)) { console.log('\nBROKEN: too few samples'); process.exit(3) }
if (lMean <= goal) { console.log('\nGOAL MET'); process.exit(0) }
if (prev && pctDelta(lMean, prev.overall.meanSettleP75) > 5) { console.log('\nREGRESSED vs previous run'); process.exit(2) }
process.exit(1)
