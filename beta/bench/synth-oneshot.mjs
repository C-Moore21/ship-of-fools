#!/usr/bin/env node
// Builds fixtures for `GET /api/shows/<date>?include=tracks`.
//
// The deployed app predates that parameter, so it cannot be recorded directly.
// Every byte here still comes from production: this fetches the real
// /api/shows/<date> payload and the real tracklist of the source that endpoint
// ranks first, then assembles them exactly the way the new server branch does
// (tracks for sources[0], plus best_source_id). Nothing is invented — only the
// envelope is new.
//
//   node bench/synth-oneshot.mjs [--api-origin=https://ship-of-fools.onrender.com]
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { fileURLToPath } from 'node:url'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const FIXTURES = path.join(HERE, 'fixtures')

const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)(?:=(.*))?$/)
    return m ? [m[1], m[2] ?? 'true'] : [a, 'true']
  })
)
const ORIGIN = argv['api-origin'] ?? 'https://ship-of-fools.onrender.com'

const key = (method, pathAndQuery, body) =>
  crypto.createHash('sha1')
    .update(method + ' ' + pathAndQuery + (body ? '|' + body : ''))
    .digest('hex').slice(0, 20)

// Every date the bench already has a sources fixture for.
const dates = new Set()
for (const f of fs.readdirSync(FIXTURES)) {
  if (!f.endsWith('.json')) continue
  let d
  try { d = JSON.parse(fs.readFileSync(path.join(FIXTURES, f), 'utf8')) } catch { continue }
  const m = String(d.url ?? '').match(/^\/api\/shows\/(\d{4}-\d{2}-\d{2})\/sources$/)
  if (m) dates.add(m[1])
}

console.log(`${dates.size} dates to build`)

let built = 0, noSources = 0, noTracks = 0
for (const date of dates) {
  const detailPath = `/api/shows/${date}`
  let detail
  try {
    const res = await fetch(ORIGIN + detailPath)
    if (!res.ok) throw new Error(String(res.status))
    detail = await res.json()
  } catch (e) {
    console.log(`  ${date}: detail failed (${e.message})`)
    continue
  }

  const sources = detail.sources ?? []
  if (!sources.length) { noSources++; continue }

  // The new server branch folds in the tracklist of its own top-ranked source
  // and names it, so the client can tell whether it matches its own pick.
  const bestId = sources[0].id
  try {
    const res = await fetch(`${ORIGIN}/api/sources/${encodeURIComponent(bestId)}/tracks`)
    if (!res.ok) throw new Error(String(res.status))
    const tracks = await res.json()
    if (tracks && Array.isArray(tracks.sets) && tracks.sets.length) {
      detail.best_source_id = bestId
      detail.tracks = tracks
    } else {
      noTracks++
    }
  } catch (e) {
    // A cache miss server-side is a real outcome — leave tracks absent and let
    // the client fall back, which is what production would do too.
    noTracks++
  }

  const body = JSON.stringify(detail)
  const fp = path.join(FIXTURES, key('GET', detailPath + '?include=tracks') + '.json')
  fs.writeFileSync(fp, JSON.stringify({
    url: detailPath + '?include=tracks',
    method: 'GET',
    postData: null,
    status: 200,
    contentType: 'application/json',
    body,
    // Record mode refuses to overwrite these. The deployed app ignores
    // ?include=tracks, so a record pass would silently replace them with
    // track-less payloads and the bench would quietly measure the fallback.
    synthesized: true,
  }))
  built++
  process.stdout.write(`\r  built ${built}/${dates.size}`)
}

console.log(`\ndone: ${built} fixtures, ${noSources} with no sources, ${noTracks} without a cached tracklist`)
