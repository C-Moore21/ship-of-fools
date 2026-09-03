/**
 * Map raw API payloads into the frontend `Show`/`YearEntry`/`Track` shapes.
 *
 * The API surfaces less metadata than the beta UI models (no weather / tour
 * run / listen counts on the shallow show list, etc). We fill sensible
 * defaults so the visual components render without special-casing.
 */

import type { GapLabel, Rarity, Show, Track, YearEntry } from '../types/archive'
import type { RawShow, RawSource, RawTrack, RawTrackSet, RawYear, SetlistStatsResp, RawSongStat } from './client'

/** GD-era mapping used for the year rail badges and detail header. */
export function eraForYear(year: number): string {
  if (year <= 1969) return 'Proto-Touring'
  if (year <= 1974) return 'Classic'
  if (year === 1975) return 'Hiatus'
  if (year <= 1978) return 'Post-Hiatus'
  if (year <= 1980) return 'Regular Cycle'
  if (year <= 1990) return 'Brent Era'
  return 'Final Era'
}

/**
 * Fallback per-year show counts used when the API returns `show_count: null`
 * (which it currently does for every year). Numbers match the placeholder
 * table shipped with the beta UI so the year-rail bar widths stay stable.
 */
const YEAR_SHOW_COUNT_FALLBACK: Record<number, number> = {
  1965: 4, 1966: 22, 1967: 41, 1968: 68, 1969: 104,
  1970: 142, 1971: 81, 1972: 88, 1973: 73, 1974: 41,
  1975: 4,
  1976: 41, 1977: 60, 1978: 81,
  1979: 77, 1980: 87,
  1981: 86, 1982: 65, 1983: 66, 1984: 64, 1985: 72,
  1986: 45, 1987: 87, 1988: 80, 1989: 73, 1990: 74,
  1991: 76, 1992: 54, 1993: 81, 1994: 84, 1995: 47,
}

export function adaptYears(raw: RawYear[]): YearEntry[] {
  return raw.map((r) => {
    const year = Number(r.year)
    return {
      year,
      shows: r.show_count ?? YEAR_SHOW_COUNT_FALLBACK[year] ?? 0,
      era: eraForYear(year),
    }
  })
}

/**
 * Build a shallow `Show` from a `RawShow` — no tracks, source metadata
 * placeholders. Ratings/soundboard/etc get filled in once a source is
 * selected and its tracks load.
 */
export function adaptShallowShow(raw: RawShow): Show {
  const year = Number(raw.display_date.slice(0, 4))
  const era = eraForYear(year)
  return {
    id: raw.id,
    date: raw.display_date,
    venue: raw.venue?.name ?? '',
    city: raw.venue?.location ?? '',
    era,
    tourRun: `${era} · ${year}`,
    // Prefer local community avg if we have real ratings for this show;
    // otherwise fall back to Archive.org's public avg_rating (populated for
    // every show now that /api/years/<year>/shows requests the field).
    avgRating: (raw.community_count ?? 0) > 0
      ? (raw.community_avg ?? 0)
      : (raw.avg_rating ?? 0),
    ratingCount: (raw.community_count ?? 0) > 0
      ? (raw.community_count ?? 0)
      : (raw.num_reviews ?? 0),
    listeners: raw.community_listens ?? 0,
    soundboard: false,
    source: '',
    weather: '',
    tempF: 0,
    tracks: [],
  }
}

/**
 * Normalize an Archive.org album/set name into Set I / II / E.
 *
 * The album tag varies wildly across shows: "Set 1", "Set I", "Set One",
 * "Encore", or sometimes just the show title ("Grateful Dead Live at ..."). We
 * only want to treat a name as encore/set-2/etc when it's ACTUALLY labeled
 * that way — a title containing "Live" (which most do) must not fire the
 * encore branch. So: look for the set/encore keyword as a whole word, and
 * require digits to stand alone (not as part of a date like "1987").
 */
export function normalizeSetName(name: string, orderIndex: number): Track['set'] {
  const n = (name || '').toLowerCase().trim()
  if (n === '' || n === 'set 1' || n === 'set i' || n === 'set one') return 'I'
  if (n === 'set 2' || n === 'set ii' || n === 'set two') return 'II'
  if (n === 'encore' || n === 'e') return 'E'
  // Prefixed forms: "set 3", "second set", "encore 1", etc.
  if (/\bencore\b/.test(n)) return 'E'
  if (/\bset\s*(?:2|ii|two|second)\b/.test(n) || /\b(?:second|two)\s+set\b/.test(n)) return 'II'
  if (/\bset\s*(?:1|i|one|first)\b/.test(n) || /\b(?:first|one)\s+set\b/.test(n)) return 'I'
  // Album tag doesn't mention set/encore at all (usually the show title).
  // Fall back to disc order: first bucket = Set I, second = II, else Encore.
  if (orderIndex === 0) return 'I'
  if (orderIndex === 1) return 'II'
  return 'E'
}

/**
 * Assign a placeholder "rarity" — the real gap/rarity data lives elsewhere
 * (setlist_cache) and isn't exposed on the tracks endpoint. Everything is
 * "common" until we wire that up.
 */
function defaultRarity(): Rarity {
  return 'common'
}

/** Map a song's total-play count → rarity tier (roughly matches classic UI). */
function rarityFromPlayCount(total: number): Rarity {
  if (total < 20) return 'legendary'
  if (total < 50) return 'special'
  if (total < 200) return 'rare'
  if (total < 500) return 'solid'
  return 'common'
}

/** Pick the most-notable gap label for a track, or null if none applies. */
function gapLabelFor(s: RawSongStat): { label: GapLabel; shows: number } | null {
  if (s.is_debut) return { label: 'Debut', shows: 0 }
  const g = s.gap_before ?? 0
  // Drought-breaker: longest-ever gap for this song
  if (s.drought_rank === 1 && g >= 60) return { label: 'Bust', shows: g }
  if (g >= 365) return { label: 'Drought', shows: g }
  if (g >= 60) return { label: 'Gap', shows: g }
  return null
}

/** Merge server-side setlist stats onto adapted tracks. Non-destructive to
 *  tracks that don't match (title normalization on the server side means
 *  some tracks — tuning, silence — never come back). */
export function applySetlistStats(tracks: Track[], stats: SetlistStatsResp): Track[] {
  // Server returns song_data keyed by normalized name; each entry has `.raw`
  // which is the *first* raw title that normalized to that key. Build the
  // reverse lookup by raw title.
  const byRaw = new Map<string, RawSongStat>()
  for (const s of Object.values(stats.songs || {})) {
    if (s.raw) byRaw.set(s.raw.trim().toLowerCase(), s)
  }
  return tracks.map((t) => {
    // Match on the base title (segue arrows already stripped in adaptTracks).
    const hit = byRaw.get(t.title.trim().toLowerCase())
    if (!hit) return t
    const rarity = rarityFromPlayCount(hit.total)
    const gap = gapLabelFor(hit)
    return { ...t, rarity, gap: gap ?? undefined }
  })
}

export function adaptTracks(sets: RawTrackSet[]): Track[] {
  // If Archive.org grouped everything under one album (as happens when the
  // taper only tagged one album), fall back to detecting set breaks from the
  // tracklist itself: a short "Tuning"/"Crowd"/"Space" that isn't the first
  // track is almost always a set marker. Split into up to 3 buckets
  // (Set I / II / Encore) using that signal.
  const effectiveSets: RawTrackSet[] =
    sets.length === 1 && sets[0].tracks.length >= 15
      ? splitByBreakMarkers(sets[0])
      : sets

  const out: Track[] = []
  effectiveSets.forEach((s, setIdx) => {
    const setLabel = normalizeSetName(s.name, setIdx)
    let position = 0
    for (const raw of s.tracks) {
      position += 1
      const rawTitle = (raw.title || '').trim()
      const segue = rawTitle.endsWith('>')
      const title = segue ? rawTitle.replace(/>$/, '').trim() : rawTitle
      out.push({
        id: raw.id,
        position,
        title,
        duration: raw.duration || 0,
        set: setLabel,
        segue,
        rarity: defaultRarity(),
      })
    }
  })
  return out
}

/**
 * Heuristic set splitter for single-album shows. Break markers: short (<60s)
 * "Tuning" or "Crowd" tracks that are not the first track. If we find one,
 * split there. If we find two, first split = Set I/II boundary, second =
 * Encore boundary. Never produces more than 3 sets.
 */
function splitByBreakMarkers(bucket: RawTrackSet): RawTrackSet[] {
  const isBreak = (t: RawTrack, idx: number): boolean => {
    if (idx === 0) return false
    const title = (t.title || '').toLowerCase()
    if (!/tuning|crowd|silence|band intro/i.test(title)) return false
    return (t.duration || 0) < 60
  }
  const breakIndices: number[] = []
  bucket.tracks.forEach((t, i) => { if (isBreak(t, i)) breakIndices.push(i) })
  if (breakIndices.length === 0) return [bucket]

  // First break → Set II boundary; second break → Encore boundary.
  const cuts = breakIndices.slice(0, 2)
  const chunks: RawTrack[][] = []
  let prev = 0
  for (const cut of cuts) {
    chunks.push(bucket.tracks.slice(prev, cut))
    prev = cut
  }
  chunks.push(bucket.tracks.slice(prev))
  const labels = ['Set 1', 'Set 2', 'Encore']
  return chunks
    .filter((c) => c.length > 0)
    .map((tracks, i) => ({ name: labels[i] || `Set ${i + 1}`, tracks }))
}

export interface SourceOption {
  id: string
  label: string
  soundboard: boolean
  rating?: number
}

/**
 * Extract the meaningful tail of an Archive.org identifier, e.g.
 *   gd77-05-08.sbd.hicks.4982.sbeok.shnf  →  hicks.4982
 * The date prefix, source-type token, and boilerplate suffixes (sbeok, shnf,
 * flac16/24, etc) get stripped so what's left is the taper name + version.
 */
function shortenIdentifier(id: string): string {
  const parts = id.split('.')
  // Drop the leading "gd<date>" segment
  const rest = parts[0]?.match(/^gd\d/) ? parts.slice(1) : parts
  const boilerplate = new Set([
    'sbeok', 'shnf', 'shn', 'flac', 'flac16', 'flac24', 'flac96',
    'aud', 'sbd', 'mtx', 'fob', 'matrix', 'soundboard', 'audience',
    'mp3', 'sirmick', 'miller', 'unknown', 'unk',
  ])
  const meaningful = rest.filter((p) => !boilerplate.has(p.toLowerCase()))
  return meaningful.slice(0, 2).join('.') || id
}

export function adaptSource(raw: RawSource): SourceOption {
  const st = (raw.source_type || '').toUpperCase() || 'UNK'
  const tail = shortenIdentifier(raw.id)
  const reviews = raw.archive_reviews ?? 0
  const label = reviews > 0 ? `${st} · ${tail} · ${reviews} rev` : `${st} · ${tail}`
  return {
    id: raw.id,
    label,
    soundboard: st.includes('SBD') || st.includes('MATRIX') || st === 'MTX',
    rating: raw.archive_rating ?? undefined,
  }
}

/**
 * Merge track data + best-source metadata into a hydrated Show.
 */
export function hydrateShow(
  base: Show,
  sources: RawSource[],
  chosen: RawSource | undefined,
  tracks: Track[],
): Show {
  const chosenType = (chosen?.source_type || '').toUpperCase()
  return {
    ...base,
    tracks,
    source: chosen?.title || base.source,
    soundboard: chosenType.includes('SBD') || chosenType.includes('MATRIX'),
    avgRating: chosen?.archive_rating ?? base.avgRating,
    ratingCount: chosen?.archive_reviews ?? base.ratingCount,
  }
}
