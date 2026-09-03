/**
 * React hooks for the SoF backend, layered on top of `../api/client`.
 *
 * - In-memory Promise caches dedupe concurrent requests and survive
 *   component remounts inside a single page load.
 * - Every hook returns `{ data, loading, error }`. Loading transitions
 *   to false on success or error.
 * - No revalidation policy yet — MongoDB does the heavy caching server-side
 *   and pages here don't stick around long enough for staleness to bite.
 */

import { useEffect, useState } from 'react'
import {
  adaptShallowShow,
  adaptSource,
  adaptTracks,
  adaptYears,
  applySetlistStats,
  hydrateShow,
  type SourceOption,
} from '../api/adapters'
import {
  getMyListens,
  getReleases,
  getSetlistStats,
  getShowsForYear,
  getSources,
  getTodaysPick,
  getTracks,
  getWeather,
  getYears,
  type RawReleaseInfo,
  type RawSource,
  type RawTodayPick,
  type RawWeather,
  type SetlistStatsResp,
} from '../api/client'
import type { Show, YearEntry } from '../types/archive'

interface AsyncState<T> {
  data: T | null
  loading: boolean
  error: Error | null
}

function useAsync<T>(key: string | null, run: () => Promise<T>): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>({
    data: null,
    loading: key != null,
    error: null,
  })

  useEffect(() => {
    if (key == null) {
      setState({ data: null, loading: false, error: null })
      return
    }
    let alive = true
    setState((s) => ({ ...s, loading: true, error: null }))
    run()
      .then((data) => {
        if (!alive) return
        setState({ data, loading: false, error: null })
      })
      .catch((error: Error) => {
        if (!alive) return
        setState({ data: null, loading: false, error })
      })
    return () => {
      alive = false
    }
    // `run` is captured by closure — key drives re-fetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])

  return state
}

// ─── caches ────────────────────────────────────────────────────────────────

let yearsCache: Promise<YearEntry[]> | null = null
const showsByYear: Map<number, Promise<Show[]>> = new Map()
const sourcesByDate: Map<string, Promise<RawSource[]>> = new Map()
const tracksBySource: Map<string, Promise<ReturnType<typeof adaptTracks>>> = new Map()
const weatherByDate: Map<string, Promise<RawWeather>> = new Map()
const setlistStatsByDate: Map<string, Promise<SetlistStatsResp>> = new Map()
let todaysPickCache: Promise<RawTodayPick[]> | null = null

function cachedYears(): Promise<YearEntry[]> {
  if (!yearsCache) {
    yearsCache = getYears()
      .then(adaptYears)
      .catch((e) => {
        yearsCache = null // allow retry
        throw e
      })
  }
  return yearsCache
}

function cachedShows(year: number): Promise<Show[]> {
  const existing = showsByYear.get(year)
  if (existing) return existing
  const p = getShowsForYear(year)
    .then((raw) => raw.map(adaptShallowShow))
    .catch((e) => {
      showsByYear.delete(year)
      throw e
    })
  showsByYear.set(year, p)
  return p
}

function cachedSources(date: string): Promise<RawSource[]> {
  const existing = sourcesByDate.get(date)
  if (existing) return existing
  const p = getSources(date).catch((e) => {
    sourcesByDate.delete(date)
    throw e
  })
  sourcesByDate.set(date, p)
  return p
}

interface TracksBundle {
  tracks: ReturnType<typeof adaptTracks>
  taper?: string
  transferer?: string
  lineage?: string
}

function cachedTracks(sourceId: string): Promise<TracksBundle> {
  const existing = tracksBySource.get(sourceId)
  if (existing) return existing as Promise<TracksBundle>
  const p: Promise<TracksBundle> = getTracks(sourceId)
    .then((doc) => ({
      tracks: adaptTracks(doc.sets || []),
      taper: doc.taper || undefined,
      transferer: doc.transferer || undefined,
      lineage: doc.lineage || undefined,
    }))
    .catch((e) => {
      tracksBySource.delete(sourceId)
      throw e
    })
  tracksBySource.set(sourceId, p as any)
  return p
}

function cachedWeather(date: string): Promise<RawWeather> {
  const existing = weatherByDate.get(date)
  if (existing) return existing
  // Weather is optional — swallow errors as empty so cache still short-circuits
  // and callers don't have to retry.
  const p = getWeather(date).catch(() => ({} as RawWeather))
  weatherByDate.set(date, p)
  return p
}

function cachedSetlistStats(
  date: string,
  songs: string[],
): Promise<SetlistStatsResp> {
  const existing = setlistStatsByDate.get(date)
  if (existing) return existing
  const p = getSetlistStats(date, songs).catch((e) => {
    setlistStatsByDate.delete(date)
    throw e
  })
  setlistStatsByDate.set(date, p)
  return p
}

/**
 * Fire-and-forget prefetch helpers — safe to call from onMouseEnter etc.
 * They hydrate the module-level caches so the next hook call resolves instantly.
 * Errors are swallowed so a hover never surfaces a rejection.
 */
export function prefetchShowsForYear(year: number): void {
  cachedShows(year).catch(() => {})
}
export function prefetchShow(date: string): void {
  // Sources + weather covers the two round-trips that fire when a show is
  // selected. Tracks + setlist-stats still wait for the click because they
  // depend on which source is chosen.
  cachedSources(date).catch(() => {})
  cachedWeather(date).catch(() => {})
}

function cachedTodaysPick(): Promise<RawTodayPick[]> {
  if (!todaysPickCache) {
    todaysPickCache = getTodaysPick().catch((e) => {
      todaysPickCache = null
      throw e
    })
  }
  return todaysPickCache
}

// ─── hooks ─────────────────────────────────────────────────────────────────

export function useYears(): AsyncState<YearEntry[]> {
  return useAsync('years', cachedYears)
}

export function useShowsForYear(year: number | null): AsyncState<Show[]> {
  return useAsync(year == null ? null : `shows:${year}`, () => cachedShows(year as number))
}

export function useSources(date: string | null): AsyncState<SourceOption[]> {
  return useAsync(date ? `sources:${date}` : null, async () => {
    const raw = await cachedSources(date as string)
    return raw.map(adaptSource)
  })
}

/**
 * Full show hydration: shallow record + best-source pick + tracks. Auto-picks
 * the highest-Archive-rated source (falling back to the first one).
 */
export function useShow(base: Show | null): AsyncState<Show> {
  const key = base ? `show:${base.id}` : null
  return useAsync(key, async () => {
    if (!base) throw new Error('no base show')
    const [rawSources, weather] = await Promise.all([
      cachedSources(base.id),
      cachedWeather(base.id),
    ])
    if (rawSources.length === 0) {
      return {
        ...base,
        tracks: [],
        weather: weather.weather ?? '',
        tempF: (weather as any).temp_f ?? (weather as any).tempF ?? 0,
      }
    }
    const best =
      [...rawSources].sort(
        (a, b) => (b.archive_rating ?? -1) - (a.archive_rating ?? -1),
      )[0] ?? rawSources[0]
    const bundle = await cachedTracks(best.id)
    // Setlist stats give real rarity + Debut/Bust/Gap/Drought badges.
    // Fetched in the background so the setlist appears immediately; when the
    // POST resolves we merge the badges in.
    let enrichedTracks = bundle.tracks
    try {
      const stats = await cachedSetlistStats(
        base.id,
        bundle.tracks.map((t) => t.title),
      )
      enrichedTracks = applySetlistStats(bundle.tracks, stats)
    } catch {
      // Non-fatal — tracks still render with default rarity.
    }
    const hydrated = hydrateShow(base, rawSources, best, enrichedTracks, {
      taper: bundle.taper,
      transferer: bundle.transferer,
      lineage: bundle.lineage,
    })
    return {
      ...hydrated,
      weather: weather.weather ?? '',
      tempF: (weather as any).temp_f ?? (weather as any).tempF ?? 0,
    }
  })
}

/**
 * "Today in Grateful Dead history" — the top-scored pick for month-day.
 * Returns a shallow Show for display in the top banner.
 */
export function useTodaysPick(): AsyncState<{
  show: Show
  otherSources: number
  otherDates: number
} | null> {
  return useAsync('today', async () => {
    const picks = await cachedTodaysPick()
    if (!picks || picks.length === 0) return null
    const top = picks[0]
    // "Other sources" = alternate recordings for the same show date.
    // "Other dates" = other years the Dead played on this same month-day.
    const sameDay = picks.filter((p) => p.id === top.id)
    const distinctDates = new Set(picks.map((p) => p.id))
    const show: Show = {
      id: top.id,
      date: top.display_date,
      venue: top.venue?.name ?? '',
      city: top.venue?.location ?? '',
      era: '',
      tourRun: '',
      avgRating: top.avg_rating ?? 0,
      ratingCount: top.num_reviews ?? 0,
      listeners: top.community_listens ?? 0,
      soundboard: (top.source_type || '').toUpperCase().includes('SBD'),
      source: top.source_type || '',
      weather: '',
      tempF: 0,
      tracks: [],
    }
    // Fill era post-hoc from the year.
    const y = Number(show.date.slice(0, 4))
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    show.era =
      y <= 1969
        ? 'Proto-Touring'
        : y <= 1974
        ? 'Classic'
        : y === 1975
        ? 'Hiatus'
        : y <= 1978
        ? 'Post-Hiatus'
        : y <= 1980
        ? 'Regular Cycle'
        : y <= 1990
        ? 'Brent Era'
        : 'Final Era'
    return {
      show,
      otherSources: Math.max(sameDay.length - 1, 0),
      otherDates: Math.max(distinctDates.size - 1, 0),
    }
  })
}

// ─── Releases & listens ────────────────────────────────────────────────────

export interface ReleaseInfo {
  name: string
  year: number | null
  /** Compact label suitable for a badge ("Dave's 15", "Dick's 18", ...). */
  short: string
}

export type ReleaseMap = Record<string, ReleaseInfo>

function shortenReleaseName(name: string): string {
  if (!name) return 'OFFICIAL'
  let m: RegExpMatchArray | null
  if ((m = name.match(/dick'?s picks (?:vol\.?\s*)?(\d+)/i))) return `Dick's ${m[1]}`
  if ((m = name.match(/dave'?s picks (?:vol\.?\s*)?(\d+)/i))) return `Dave's ${m[1]}`
  if ((m = name.match(/road trips (?:vol\.?\s*)?([\d.]+)/i))) return `Road Trips ${m[1]}`
  if ((m = name.match(/download series (?:vol\.?\s*)?(\d+)/i))) return `DLS ${m[1]}`
  if ((m = name.match(/30 days of dead\s+(\d{4})/i))) return `30 Days '${m[1].slice(2)}`
  return name.length > 20 ? name.slice(0, 19) + '…' : name
}

let releasesCache: Promise<ReleaseMap> | null = null
function cachedReleases(): Promise<ReleaseMap> {
  if (!releasesCache) {
    releasesCache = getReleases()
      .then((raw: Record<string, RawReleaseInfo>) => {
        const out: ReleaseMap = {}
        for (const [date, info] of Object.entries(raw)) {
          out[date] = {
            name: info.name,
            year: info.year ?? null,
            short: shortenReleaseName(info.name),
          }
        }
        return out
      })
      .catch((e) => {
        releasesCache = null
        throw e
      })
  }
  return releasesCache
}

export function useReleases(): AsyncState<ReleaseMap> {
  return useAsync('releases', cachedReleases)
}

// Listens are per-user, so we key the cache on login state and blow it away
// when the user logs out (parent flips `loggedIn` and the null key triggers a
// reset inside `useAsync`).
let listensCache: Promise<Set<string>> | null = null
function cachedListens(): Promise<Set<string>> {
  if (!listensCache) {
    listensCache = getMyListens()
      .then((rows) => {
        const s = new Set<string>()
        for (const r of rows) {
          const d = r.show_date || r.show_id
          if (d) s.add(d)
        }
        return s
      })
      .catch((e) => {
        listensCache = null
        throw e
      })
  }
  return listensCache
}

/** Reset by callers on login/logout transitions so the next mount refetches. */
export function invalidateListens(): void {
  listensCache = null
}

export function useMyListens(loggedIn: boolean): AsyncState<Set<string>> {
  // `key` toggles to null when logged out — useAsync then resets to empty.
  return useAsync(loggedIn ? 'listens:mine' : null, cachedListens)
}
