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
  hydrateShow,
  type SourceOption,
} from '../api/adapters'
import {
  getShowsForYear,
  getSources,
  getTodaysPick,
  getTracks,
  getYears,
  type RawSource,
  type RawTodayPick,
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

function cachedTracks(sourceId: string) {
  const existing = tracksBySource.get(sourceId)
  if (existing) return existing
  const p = getTracks(sourceId)
    .then((doc) => adaptTracks(doc.sets || []))
    .catch((e) => {
      tracksBySource.delete(sourceId)
      throw e
    })
  tracksBySource.set(sourceId, p)
  return p
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
    const rawSources = await cachedSources(base.id)
    if (rawSources.length === 0) {
      return { ...base, tracks: [] }
    }
    const best =
      [...rawSources].sort(
        (a, b) => (b.archive_rating ?? -1) - (a.archive_rating ?? -1),
      )[0] ?? rawSources[0]
    const tracks = await cachedTracks(best.id)
    return hydrateShow(base, rawSources, best, tracks)
  })
}

/**
 * "Today in Grateful Dead history" — the top-scored pick for month-day.
 * Returns a shallow Show for display in the top banner.
 */
export function useTodaysPick(): AsyncState<{ show: Show; otherSources: number } | null> {
  return useAsync('today', async () => {
    const picks = await cachedTodaysPick()
    if (!picks || picks.length === 0) return null
    const top = picks[0]
    // Group by date to compute "other sources" for the top show.
    const sameDay = picks.filter((p) => p.id === top.id)
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
    return { show, otherSources: Math.max(sameDay.length - 1, 0) }
  })
}
