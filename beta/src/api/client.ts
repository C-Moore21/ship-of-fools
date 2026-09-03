/**
 * Typed fetch wrappers for the Ship of Fools Flask backend.
 *
 * All endpoints live at `/api/*` on the same origin. We forward cookies so
 * ratings/notes/listens work once the auth-and-social layer is wired up.
 *
 * The raw API shapes are intentionally narrow — see `adapters.ts` for the
 * mapping into the frontend `Show`/`YearEntry`/`Track` types.
 */

export interface RawYear {
  year: string
  show_count: number | null
}

export interface RawVenue {
  name: string
  location: string
}

export interface RawShow {
  id: string
  display_date: string
  venue: RawVenue
  avg_rating: number | null           // Archive.org's public rating (best recording per date)
  num_reviews?: number                 // Archive.org's public review count
  // Enriched by /api/years/<year>/shows — see app.py _enrich_shows_with_community
  community_avg?: number
  community_count?: number
  community_listens?: number
}

export interface RawTodayPick extends RawShow {
  identifier: string
  source_type: string
  num_reviews: number
  score: number
  community_listens: number
}

export interface RawSource {
  id: string
  title: string
  source_type: string
  archive_rating: number | null
  archive_reviews: number | null
  sets: unknown
}

export interface RawTrack {
  id: string
  title: string
  duration: number
  mp3_url: string
  track: number
}

export interface RawTrackSet {
  name: string
  tracks: RawTrack[]
}

export interface RawTracksDoc {
  sets: RawTrackSet[]
  lineage: string
  taper: string
  transferer: string
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
    this.name = 'ApiError'
  }
}

async function jsonFetch<T>(path: string): Promise<T> {
  let res: Response
  try {
    res = await fetch(path, { credentials: 'include' })
  } catch (e) {
    throw new ApiError(0, `Network error fetching ${path}: ${(e as Error).message}`)
  }
  if (!res.ok) {
    // Try to surface a server error blob if present, else status text.
    let detail = res.statusText
    try {
      const body = await res.json()
      if (body && typeof body === 'object' && 'error' in body) {
        detail = String((body as { error: unknown }).error)
      }
    } catch {
      /* body not JSON */
    }
    throw new ApiError(res.status, `${path} → ${res.status} ${detail}`)
  }
  return (await res.json()) as T
}

export function getYears(): Promise<RawYear[]> {
  return jsonFetch<RawYear[]>('/api/years')
}

export function getShowsForYear(year: number | string): Promise<RawShow[]> {
  return jsonFetch<RawShow[]>(`/api/years/${encodeURIComponent(String(year))}/shows`)
}

/**
 * There is no dedicated "get one show" endpoint — the classic UI derives the
 * show record from `sources` + `tracks` calls. We expose `getSources` +
 * `getTracks` and let the hook layer stitch them.
 */
export function getSources(date: string): Promise<RawSource[]> {
  return jsonFetch<RawSource[]>(`/api/shows/${encodeURIComponent(date)}/sources`)
}

export function getTracks(sourceId: string): Promise<RawTracksDoc> {
  return jsonFetch<RawTracksDoc>(`/api/sources/${encodeURIComponent(sourceId)}/tracks`)
}

export function getTodaysPick(): Promise<RawTodayPick[]> {
  return jsonFetch<RawTodayPick[]>('/api/today')
}

// Per-song gap/debut/drought stats. Server keys by NORMALIZED song title;
// `raw` gives us back the original so we can match to a track by title.
export interface RawSongStat {
  raw: string
  prev_date: string | null
  next_date: string | null
  gap_before: number | null
  perf_num: number
  total: number
  is_debut: boolean
  drought_rank: number | null
}

export interface SetlistStatsResp {
  rarity_score: number
  rarity_label: string
  songs: Record<string, RawSongStat>
}

export function getSetlistStats(showDate: string, songs: string[]): Promise<SetlistStatsResp> {
  return fetch(`/api/shows/${encodeURIComponent(showDate)}/setlist-stats`, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ songs }),
  }).then(async (r) => {
    if (!r.ok) throw new Error(`setlist-stats ${r.status}`)
    return r.json()
  })
}

export interface RawWeather {
  weather?: string
  temp_f?: number
  tempF?: number
}

export function getWeather(showDate: string): Promise<RawWeather> {
  return jsonFetch<RawWeather>(`/api/shows/${encodeURIComponent(showDate)}/weather`)
}

// ─── Releases & personal listens ─────────────────────────────────────────
export interface RawReleaseInfo {
  name: string
  year: number | null
}

/** Dead-map of show_date → primary official release. Small (~600–800 dates). */
export function getReleases(): Promise<Record<string, RawReleaseInfo>> {
  return jsonFetch<Record<string, RawReleaseInfo>>('/api/releases/all')
}

export interface RawListenRow {
  show_date?: string
  show_id?: string
  track_id?: string
  track_title?: string
  seconds?: number
  ts?: number
}

/**
 * Logged-in user's listen history — used only to mark previously-heard shows.
 * Backend endpoint is session-guarded; on 401 we surface an empty list rather
 * than throw so callers can render the logged-out state.
 */
export function getMyListens(): Promise<RawListenRow[]> {
  return fetch('/api/listens/history', { credentials: 'include' }).then(
    async (r) => {
      if (r.status === 401) return [] as RawListenRow[]
      if (!r.ok) throw new ApiError(r.status, `/api/listens/history → ${r.status}`)
      return (await r.json()) as RawListenRow[]
    },
  )
}
