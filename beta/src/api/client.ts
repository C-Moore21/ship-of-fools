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
  avg_rating: number | null
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
