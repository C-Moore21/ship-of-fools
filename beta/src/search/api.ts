// api.ts — typed wrapper for /api/search.
//
// The backend (app.py `/api/search`) currently returns a flat list of shows.
// Each result has: { id, display_date, venue: { name, location } }. The `id`
// is the ISO show date (YYYY-MM-DD) used for jump-to navigation.
//
// The public shape below models that response, plus a `kind` discriminator so
// callers can group by category. Today only `kind: 'show'` is emitted; the
// discriminator leaves room for the API to grow songs / venues later without
// breaking consumers.

export interface SearchVenue {
  name: string
  location: string
}

export interface ShowResult {
  kind: 'show'
  id: string            // YYYY-MM-DD, same as display_date
  display_date: string
  venue: SearchVenue
}

export interface SongResult {
  kind: 'song'
  id: string
  label: string
}

export interface VenueResult {
  kind: 'venue'
  id: string
  name: string
  location: string
}

export type SearchResult = ShowResult | SongResult | VenueResult

interface RawShow {
  id?: string
  display_date?: string
  venue?: Partial<SearchVenue> | null
}

interface RawSong {
  id?: string
  label?: string
}

interface RawVenue {
  id?: string
  name?: string
  location?: string
}

// The API may (now or in the future) return either a bare array of shows or
// a categorized object. Normalize both into a flat SearchResult[].
type RawPayload =
  | RawShow[]
  | {
      shows?: RawShow[]
      songs?: RawSong[]
      venues?: RawVenue[]
    }
  | { error?: string }

function normalize(payload: RawPayload): SearchResult[] {
  if (!payload) return []
  if (Array.isArray(payload)) {
    return payload
      .filter((s) => s && (s.id || s.display_date))
      .map<ShowResult>((s) => ({
        kind: 'show',
        id: String(s.id ?? s.display_date ?? ''),
        display_date: String(s.display_date ?? s.id ?? ''),
        venue: {
          name: s.venue?.name ?? '',
          location: s.venue?.location ?? '',
        },
      }))
  }
  const out: SearchResult[] = []
  const obj = payload as {
    shows?: RawShow[]
    songs?: RawSong[]
    venues?: RawVenue[]
  }
  for (const s of obj.shows ?? []) {
    if (!s || !(s.id || s.display_date)) continue
    out.push({
      kind: 'show',
      id: String(s.id ?? s.display_date ?? ''),
      display_date: String(s.display_date ?? s.id ?? ''),
      venue: {
        name: s.venue?.name ?? '',
        location: s.venue?.location ?? '',
      },
    })
  }
  for (const s of obj.songs ?? []) {
    if (!s?.id) continue
    out.push({ kind: 'song', id: String(s.id), label: String(s.label ?? s.id) })
  }
  for (const v of obj.venues ?? []) {
    if (!v?.id && !v?.name) continue
    out.push({
      kind: 'venue',
      id: String(v.id ?? v.name ?? ''),
      name: String(v.name ?? ''),
      location: String(v.location ?? ''),
    })
  }
  return out
}

export async function searchAll(
  query: string,
  signal?: AbortSignal,
): Promise<SearchResult[]> {
  const q = query.trim()
  if (q.length < 2) return []
  const res = await fetch(`/api/search?q=${encodeURIComponent(q)}`, {
    credentials: 'include',
    signal,
  })
  if (!res.ok) {
    // Try to surface the server's error message; fall back to status.
    let msg = `Search failed (${res.status})`
    try {
      const body = (await res.json()) as { error?: string }
      if (body?.error) msg = body.error
    } catch {
      /* ignore */
    }
    throw new Error(msg)
  }
  const payload = (await res.json()) as RawPayload
  if (payload && typeof payload === 'object' && !Array.isArray(payload) && 'error' in payload) {
    throw new Error(String((payload as { error?: string }).error ?? 'Search failed'))
  }
  return normalize(payload)
}
