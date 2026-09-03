// Thin fetchers for the modal features. Deliberately isolated from the
// larger data layer (useSofData) so this module owns its own network calls.

export interface TodayRecording {
  id: string; // show date "1977-05-08"
  identifier: string; // archive.org identifier
  display_date: string;
  venue: { name?: string; location?: string };
  source_type: string;
  avg_rating: number | null;
  num_reviews: number;
  score: number;
  community_listens?: number;
}

/** GET /api/today — every recording from a show that happened on today's month/day. */
export async function fetchTodayInHistory(): Promise<TodayRecording[]> {
  const res = await fetch('/api/today', { credentials: 'include' });
  if (!res.ok) throw new Error(`today ${res.status}`);
  const data = await res.json();
  if (!Array.isArray(data)) return [];
  return data as TodayRecording[];
}

/** Recordings grouped by show date, group order preserving score-sort. */
export interface TodayGroup {
  showId: string;
  year: string;
  best: TodayRecording;
  recs: TodayRecording[];
}
export function groupTodayByShow(recs: TodayRecording[]): TodayGroup[] {
  const map = new Map<string, TodayRecording[]>();
  const order: string[] = [];
  for (const r of recs) {
    if (!map.has(r.id)) {
      map.set(r.id, []);
      order.push(r.id);
    }
    map.get(r.id)!.push(r);
  }
  return order.map((showId) => {
    const list = map.get(showId)!;
    return {
      showId,
      year: showId.slice(0, 4),
      best: list[0],
      recs: list,
    };
  });
}

export interface BlindTrack {
  track_url: string;
}
export interface BlindReveal {
  show_date: string;
  year: string;
  venue: string;
  track_title: string;
  identifier: string;
}

/** GET /api/blindtest — server picks a random show + track, stashes reveal in session. */
export async function fetchBlindTrack(): Promise<BlindTrack> {
  const res = await fetch('/api/blindtest', { credentials: 'include' });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `blindtest ${res.status}`);
  }
  return res.json();
}

/** GET /api/blindtest/reveal — returns the metadata for the last-fetched blind track. */
export async function fetchBlindReveal(): Promise<BlindReveal> {
  const res = await fetch('/api/blindtest/reveal', { credentials: 'include' });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `reveal ${res.status}`);
  }
  return res.json();
}
