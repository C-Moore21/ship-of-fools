// Thin fetchers for the section panels (Rated / Stats / History / Leaderboard).
// All endpoints require an authenticated session (except /api/leaderboard/controversial).
// Callers should send credentials so the Flask session cookie rides along.

async function getJSON<T>(url: string): Promise<T> {
  const res = await fetch(url, { credentials: 'include' });
  if (res.status === 401 || res.status === 403) {
    const err = new Error('unauthorized') as Error & { status?: number };
    err.status = res.status;
    throw err;
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json() as Promise<T>;
}

// ── Rated ────────────────────────────────────────────────────────────────────
export interface TrackRating {
  track_id: string;
  track_title: string;
  show_date: string;
  source_id: string;
  stars: number;
}

export interface ShowRating {
  show_id: string;
  venue?: string;
  stars: number;
}

export const fetchMyTrackRatings = () =>
  getJSON<TrackRating[]>('/api/ratings/mine');

export const fetchMyShowRatings = () =>
  getJSON<ShowRating[]>('/api/show-ratings/mine');

// ── History ──────────────────────────────────────────────────────────────────
export interface ListenRow {
  session_id?: string;
  track_id?: string;
  track_title?: string;
  show_id?: string;
  show_date?: string;
  source_id?: string;
  seconds: number;
  ts?: string;
  username?: string;
}

export const fetchListenHistory = () =>
  getJSON<ListenRow[]>('/api/listens/history');

// ── Stats ────────────────────────────────────────────────────────────────────
export interface StatsTopShow  { show_date: string; seconds: number; }
export interface StatsTopTrack { track_title: string; show_date: string; seconds: number; }
export interface StatsTopSong  { title: string; norm: string; seconds: number; show_count: number; }
export interface StatsEraRow   { era: string; seconds: number; }
export interface StatsCalDay   { date: string; count: number; }
export interface StatsResponse {
  total_seconds: number;
  total_listens: number;
  top_shows:  StatsTopShow[];
  top_tracks: StatsTopTrack[];
  top_songs:  StatsTopSong[];
  years:      string[];
  year:       string;
  by_era:     StatsEraRow[];
  streak:     number;
  cal_data:   StatsCalDay[];
}

export const fetchListenStats = (year?: string) => {
  const qs = year ? `?year=${encodeURIComponent(year)}` : '';
  return getJSON<StatsResponse>(`/api/listens/stats${qs}`);
};

// ── Leaderboard ──────────────────────────────────────────────────────────────
export type LeaderboardPeriod = 'week' | 'month' | 'all';

export interface LeaderboardRow {
  username: string;
  display_name: string;
  seconds: number;
  shows: number;
  streak: number;
}

export interface ControversialRow {
  show_id: string;
  venue?: string;
  avg: number;
  count: number;
  stddev: number;
}

export const fetchLeaderboard = (period: LeaderboardPeriod) =>
  getJSON<LeaderboardRow[]>(`/api/leaderboard?period=${period}`);

export const fetchControversials = () =>
  getJSON<ControversialRow[]>('/api/leaderboard/controversial');
