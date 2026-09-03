import { useCallback, useEffect, useState } from 'react';
import {
  fetchControversials,
  fetchLeaderboard,
  fetchListenHistory,
  fetchListenStats,
  fetchMyShowRatings,
  fetchMyTrackRatings,
  type ControversialRow,
  type LeaderboardPeriod,
  type LeaderboardRow,
  type ListenRow,
  type ShowRating,
  type StatsResponse,
  type TrackRating,
} from './api';

export interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
}

function useAsync<T>(load: () => Promise<T>, deps: unknown[]): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    load()
      .then((v) => { if (!cancelled) setData(v); })
      .catch((e: Error & { status?: number }) => {
        if (cancelled) return;
        setError(e.status === 401 || e.status === 403 ? 'unauthorized' : e.message);
      })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  const reload = useCallback(() => setNonce((n) => n + 1), []);
  return { data, loading, error, reload };
}

export interface RatedData {
  trackRatings: TrackRating[];
  showRatings:  ShowRating[];
}

export function useRatedData(loggedIn: boolean): AsyncState<RatedData> {
  return useAsync<RatedData>(async () => {
    if (!loggedIn) {
      const err = new Error('unauthorized') as Error & { status?: number };
      err.status = 401;
      throw err;
    }
    const [trackRatings, showRatings] = await Promise.all([
      fetchMyTrackRatings(),
      fetchMyShowRatings(),
    ]);
    return { trackRatings, showRatings };
  }, [loggedIn]);
}

export function useStatsData(loggedIn: boolean, year?: string): AsyncState<StatsResponse> {
  return useAsync<StatsResponse>(async () => {
    if (!loggedIn) {
      const err = new Error('unauthorized') as Error & { status?: number };
      err.status = 401;
      throw err;
    }
    return fetchListenStats(year);
  }, [loggedIn, year ?? '']);
}

export function useHistoryData(loggedIn: boolean): AsyncState<ListenRow[]> {
  return useAsync<ListenRow[]>(async () => {
    if (!loggedIn) {
      const err = new Error('unauthorized') as Error & { status?: number };
      err.status = 401;
      throw err;
    }
    return fetchListenHistory();
  }, [loggedIn]);
}

export interface LeaderboardData {
  rows: LeaderboardRow[];
  controversials: ControversialRow[];
}

export function useLeaderboardData(
  loggedIn: boolean,
  period: LeaderboardPeriod,
): AsyncState<LeaderboardData> {
  return useAsync<LeaderboardData>(async () => {
    if (!loggedIn) {
      // Controversials are public; still return them so anon users see something.
      const controversials = await fetchControversials().catch(() => []);
      return { rows: [], controversials };
    }
    const [rows, controversials] = await Promise.all([
      fetchLeaderboard(period),
      fetchControversials().catch(() => [] as ControversialRow[]),
    ]);
    return { rows, controversials };
  }, [loggedIn, period]);
}
