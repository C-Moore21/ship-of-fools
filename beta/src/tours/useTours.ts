// Data hooks for the Tour Runs modal. Two levels:
//   useTours()             — the era → runs list (loaded once per open)
//   useTourProgress(id)    — details for a specific run
// Both are lazy: they don't fetch until `enabled` is true so the parent can
// tie loading to the modal's open state.
import { useCallback, useEffect, useState } from 'react';
import { toursApi, type TourEra, type TourProgressResp } from './api';

export interface UseToursReturn {
  eras: TourEra[] | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
}

export function useTours(enabled: boolean): UseToursReturn {
  const [eras, setEras] = useState<TourEra[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    toursApi.list()
      .then(d => { if (!cancelled) setEras(d.eras || []); })
      .catch(e => { if (!cancelled) setError(e?.message || String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [enabled, nonce]);

  const reload = useCallback(() => setNonce(n => n + 1), []);
  return { eras, loading, error, reload };
}

export interface UseTourProgressReturn {
  data: TourProgressResp | null;
  loading: boolean;
  error: string | null;
}

export function useTourProgress(tourId: string | null): UseTourProgressReturn {
  const [data, setData] = useState<TourProgressResp | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!tourId) { setData(null); setError(null); return; }
    let cancelled = false;
    setLoading(true);
    setError(null);
    setData(null);
    toursApi.progress(tourId)
      .then(d => { if (!cancelled) setData(d); })
      .catch(e => { if (!cancelled) setError(e?.message || String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [tourId]);

  return { data, loading, error };
}
