import { useCallback, useEffect, useState } from 'react';
import { getMyRating, saveRating, getShowAvgRating, ShowAvg } from './api';

export interface UseShowRating {
  stars: number;                // user's current rating (0 if none)
  loading: boolean;
  saving: boolean;
  error: string | null;
  setStars: (next: number) => Promise<void>;
  avg: ShowAvg | null;
}

/**
 * Per-show rating hook. Half-stars (0.5 step) are supported end-to-end
 * (backend `round(stars * 2) / 2` normalization). Setting `stars` to 0
 * deletes the rating.
 */
export function useShowRating(
  showDate: string | null | undefined,
  loggedIn: boolean,
  venue = '',
): UseShowRating {
  const [stars, setStarsState] = useState(0);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [avg, setAvg] = useState<ShowAvg | null>(null);

  useEffect(() => {
    let cancelled = false;
    setStarsState(0);
    setAvg(null);
    setError(null);
    if (!showDate) return;
    // Community avg is public — try regardless of login.
    getShowAvgRating(showDate).then((v) => {
      if (!cancelled) setAvg(v);
    });
    if (!loggedIn) return;
    setLoading(true);
    getMyRating(showDate)
      .then((v) => {
        if (!cancelled) setStarsState(v);
      })
      .catch((e) => {
        if (!cancelled) setError(String(e?.message || e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [showDate, loggedIn]);

  const setStars = useCallback(
    async (next: number) => {
      if (!showDate || !loggedIn) return;
      const clamped = Math.max(0, Math.min(5, Math.round(next * 2) / 2));
      const prev = stars;
      // Click-same-star = clear (matches classic UI behavior)
      const target = clamped === prev ? 0 : clamped;
      setStarsState(target);
      setSaving(true);
      setError(null);
      try {
        const confirmed = await saveRating(showDate, target, venue);
        setStarsState(confirmed);
      } catch (e: any) {
        setStarsState(prev);
        setError(String(e?.message || e));
      } finally {
        setSaving(false);
      }
    },
    [showDate, loggedIn, stars, venue],
  );

  return { stars, loading, saving, error, setStars, avg };
}
