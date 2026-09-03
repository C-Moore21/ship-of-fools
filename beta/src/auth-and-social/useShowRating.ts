import { useCallback, useEffect, useState } from 'react';
import { getMyRating, saveRating, getShowAvgRating, ShowAvg } from './api';

// Module-level per-show caches. Ratings/avgs are cheap but hitting the
// endpoints for every show flip adds up; cache the resolved values (not the
// promises — they can be mutated by setStars).
const myRatingCache = new Map<string, number>();
const avgRatingCache = new Map<string, ShowAvg | null>();
const inflightMyRating = new Map<string, Promise<number>>();
const inflightAvgRating = new Map<string, Promise<ShowAvg | null>>();

/** Blow away user-scoped caches on login/logout transitions. */
export function invalidateShowRatingCache(): void {
  myRatingCache.clear();
  inflightMyRating.clear();
}

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
    // Seed from cache if we've fetched this show before — no network flicker.
    const cachedAvg = showDate ? avgRatingCache.get(showDate) : undefined;
    const cachedMy = showDate ? myRatingCache.get(showDate) : undefined;
    setStarsState(cachedMy ?? 0);
    setAvg(cachedAvg ?? null);
    setError(null);
    if (!showDate) return;
    // Community avg is public — try regardless of login. Dedupe in-flight.
    if (cachedAvg === undefined) {
      let p = inflightAvgRating.get(showDate);
      if (!p) {
        p = getShowAvgRating(showDate).then((v) => {
          avgRatingCache.set(showDate, v);
          inflightAvgRating.delete(showDate);
          return v;
        }).catch(() => {
          inflightAvgRating.delete(showDate);
          return null;
        });
        inflightAvgRating.set(showDate, p);
      }
      p.then((v) => { if (!cancelled) setAvg(v); });
    }
    if (!loggedIn) return;
    if (cachedMy !== undefined) return; // Already have it — no request.
    setLoading(true);
    let mine = inflightMyRating.get(showDate);
    if (!mine) {
      mine = getMyRating(showDate).then((v) => {
        myRatingCache.set(showDate, v);
        inflightMyRating.delete(showDate);
        return v;
      }).catch((e) => {
        inflightMyRating.delete(showDate);
        throw e;
      });
      inflightMyRating.set(showDate, mine);
    }
    mine
      .then((v) => { if (!cancelled) setStarsState(v); })
      .catch((e) => { if (!cancelled) setError(String(e?.message || e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
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
        // Stale-while-revalidate: keep the module cache in sync so a re-select
        // of this show doesn't refetch and flash the old value.
        myRatingCache.set(showDate, confirmed);
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
