import { useCallback, useEffect, useState } from 'react';
import {
  getMyTrackRatings,
  saveTrackRating,
  type TrackRatingMeta,
} from './api';

export interface UseTrackRatings {
  ratings: Record<string, number>;
  loading: boolean;
  error: string | null;
  setRating: (trackId: string, stars: number, meta?: TrackRatingMeta) => void;
}

/**
 * Per-track rating hook. Loads all of the current user's ratings for a given
 * source_id in one request, then optimistically applies clicks. Clicking the
 * same star value the track already has clears the rating (matches classic UI).
 *
 * `meta` (track title + show date) is optional but recommended — the backend
 * stores them so `/api/ratings/mine` can display them without a re-lookup.
 */
export function useTrackRatings(
  sourceId: string | null | undefined,
  loggedIn: boolean,
): UseTrackRatings {
  const [ratings, setRatings] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setRatings({});
    setError(null);
    if (!sourceId || !loggedIn) return;
    let cancelled = false;
    setLoading(true);
    getMyTrackRatings(sourceId)
      .then((r) => {
        if (!cancelled) setRatings(r);
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
  }, [sourceId, loggedIn]);

  const setRating = useCallback(
    (trackId: string, stars: number, meta?: TrackRatingMeta) => {
      if (!sourceId || !loggedIn || !trackId) return;
      setRatings((prev) => {
        const existing = prev[trackId] || 0;
        const clamped = Math.max(0, Math.min(5, Math.round(stars * 2) / 2));
        const target = clamped === existing ? 0 : clamped;
        const next = { ...prev };
        if (target <= 0) delete next[trackId];
        else next[trackId] = target;
        // Fire-and-forget with rollback on failure.
        saveTrackRating(sourceId, trackId, target, meta).catch((e) => {
          setError(String(e?.message || e));
          setRatings((cur) => {
            const rollback = { ...cur };
            if (existing > 0) rollback[trackId] = existing;
            else delete rollback[trackId];
            return rollback;
          });
        });
        return next;
      });
    },
    [sourceId, loggedIn],
  );

  return { ratings, loading, error, setRating };
}
