// Data hooks for the Observatory. Simple module-scoped caches (one per key)
// keep repeat tab switches instant without pulling in a state library.

import { useEffect, useRef, useState } from 'react';
import {
  getHeatmap,
  getScatter,
  getShowsMap,
  getUsStates,
  HeatmapResponse,
  ScatterResponse,
  ShowsMapResponse,
  GeoJSONFeatureCollection,
} from './api';

interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
}

// ── Module-level caches (shared across mounts) ────────────────────────────
let heatmapCache: HeatmapResponse | null = null;
const scatterCache = new Map<string, ScatterResponse>();
let showsMapCache: ShowsMapResponse | null = null;
let usStatesCache: GeoJSONFeatureCollection | null = null;

function useAsync<T>(
  key: string,
  fn: () => Promise<T>,
  cacheRead: () => T | null,
  cacheWrite: (v: T) => void,
): AsyncState<T> {
  const [data, setData] = useState<T | null>(cacheRead());
  const [loading, setLoading] = useState<boolean>(!cacheRead());
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  useEffect(() => {
    const cached = cacheRead();
    if (cached && tick === 0) {
      setData(cached);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    fn()
      .then(v => {
        if (!mounted.current) return;
        cacheWrite(v);
        setData(v);
        setLoading(false);
      })
      .catch(e => {
        if (!mounted.current) return;
        setError(e?.message || String(e));
        setLoading(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, tick]);

  return { data, loading, error, reload: () => setTick(t => t + 1) };
}

export function useHeatmap() {
  return useAsync<HeatmapResponse>(
    'heatmap',
    getHeatmap,
    () => heatmapCache,
    v => { heatmapCache = v; },
  );
}

export function useScatter(songId: string | null) {
  const key = songId || '';
  return useAsync<ScatterResponse>(
    `scatter:${key}`,
    () => getScatter(key),
    () => (key ? scatterCache.get(key) || null : null),
    v => { if (key) scatterCache.set(key, v); },
  );
}

export function useShowsMap() {
  return useAsync<ShowsMapResponse>(
    'shows-map',
    getShowsMap,
    () => showsMapCache,
    v => { showsMapCache = v; },
  );
}

export function useUsStates() {
  return useAsync<GeoJSONFeatureCollection>(
    'us-states',
    getUsStates,
    () => usStatesCache,
    v => { usStatesCache = v; },
  );
}
