// Typed fetch wrappers for the Observatory backend.
// All requests use credentials:'include' so session cookies flow through.

export interface YearBucket {
  year: number | string;
  avg_rating: number;
  reviews: number;
  count: number;
}

export interface HeatmapSong {
  song_id: string;
  label: string;
  years: YearBucket[];
}

export interface HeatmapResponse {
  songs: HeatmapSong[];
  all_songs: { id: string; label: string }[];
}

export interface Performance {
  date: string;
  duration: number;
  source: 'SBD' | 'AUD' | 'MTX' | 'FOB' | 'UNK' | string;
  reviews: number;
  rating: number;
  id: string;
}

export interface ScatterResponse {
  song: string;
  song_id: string;
  songs: { id: string; label: string }[];
  performances: Performance[];
}

export interface MapShow {
  date: string;
  lat: number;
  lng: number;
  venue?: string;
  location?: string;
  rating?: number;
}

export interface ShowsMapResponse {
  shows: MapShow[];
  pending?: boolean;
}

export interface GeoJSONFeatureCollection {
  type: 'FeatureCollection';
  features: Array<{
    type: 'Feature';
    properties?: Record<string, unknown>;
    geometry: {
      type: 'Polygon' | 'MultiPolygon' | string;
      coordinates: unknown;
    } | null;
  }>;
}

const BASE = ''; // same-origin

async function j<T>(path: string): Promise<T> {
  const res = await fetch(BASE + path, { credentials: 'include' });
  if (!res.ok) {
    let msg = res.statusText;
    try {
      const b = await res.json();
      if (b?.error) msg = b.error;
    } catch {
      /* ignore */
    }
    throw new Error(msg || `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function getHeatmap(): Promise<HeatmapResponse> {
  return j<HeatmapResponse>('/api/observatory/heatmap');
}

export function getScatter(songId: string): Promise<ScatterResponse> {
  return j<ScatterResponse>(`/api/observatory?song=${encodeURIComponent(songId)}`);
}

export async function getShowsMap(): Promise<ShowsMapResponse> {
  const raw = await j<ShowsMapResponse | MapShow[]>('/api/shows/map');
  if (Array.isArray(raw)) return { shows: raw };
  return raw;
}

export function getUsStates(): Promise<GeoJSONFeatureCollection> {
  return j<GeoJSONFeatureCollection>('/api/map/us-states');
}
