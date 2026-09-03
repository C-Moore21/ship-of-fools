// Typed fetchers for Tour Runs (`/api/tours` + per-tour progress).
// Kept isolated from the shared data layer so this module owns its network.

export interface TourRun {
  id: string;
  name: string;
  start: string;   // "YYYY-MM-DD"
  end: string;     // "YYYY-MM-DD"
  /** Community avg show rating across the run, 0–5, or null if unrated. */
  momentum: number | null;
}

export interface TourEra {
  id: string;
  name: string;
  runs: TourRun[];
}

export interface ToursListResp {
  eras: TourEra[];
}

export interface TourShow {
  date: string;
  venue?: string;
  location?: string;
}

export interface TourCohortEntry {
  display_name: string;
  completed_count: number;
  last_show?: string;
}

export interface TourProgressResp {
  tour: { id: string; name: string; start: string; end: string };
  shows: TourShow[];
  /** Show dates the current user has listened to inside the run. */
  my_progress: string[];
  cohort: TourCohortEntry[];
}

async function jsonFetch<T>(url: string): Promise<T> {
  const res = await fetch(url, { credentials: 'same-origin' });
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const j = await res.json();
      if (j?.error) msg = j.error;
    } catch { /* ignore */ }
    throw new Error(msg);
  }
  return res.json() as Promise<T>;
}

export const toursApi = {
  list(): Promise<ToursListResp> {
    return jsonFetch<ToursListResp>('/api/tours');
  },
  progress(tourId: string): Promise<TourProgressResp> {
    return jsonFetch<TourProgressResp>(`/api/tours/${encodeURIComponent(tourId)}/progress`);
  },
};
