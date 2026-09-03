// Trip Mode — auto-advance across tour-run shows, capped at 3 shows per
// session. Ported from templates/index.html (_tripMode / tripResolveNext /
// tripMaybeAdvancePlayingShow / tripAdvanceToNextShow).
//
// This module is separate from engine.ts because Trip navigates BETWEEN
// shows, which means fetching sources + tracks for the next night — that's
// data-layer work, not audio-layer work. The engine only knows about a
// single show at a time.

import type { AudioEngine } from './engine';
import { fetchSourceTracks } from './engine';
import type { AudioShow, AudioSource } from './types';

const TRIP_MAX_ADVANCES = 2; // 2 auto-advances after the seed show = 3 total.
const RUN_WINDOW_DAYS = 7;

export interface TripDest {
  date: string;
  venue: { name?: string; city?: string; state?: string };
  sourceId: string;
  sourceType: string;
  firstTrackUrl: string | null;
  firstTrackTitle: string | null;
}

export type TripState =
  | { status: 'off' }
  | { status: 'resolving'; seedDate: string }
  | { status: 'ready'; dest: TripDest }
  | { status: 'end-of-run'; seedDate: string }
  | { status: 'capped' };

export type TripListener = (state: TripState) => void;

/**
 * Pure resolver — no globals, no engine mutation. Given a seed date + the
 * user's preferred source type, return the next-night show in the same tour
 * run (within RUN_WINDOW_DAYS) or null.
 *
 * Uses backend endpoints already served by app.py:
 *   /api/shows/<date>/tour        -> [{id, venue, ...}, ...]
 *   /api/shows/<date>/sources     -> [{id, source_type, ...}, ...]
 *   /api/sources/<id>/tracks      -> {sets:[{tracks:[...]}], ...}
 */
export async function resolveNextTripShow(
  seedDate: string,
  preferredSourceType: string,
): Promise<TripDest | null> {
  try {
    const tourRes = await fetch(`/api/shows/${seedDate}/tour`);
    if (!tourRes.ok) return null;
    const tour = await tourRes.json();
    if (!Array.isArray(tour)) return null;
    const later = tour
      .filter((s: any) => s.id && s.id > seedDate)
      .sort((a: any, b: any) => String(a.id).localeCompare(String(b.id)));
    if (!later.length) return null;
    const next = later[0];
    const dCur = new Date(seedDate);
    const dNxt = new Date(next.id);
    if ((dNxt.getTime() - dCur.getTime()) / 86400000 > RUN_WINDOW_DAYS) return null;
    const srcRes = await fetch(`/api/shows/${next.id}/sources`);
    if (!srcRes.ok) return null;
    const sources = await srcRes.json();
    if (!Array.isArray(sources) || !sources.length) return null;
    // Prefer same source type; fall back to first available.
    let pickIdx = sources.findIndex((s: any) => s.source_type === preferredSourceType);
    if (pickIdx < 0) pickIdx = 0;
    const pickSrc = sources[pickIdx];
    // Grab first track's URL for HTTP-cache warming (no second AVPlayer!).
    let firstTrackUrl: string | null = null;
    let firstTrackTitle: string | null = null;
    try {
      const tracks = await fetchSourceTracks(pickSrc.id);
      if (tracks.length) {
        firstTrackUrl = tracks[0].mp3_url || null;
        firstTrackTitle = tracks[0].title || null;
      }
    } catch (e) {}
    return {
      date: next.id,
      venue: next.venue || {},
      sourceId: String(pickSrc.id),
      sourceType: pickSrc.source_type || 'UNK',
      firstTrackUrl,
      firstTrackTitle,
    };
  } catch (e) {
    return null;
  }
}

/**
 * TripMode manager. Wires itself to the engine's `ended` event, resolves the
 * next-night show against the PLAYING show's date (not whatever the user has
 * browsed to), and drives the engine to keep playing.
 */
export class TripMode {
  private engine: AudioEngine;
  private enabled = false;
  private advanceCount = 0;
  private inProgress = false;
  private nextDest: TripDest | null = null;
  private currentSeed: string | null = null;
  private currentSourceType: string = 'UNK';
  private listeners = new Set<TripListener>();
  private unsubEnded: (() => void) | null = null;

  constructor(engine: AudioEngine) {
    this.engine = engine;
    this.enabled = this.readPersisted();
    this.unsubEnded = engine.on((ev) => {
      if (ev.type === 'ended') this.onShowEnded();
      if (ev.type === 'showchange') this.onShowChanged();
    });
  }

  destroy() {
    if (this.unsubEnded) this.unsubEnded();
    this.listeners.clear();
  }

  isEnabled() {
    return this.enabled;
  }

  toggle() {
    this.enabled = !this.enabled;
    this.advanceCount = 0;
    this.writePersisted();
    if (this.enabled) {
      const ctx = this.engine.getContext();
      if (ctx) this.resolveNext(ctx.show.date, ctx.source.source_type || 'UNK');
      else this.emit({ status: 'off' });
    } else {
      this.nextDest = null;
      this.emit({ status: 'off' });
    }
  }

  on(fn: TripListener): () => void {
    this.listeners.add(fn);
    // Fire once with current state so subscribers can render immediately.
    fn(this.currentState());
    return () => this.listeners.delete(fn);
  }

  private currentState(): TripState {
    if (!this.enabled) return { status: 'off' };
    if (this.nextDest) return { status: 'ready', dest: this.nextDest };
    if (this.currentSeed) return { status: 'resolving', seedDate: this.currentSeed };
    return { status: 'off' };
  }

  private emit(state: TripState) {
    this.listeners.forEach((fn) => {
      try {
        fn(state);
      } catch (e) {}
    });
  }

  private onShowChanged() {
    // Re-resolve destination whenever the engine's playing show changes.
    if (!this.enabled) return;
    const ctx = this.engine.getContext();
    if (!ctx) return;
    this.resolveNext(ctx.show.date, ctx.source.source_type || 'UNK');
  }

  private async resolveNext(seedDate: string, sourceType: string) {
    this.currentSeed = seedDate;
    this.currentSourceType = sourceType;
    this.nextDest = null;
    this.emit({ status: 'resolving', seedDate });
    const dest = await resolveNextTripShow(seedDate, sourceType);
    // Guard against staleness — user may have moved on while we fetched.
    if (this.currentSeed !== seedDate) return;
    if (dest) {
      this.nextDest = dest;
      // Warm HTTP cache for the next show's first track. NEVER instantiate a
      // second AVPlayer — the engine's preload path knows this too.
      if (dest.firstTrackUrl) {
        fetch(dest.firstTrackUrl, {
          headers: { Range: 'bytes=0-1048575' },
          mode: 'cors',
          credentials: 'omit',
        }).catch(() => {});
      }
      this.emit({ status: 'ready', dest });
    } else {
      this.emit({ status: 'end-of-run', seedDate });
    }
  }

  private async onShowEnded() {
    if (!this.enabled || this.inProgress) return;
    if (this.advanceCount >= TRIP_MAX_ADVANCES) {
      // Hit the 3-show cap. Turn Trip off; host may re-enable.
      this.enabled = false;
      this.nextDest = null;
      this.advanceCount = 0;
      this.writePersisted();
      this.emit({ status: 'capped' });
      return;
    }
    // Re-resolve against the PLAYING show's date so browsing away doesn't
    // redirect Trip through the wrong tour run.
    const ctx = this.engine.getContext();
    const seed = ctx ? ctx.show.date : this.currentSeed;
    const srcType = ctx ? ctx.source.source_type || 'UNK' : this.currentSourceType;
    if (!seed) return;
    let dest = this.nextDest;
    if (!dest || (ctx && dest.date <= seed)) {
      dest = await resolveNextTripShow(seed, srcType);
    }
    if (!dest) {
      this.emit({ status: 'end-of-run', seedDate: seed });
      return;
    }
    await this.advanceTo(dest);
  }

  /**
   * Actually load the next show and start playback. The host provides the
   * show-shape adapter — Trip only knows date/venue/sourceId, not the host
   * UI's Show type.
   */
  private async advanceTo(dest: TripDest) {
    this.inProgress = true;
    try {
      const tracks = await fetchSourceTracks(dest.sourceId);
      if (!tracks.length) return;
      const show: AudioShow = {
        date: dest.date,
        label: dest.venue.name ? `${dest.date} · ${dest.venue.name}` : dest.date,
        venue: dest.venue,
      };
      const source: AudioSource = {
        id: dest.sourceId,
        source_type: dest.sourceType,
      };
      this.engine.playShow(show, source, tracks, 0);
      this.advanceCount++;
      this.nextDest = null;
    } finally {
      this.inProgress = false;
    }
  }

  private readPersisted(): boolean {
    try {
      return localStorage.getItem('sof_trip_mode') === '1';
    } catch (e) {
      return false;
    }
  }
  private writePersisted() {
    try {
      localStorage.setItem('sof_trip_mode', this.enabled ? '1' : '0');
    } catch (e) {}
  }
}

// Singleton — one Trip per engine.
let _trip: TripMode | null = null;
export function getTripMode(engine: AudioEngine): TripMode {
  if (!_trip) _trip = new TripMode(engine);
  return _trip;
}
