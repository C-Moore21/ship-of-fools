// React binding for the audio engine. The engine holds all mutable state
// (audio elements, MediaSession, AirPlay, crossfade timers) as module-level
// singletons — React only mirrors that state for rendering.

import { useCallback, useEffect, useState } from 'react';
import { getEngine } from './engine';
import { getTripMode, type TripState } from './TripMode';
import type { AudioShow, AudioSource, AudioTrack, PlaybackContext } from './types';

export interface SofAudio {
  /** Currently-loaded playback context, or null when nothing has ever played. */
  ctx: PlaybackContext | null;
  /** Sugar: `ctx.tracks[ctx.trackIdx]`. */
  track: AudioTrack | null;
  /** Sugar: `ctx.show`. */
  show: AudioShow | null;
  playing: boolean;
  volume: number;
  airplayActive: boolean;

  // Playback controls
  playShow: (show: AudioShow, source: AudioSource, tracks: AudioTrack[], idx?: number) => void;
  play: () => void;
  pause: () => void;
  toggle: () => void;
  next: () => void;
  prev: () => void;
  seek: (seconds: number) => void;
  setVolume: (v: number) => void;
  toggleMute: () => void;

  /** Navigate the host UI back to the show that is *actually playing*.
   *  The engine can't do this alone — it doesn't know how the host renders
   *  routes / lists. Host wires a resolver via `setSnapHandler`. */
  snapToPlayingShow: () => void;

  // Trip Mode
  trip: TripState;
  tripEnabled: boolean;
  toggleTrip: () => void;
}

// Host injects one function that knows how to navigate the UI back to a
// given show. This decouples the audio module from routing.
type SnapHandler = (ctx: PlaybackContext) => void;
let _snapHandler: SnapHandler | null = null;
export function setSnapHandler(fn: SnapHandler | null) {
  _snapHandler = fn;
}

/**
 * Time-only subscription. Split out because `timeupdate` fires ~4x/sec — any
 * component that reads `elapsed`/`duration` re-renders that often. PlayerBar
 * uses this; nothing else should. Everything else calls useSofAudio() which
 * intentionally does NOT surface time.
 */
export function useSofAudioTime(): { elapsed: number; duration: number } {
  const engine = getEngine();
  const [elapsed, setElapsed] = useState(() => engine.getCurrentTime());
  const [duration, setDuration] = useState(() => engine.getDuration());
  useEffect(() => {
    const off = engine.on((ev) => {
      if (ev.type === 'timeupdate') {
        setElapsed(ev.currentTime);
        setDuration(ev.duration);
      }
    });
    return off;
  }, [engine]);
  return { elapsed, duration };
}

export function useSofAudio(): SofAudio {
  const engine = getEngine();
  const trip = getTripMode(engine);

  const [ctx, setCtx] = useState<PlaybackContext | null>(() => engine.getContext());
  const [playing, setPlaying] = useState(() => engine.isPlaying());
  const [volume, setVolumeState] = useState(() => engine.getVolume());
  const [airplayActive, setAirplay] = useState(() => engine.isAirplayActive());
  const [tripState, setTripState] = useState<TripState>({ status: 'off' });

  useEffect(() => {
    const off = engine.on((ev) => {
      switch (ev.type) {
        case 'trackchange':
        case 'showchange':
          setCtx(ev.ctx);
          setPlaying(engine.isPlaying());
          break;
        case 'play':
          setPlaying(true);
          break;
        case 'pause':
        case 'ended':
          setPlaying(false);
          break;
        // NOTE: timeupdate is intentionally NOT handled here. It fires 4x/sec
        // and would re-render every consumer. Use useSofAudioTime() from the
        // one component (PlayerBar) that needs a live clock.
        case 'volume':
          setVolumeState(ev.volume);
          break;
        case 'airplay':
          setAirplay(ev.active);
          break;
      }
    });
    return off;
  }, [engine]);

  useEffect(() => {
    const off = trip.on((s) => setTripState(s));
    return off;
  }, [trip]);

  const playShow = useCallback(
    (show: AudioShow, source: AudioSource, tracks: AudioTrack[], idx = 0) =>
      engine.playShow(show, source, tracks, idx),
    [engine],
  );
  const play = useCallback(() => engine.play(), [engine]);
  const pause = useCallback(() => engine.pause(), [engine]);
  const toggle = useCallback(() => engine.toggle(), [engine]);
  const next = useCallback(() => engine.next(), [engine]);
  const prev = useCallback(() => engine.prev(), [engine]);
  const seek = useCallback((s: number) => engine.seek(s), [engine]);
  const setVolume = useCallback((v: number) => engine.setVolume(v), [engine]);
  const toggleMute = useCallback(() => engine.toggleMute(), [engine]);
  const toggleTrip = useCallback(() => trip.toggle(), [trip]);

  const snapToPlayingShow = useCallback(() => {
    const c = engine.getContext();
    if (!c) return;
    if (_snapHandler) _snapHandler(c);
  }, [engine]);

  const track =
    ctx && ctx.trackIdx >= 0 && ctx.trackIdx < ctx.tracks.length ? ctx.tracks[ctx.trackIdx] : null;

  return {
    ctx,
    track,
    show: ctx ? ctx.show : null,
    playing,
    volume,
    airplayActive,
    playShow,
    play,
    pause,
    toggle,
    next,
    prev,
    seek,
    setVolume,
    toggleMute,
    snapToPlayingShow,
    trip: tripState,
    tripEnabled: tripState.status !== 'off',
    toggleTrip,
  };
}
