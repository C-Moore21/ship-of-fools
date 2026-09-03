// Audio-engine types. Deliberately independent of ../types/archive so this
// module can be dropped into any host UI. The host maps its own Show/Track
// shapes onto these before calling engine.playShow(...).

export interface AudioTrack {
  /** Stable id — usually the Archive.org file name (e.g. "gd77-05-08d1t04.mp3"). */
  id: string;
  title: string;
  /** Seconds, may be 0 if unknown — engine will use audio element's own duration. */
  duration: number;
  mp3_url: string;
  /** True when the title ends in ">" or "→" — signals continuous playing into
   *  next track. Crossfade is suppressed for these to avoid double-tracking. */
  segue?: boolean;
}

export interface AudioSource {
  /** Archive.org identifier (e.g. "gd1977-05-08.sbd.hicks.4982.sbeok.shnf"). */
  id: string;
  /** SBD / AUD / MTX / FM / UNK — used by Trip Mode to prefer same type. */
  source_type?: string;
  title?: string;
}

export interface AudioShow {
  /** ISO show date, e.g. "1977-05-08". Doubles as the show id. */
  date: string;
  /** Rendered "1977-05-08 · Barton Hall" or similar for the player bar. */
  label: string;
  venue?: { name?: string; city?: string; state?: string };
}

/**
 * A snapshot of what is *actually playing* — held by the engine so that the
 * host UI can navigate freely (browse other shows) without hijacking the
 * audio's next-track / crossfade / handoff / listen-attribution logic. The
 * classic UI calls these `_pb*` (playback context). Divergent from "view
 * context" (what the user is looking at) is the whole point.
 */
export interface PlaybackContext {
  show: AudioShow;
  source: AudioSource;
  tracks: AudioTrack[];
  trackIdx: number;
}

export type AudioEvent =
  | { type: 'trackchange'; ctx: PlaybackContext | null }
  | { type: 'showchange'; ctx: PlaybackContext | null }
  | { type: 'play' }
  | { type: 'pause' }
  | { type: 'ended' }
  | { type: 'timeupdate'; currentTime: number; duration: number }
  | { type: 'volume'; volume: number }
  | { type: 'airplay'; active: boolean };

export type AudioEventListener = (ev: AudioEvent) => void;
