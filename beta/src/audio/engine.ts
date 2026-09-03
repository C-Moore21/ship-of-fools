// Ship of Fools — audio engine (React-agnostic).
//
// Ported from templates/index.html. Preserves the classic UI's hard-earned
// invariants — do NOT hardcode audio.volume = 1 at any call site; do NOT call
// audio.load() while AirPlay is connected; set MediaSession metadata BEFORE
// play() resolves; keep the preload element torn down while casting (a second
// AVPlayer breaks routing per WebKit bug #145959).

import type {
  AudioEvent,
  AudioEventListener,
  AudioShow,
  AudioSource,
  AudioTrack,
  PlaybackContext,
} from './types';

const FADE_DURATION_MS = 1200;
const FADE_TRIGGER_S = 1.4;
const HANDOFF_LEAD_S = 0.6;
const PRELOAD_75 = 0.75;

// ── Resume snapshot ────────────────────────────────────────────────────
// Persisted to localStorage so a reload can offer "▶ Resume …". Bump
// RESUME_VERSION whenever the shape changes so stale rows get discarded.
const RESUME_KEY = 'sof_resume';
const RESUME_VERSION = 1;
const RESUME_WRITE_DEBOUNCE_MS = 500;
const RESUME_MIN_ELAPSED_S = 5;

export interface ResumeSnapshot {
  v: number;
  show: AudioShow;
  source: AudioSource;
  trackCount: number;
  trackIdx: number;
  elapsed: number;
  savedAt: number;
}

const isIOSPlatform = () =>
  /iPad|iPhone|iPod/.test(navigator.userAgent) ||
  ((navigator as any).platform === 'MacIntel' && (navigator as any).maxTouchPoints > 1);

const isSegueTrack = (t: AudioTrack | undefined | null): boolean => {
  if (!t?.title) return false;
  return /[>→]\s*$/.test(String(t.title).trim());
};

export class AudioEngine {
  // Two audio elements: `audio` is the active AVPlayer, `preload` buffers the
  // next track and is kept muted. They swap roles on gapless handoff/splice.
  private audio: HTMLAudioElement;
  private preload: HTMLAudioElement;
  private preloadParent: Node | null = null;
  private preloadNextSibling: Node | null = null;

  // Playback context — what's actually on audio.src. Snapshotted at every
  // playTrack call; onEnded/handoff/crossfade read from here, NOT from any
  // "current view" state the host may be tracking.
  private pb: PlaybackContext | null = null;

  private userVolume = 1;
  private lastVolume = 1; // for unmute restore

  private airplayActive = false;

  // Preload bookkeeping
  private psIdx = -1;
  private psUrl: string | null = null;
  private psOk = false;

  private crossfadeActive = false;
  private gaplessHandoffStarted = false;
  private preload75Done = false;

  private listeners = new Set<AudioEventListener>();

  // Debounce handle for resume-state writes (fires up to ~2x/sec during
  // playback; localStorage is synchronous so hammering it on every
  // timeupdate would jank the main thread).
  private resumeWriteTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(opts?: { volume?: number }) {
    if (typeof opts?.volume === 'number') this.userVolume = opts.volume;

    this.audio = new Audio();
    this.preload = new Audio();
    this.audio.setAttribute('x-webkit-airplay', 'allow');
    this.preload.preload = 'auto';
    this.preload.muted = true;
    this.preload.setAttribute('x-webkit-airplay', 'deny');
    this.preload.setAttribute('disableRemotePlayback', '');
    try {
      (this.preload as any).disableRemotePlayback = true;
    } catch (e) {}
    // CORS for Web Audio analysis. Archive.org sends ACAO:* on download URLs.
    this.audio.crossOrigin = 'anonymous';
    this.preload.crossOrigin = 'anonymous';
    this.audio.volume = this.userVolume;
    this.preload.volume = 0;

    // Attach to the DOM so AirPlay/Remote Playback events fire correctly.
    if (typeof document !== 'undefined') {
      const host = document.createElement('div');
      host.style.cssText = 'position:absolute;width:0;height:0;overflow:hidden;';
      host.appendChild(this.audio);
      host.appendChild(this.preload);
      document.body.appendChild(host);
      this.preloadParent = this.preload.parentNode;
      this.preloadNextSibling = this.preload.nextSibling;
    }

    this.wireEvents();
    this.bindRemotePlaybackWatcher(this.audio);
    this.bindRemotePlaybackWatcher(this.preload);
  }

  // ── Public API ────────────────────────────────────────────────────────

  on(fn: AudioEventListener): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  getContext(): PlaybackContext | null {
    return this.pb;
  }
  isPlaying(): boolean {
    return !!this.pb && !this.audio.paused;
  }
  getVolume(): number {
    return this.userVolume;
  }
  getCurrentTime(): number {
    return this.audio.currentTime || 0;
  }
  getDuration(): number {
    return this.audio.duration || 0;
  }
  isAirplayActive(): boolean {
    return this.airplayActive;
  }

  /** Play a track from a specific show+source. This is the ONE entry point
   *  that snapshots playback context; view navigation never touches audio. */
  playShow(show: AudioShow, source: AudioSource, tracks: AudioTrack[], idx = 0): void {
    if (idx < 0 || idx >= tracks.length) return;
    this.pb = { show, source, tracks: tracks.slice(), trackIdx: idx };
    this.emit({ type: 'showchange', ctx: this.pb });
    this.startTrack(this.pb, idx, /*fromUser*/ true);
  }

  /** Change the currently playing track index within the current show. */
  playIdx(idx: number): void {
    if (!this.pb) return;
    if (idx < 0 || idx >= this.pb.tracks.length) return;
    // Same-track guard (prevents double-play on double-click).
    const t = this.pb.tracks[idx];
    if (
      idx === this.pb.trackIdx &&
      !this.audio.paused &&
      this.audio.currentTime > 0.5 &&
      this.audio.src === t.mp3_url
    )
      return;
    this.pb = { ...this.pb, trackIdx: idx };
    this.startTrack(this.pb, idx, /*fromUser*/ true);
  }

  next(): void {
    if (!this.pb) return;
    this.playIdx(this.pb.trackIdx + 1);
  }
  prev(): void {
    if (!this.pb) return;
    this.playIdx(this.pb.trackIdx - 1);
  }

  play(): void {
    this.audio.play().catch(() => {});
  }
  pause(): void {
    this.audio.pause();
  }
  toggle(): void {
    if (this.audio.paused) this.play();
    else this.pause();
  }

  seek(seconds: number): void {
    if (!isFinite(seconds)) return;
    try {
      this.audio.currentTime = seconds;
    } catch (e) {}
  }

  /** User-scoped volume — the ONLY place volume is set. All internal
   *  transitions (splice, crossfade, handoff) read this back via userVolume so
   *  they don't stomp the user's slider to 1. */
  setVolume(v: number): void {
    v = Math.max(0, Math.min(1, v));
    this.userVolume = v;
    if (v > 0) this.lastVolume = v;
    // Only the active element gets user volume — preload stays at 0 until it
    // becomes active (splice) or is being crossfaded up.
    try {
      this.audio.volume = v;
    } catch (e) {}
    this.emit({ type: 'volume', volume: v });
  }

  toggleMute(): void {
    if (this.userVolume > 0) this.setVolume(0);
    else this.setVolume(this.lastVolume || 1);
  }

  // ── Resume-state persistence ──────────────────────────────────────────

  private scheduleResumeWrite() {
    if (this.resumeWriteTimer) return;
    this.resumeWriteTimer = setTimeout(() => {
      this.resumeWriteTimer = null;
      this.writeResumeSnapshot();
    }, RESUME_WRITE_DEBOUNCE_MS);
  }

  private writeResumeSnapshot() {
    if (typeof localStorage === 'undefined') return;
    const pb = this.pb;
    if (!pb) return;
    const elapsed = this.getCurrentTime();
    // Skip early-track state — reload wouldn't offer to resume anyway, and
    // this prevents the prompt reappearing 1s after a "Start over" dismiss.
    if (elapsed < RESUME_MIN_ELAPSED_S) return;
    const snap: ResumeSnapshot = {
      v: RESUME_VERSION,
      show: pb.show,
      source: pb.source,
      trackCount: pb.tracks.length,
      trackIdx: pb.trackIdx,
      elapsed,
      savedAt: Date.now(),
    };
    try {
      localStorage.setItem(RESUME_KEY, JSON.stringify(snap));
    } catch (e) {}
  }

  /** Read + validate the last saved resume snapshot. Returns null when
   *  missing, unparseable, version-mismatched, or structurally invalid. */
  static loadResumeState(): ResumeSnapshot | null {
    if (typeof localStorage === 'undefined') return null;
    let raw: string | null = null;
    try {
      raw = localStorage.getItem(RESUME_KEY);
    } catch (e) {
      return null;
    }
    if (!raw) return null;
    let parsed: any = null;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      try { localStorage.removeItem(RESUME_KEY); } catch (_) {}
      return null;
    }
    if (!parsed || typeof parsed !== 'object') return null;
    if (parsed.v !== RESUME_VERSION) {
      try { localStorage.removeItem(RESUME_KEY); } catch (_) {}
      return null;
    }
    if (
      !parsed.show ||
      typeof parsed.show.date !== 'string' ||
      !parsed.source ||
      typeof parsed.source.id !== 'string' ||
      typeof parsed.trackIdx !== 'number' ||
      typeof parsed.trackCount !== 'number' ||
      typeof parsed.elapsed !== 'number' ||
      parsed.elapsed < RESUME_MIN_ELAPSED_S ||
      parsed.trackIdx < 0 ||
      parsed.trackIdx >= parsed.trackCount
    ) {
      return null;
    }
    return parsed as ResumeSnapshot;
  }

  /** Clears any saved resume snapshot (used by the dismiss button). */
  static clearResumeState(): void {
    if (typeof localStorage === 'undefined') return;
    try { localStorage.removeItem(RESUME_KEY); } catch (e) {}
  }

  destroy(): void {
    try {
      this.audio.pause();
      this.preload.pause();
    } catch (e) {}
    this.listeners.clear();
  }

  // ── Internals ─────────────────────────────────────────────────────────

  private emit(ev: AudioEvent) {
    this.listeners.forEach((fn) => {
      try {
        fn(ev);
      } catch (e) {}
    });
  }

  private isCasting(): boolean {
    const a: any = this.audio;
    return (
      this.airplayActive ||
      (a.remote && a.remote.state === 'connected') ||
      !!a.webkitCurrentPlaybackTargetIsWireless
    );
  }

  private startTrack(pb: PlaybackContext, idx: number, _fromUser: boolean) {
    const t = pb.tracks[idx];
    if (!t?.mp3_url) return;

    this.gaplessHandoffStarted = false;
    // Cancel any in-flight crossfade — user-initiated play takes priority.
    if (this.crossfadeActive) {
      this.crossfadeActive = false;
      try {
        this.audio.volume = this.userVolume;
        this.preload.volume = 0;
        this.preload.muted = true;
        this.preload.pause();
      } catch (e) {}
    }

    const casting = this.isCasting();
    const canSplice =
      !isIOSPlatform() &&
      !casting &&
      this.preload.src === t.mp3_url &&
      this.preload.readyState >= 3;

    if (canSplice) {
      // Gapless splice — start NEW first, pause OLD second. Yields a tiny
      // overlap rather than a gap; on segues the overlap aligns with the
      // continuous music.
      [this.audio, this.preload] = [this.preload, this.audio];
      this.audio.muted = false;
      this.audio.volume = this.userVolume;
      this.audio.play().catch(() => {});
      this.preload.muted = true;
      this.preload.volume = 0;
      try {
        this.preload.pause();
      } catch (e) {}
    } else {
      this.audio.src = t.mp3_url;
      // NEVER call load() while casting — it drops the AirPlay session.
      if (!casting) {
        this.audio.load();
      }
      this.audio.play().catch(() => {});
    }

    this.preload75Done = false;
    this.psIdx = -1;
    this.psUrl = null;
    this.psOk = false;

    // MediaSession must be set before play() resolves so the AirPlay receiver
    // picks up the right metadata.
    this.setMediaSession(pb, idx);

    this.preloadTrack(idx + 1);
    this.emit({ type: 'trackchange', ctx: this.pb });
    this.scheduleResumeWrite();
  }

  private setMediaSession(pb: PlaybackContext, idx: number) {
    if (typeof navigator === 'undefined' || !('mediaSession' in navigator)) return;
    const t = pb.tracks[idx];
    try {
      (navigator as any).mediaSession.metadata = new (window as any).MediaMetadata({
        title: t.title || '—',
        artist: 'Grateful Dead',
        album: pb.show.label,
      });
      const ms: any = (navigator as any).mediaSession;
      ms.setActionHandler('play', () => this.play());
      ms.setActionHandler('pause', () => this.pause());
      ms.setActionHandler('previoustrack', () => this.prev());
      ms.setActionHandler('nexttrack', () => this.next());
      ms.setActionHandler('stop', () => {
        this.pause();
        this.seek(0);
      });
      ms.setActionHandler('seekbackward', ({ seekOffset }: any) =>
        this.seek(Math.max(0, this.getCurrentTime() - (seekOffset || 10))),
      );
      ms.setActionHandler('seekforward', ({ seekOffset }: any) =>
        this.seek(Math.min(this.getDuration(), this.getCurrentTime() + (seekOffset || 30))),
      );
      try {
        ms.setActionHandler('seekto', ({ seekTime, fastSeek }: any) => {
          if (fastSeek && (this.audio as any).fastSeek) (this.audio as any).fastSeek(seekTime);
          else this.seek(seekTime);
          this.updatePositionState();
        });
      } catch (e) {}
    } catch (e) {}
  }

  private updatePositionState() {
    if (typeof navigator === 'undefined' || !('mediaSession' in navigator)) return;
    const d = this.getDuration();
    if (!d || isNaN(d)) return;
    try {
      (navigator as any).mediaSession.setPositionState({
        duration: d,
        playbackRate: this.audio.playbackRate,
        position: Math.min(this.getCurrentTime(), d),
      });
    } catch (e) {}
  }

  private preloadTrack(idx: number) {
    if (!this.pb) return;
    if (idx < 0 || idx >= this.pb.tracks.length) return;
    const url = this.pb.tracks[idx].mp3_url;
    if (!url || this.psIdx === idx) return;
    // Casting: never spin up a second AVPlayer. Warm the HTTP cache instead.
    if (this.isCasting()) {
      // Range request matches what an <audio> would issue on load.
      fetch(url, {
        headers: { Range: 'bytes=0-1048575' },
        mode: 'cors',
        credentials: 'omit',
      }).catch(() => {});
      return;
    }
    this.psIdx = idx;
    this.psUrl = url;
    this.psOk = false;
    this.preload.src = url;
    this.preload.load();
  }

  private wireEvents() {
    const onTimeUpdate = () => {
      const dur = this.audio.duration;
      const cur = this.audio.currentTime;
      if (!this.preload75Done && dur && cur / dur >= PRELOAD_75) {
        this.preload75Done = true;
        if (this.pb) this.preloadTrack(this.pb.trackIdx + 1);
      }
      if (dur && !isNaN(dur)) {
        const remaining = dur - cur;
        if (remaining > 0 && remaining <= HANDOFF_LEAD_S + 0.3) this.maybeStartGaplessHandoff();
        if (remaining > 0 && remaining <= FADE_TRIGGER_S) this.maybeStartCrossfade();
      }
      this.emit({ type: 'timeupdate', currentTime: cur, duration: dur || 0 });
      this.updatePositionState();
      this.scheduleResumeWrite();
    };
    this.audio.addEventListener('timeupdate', onTimeUpdate);
    // Rebind on swap: both elements listen, but we only act when the event
    // fires on the currently-active `this.audio`.
    this.preload.addEventListener('timeupdate', () => {
      // Preload timeupdate is only interesting during silent handoff — ignore.
    });

    const onEnded = () => this.onEnded();
    this.audio.addEventListener('ended', onEnded);
    this.preload.addEventListener('ended', onEnded);

    this.audio.addEventListener('play', () => this.emit({ type: 'play' }));
    this.preload.addEventListener('play', () => {
      // Silent handoff / crossfade start on preload — do NOT emit play; the
      // host bar reflects the active `this.audio` only.
    });
    this.audio.addEventListener('pause', () => this.emit({ type: 'pause' }));
    this.preload.addEventListener('pause', () => {});

    this.preload.addEventListener('canplaythrough', () => {
      this.psOk = true;
    });
  }

  private onEnded() {
    if (this.crossfadeActive) return; // let crossfade complete the swap
    if (!this.pb) return;
    const nextIdx = this.pb.trackIdx + 1;
    if (nextIdx >= this.pb.tracks.length) {
      this.emit({ type: 'ended' });
      return;
    }
    const nextTrack = this.pb.tracks[nextIdx];
    if (!nextTrack?.mp3_url) return;
    this.pb = { ...this.pb, trackIdx: nextIdx };

    const casting = this.isCasting();
    if (!casting && this.preload.src === nextTrack.mp3_url && this.preload.readyState >= 3) {
      [this.audio, this.preload] = [this.preload, this.audio];
      this.audio.muted = false;
      this.audio.volume = this.userVolume;
      this.audio.play().catch(() => {});
      this.preload.muted = true;
      this.preload.volume = 0;
      try {
        this.preload.pause();
      } catch (e) {}
    } else {
      this.audio.src = nextTrack.mp3_url;
      if (!casting) this.audio.load();
      this.audio.play().catch(() => {});
    }
    // MediaSession updated so lock-screen / receiver show the right title.
    this.setMediaSession(this.pb, nextIdx);
    this.preload75Done = false;
    this.psIdx = -1;
    this.preloadTrack(nextIdx + 1);
    this.emit({ type: 'trackchange', ctx: this.pb });
    this.scheduleResumeWrite();
  }

  private maybeStartGaplessHandoff() {
    if (this.gaplessHandoffStarted) return;
    if (!this.pb) return;
    const nextIdx = this.pb.trackIdx + 1;
    if (nextIdx >= this.pb.tracks.length) return;
    const t = this.pb.tracks[nextIdx];
    if (!t?.mp3_url) return;
    if (this.preload.src !== t.mp3_url || this.preload.readyState < 3) return;
    if (this.isCasting()) return;
    if (isIOSPlatform()) return;
    this.gaplessHandoffStarted = true;
    try {
      this.preload.muted = true;
      this.preload.volume = 0;
      this.preload.play().catch(() => {
        this.gaplessHandoffStarted = false;
      });
    } catch (e) {
      this.gaplessHandoffStarted = false;
    }
  }

  private maybeStartCrossfade() {
    if (this.crossfadeActive) return;
    if (!this.pb) return;
    const nextIdx = this.pb.trackIdx + 1;
    if (nextIdx >= this.pb.tracks.length) return;
    const cur = this.pb.tracks[this.pb.trackIdx];
    // Never crossfade Dead segues — you'd hear the same music twice.
    if (isSegueTrack(cur)) return;
    const t = this.pb.tracks[nextIdx];
    if (!t?.mp3_url) return;
    if (this.preload.src !== t.mp3_url || this.preload.readyState < 3) return;
    if (this.isCasting()) return;
    if (isIOSPlatform()) return;
    this.crossfadeActive = true;
    this.doCrossfade(nextIdx);
  }

  private doCrossfade(nextIdx: number) {
    try {
      this.preload.muted = false;
      this.preload.volume = 0;
      this.preload.play().catch(() => {
        this.crossfadeActive = false;
      });
    } catch (e) {
      this.crossfadeActive = false;
      return;
    }
    const start = performance.now();
    const targetVol = this.userVolume; // scale fade curve so we land at userVol, not 1
    const tick = (now: number) => {
      const p = Math.min((now - start) / FADE_DURATION_MS, 1);
      const outGain = Math.cos((p * Math.PI) / 2);
      const inGain = Math.sin((p * Math.PI) / 2);
      try {
        this.audio.volume = Math.max(0, Math.min(1, outGain * targetVol));
      } catch (e) {}
      try {
        this.preload.volume = Math.max(0, Math.min(1, inGain * targetVol));
      } catch (e) {}
      if (p < 1) requestAnimationFrame(tick);
      else this.finalizeCrossfade(nextIdx);
    };
    requestAnimationFrame(tick);
  }

  private finalizeCrossfade(nextIdx: number) {
    if (!this.pb) {
      this.crossfadeActive = false;
      return;
    }
    this.pb = { ...this.pb, trackIdx: nextIdx };
    try {
      this.audio.pause();
      this.audio.currentTime = 0;
      this.audio.volume = this.userVolume;
    } catch (e) {}
    [this.audio, this.preload] = [this.preload, this.audio];
    try {
      this.preload.muted = true;
      this.preload.volume = 0;
      this.preload.pause();
    } catch (e) {}
    this.crossfadeActive = false;
    this.gaplessHandoffStarted = false;
    this.preload75Done = false;
    this.psIdx = -1;
    this.setMediaSession(this.pb, nextIdx);
    this.preloadTrack(nextIdx + 1);
    this.emit({ type: 'trackchange', ctx: this.pb });
    this.scheduleResumeWrite();
  }

  // ── AirPlay / Remote Playback ─────────────────────────────────────────

  private tearDownPreload() {
    try {
      this.preload.pause();
      this.preload.removeAttribute('src');
      this.preload.load();
      if (this.preload.parentNode) this.preload.parentNode.removeChild(this.preload);
    } catch (e) {}
    this.psIdx = -1;
    this.psUrl = null;
    this.psOk = false;
  }

  private restorePreload() {
    if (this.preload.parentNode || !this.preloadParent) return;
    try {
      (this.preloadParent as Node).insertBefore(this.preload, this.preloadNextSibling);
    } catch (e) {}
  }

  private bindRemotePlaybackWatcher(el: HTMLAudioElement) {
    const remote: any = (el as any).remote;
    const sync = () => {
      const wasActive = this.airplayActive;
      this.airplayActive = remote
        ? remote.state === 'connected' || remote.state === 'connecting'
        : !!(el as any).webkitCurrentPlaybackTargetIsWireless;
      if (this.airplayActive && !wasActive) this.tearDownPreload();
      else if (!this.airplayActive && wasActive) this.restorePreload();
      if (this.airplayActive !== wasActive) {
        this.emit({ type: 'airplay', active: this.airplayActive });
      }
    };
    if (remote && remote.addEventListener) {
      remote.addEventListener('connect', sync);
      remote.addEventListener('connecting', sync);
      remote.addEventListener('disconnect', sync);
    }
    el.addEventListener('webkitcurrentplaybacktargetiswirelesschanged' as any, () => {
      const wasActive = this.airplayActive;
      this.airplayActive = !!(el as any).webkitCurrentPlaybackTargetIsWireless;
      if (this.airplayActive && !wasActive) this.tearDownPreload();
      else if (!this.airplayActive && wasActive) this.restorePreload();
      if (this.airplayActive !== wasActive) {
        this.emit({ type: 'airplay', active: this.airplayActive });
      }
    });
  }
}

// ── Track fetch helper ──────────────────────────────────────────────────
// Backend endpoint is /api/sources/<id>/tracks. Response:
//   { sets: [{ name, tracks: [{ id, title, duration, mp3_url, track, _ord }] }],
//     lineage, taper, transferer }

export async function fetchSourceTracks(sourceId: string): Promise<AudioTrack[]> {
  const res = await fetch(`/api/sources/${sourceId}/tracks`);
  if (!res.ok) throw new Error(`tracks fetch failed: ${res.status}`);
  const data = await res.json();
  const out: AudioTrack[] = [];
  for (const set of data.sets || []) {
    for (const t of set.tracks || []) {
      out.push({
        id: String(t.id ?? out.length),
        title: t.title || '',
        duration: Number(t.duration) || 0,
        mp3_url: t.mp3_url || t.flac_url || '',
        segue: /[>→]\s*$/.test(String(t.title || '').trim()),
      });
    }
  }
  return out;
}

// ── Singleton — the classic UI has exactly ONE AVPlayer. Mirror that. ──
let _engine: AudioEngine | null = null;
export function getEngine(): AudioEngine {
  if (typeof window === 'undefined') {
    // SSR guard — return a stub. The React hook only calls into the real one
    // inside useEffect, so this branch is defensive only.
    throw new Error('AudioEngine requires window');
  }
  if (!_engine) _engine = new AudioEngine();
  return _engine;
}
