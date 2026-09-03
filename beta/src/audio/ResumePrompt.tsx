// Floating "Resume [venue] · [track]" notice shown on load when the engine
// is idle and a valid ResumeSnapshot exists in localStorage. Deliberately
// NOT auto-play — the classic UI's auto-resume was too aggressive (surprised
// users, hijacked speakers). This is opt-in per reload.
//
// Rehydration path: fetch the source's tracks fresh (title/duration may have
// been re-transferred on Archive.org since we cached), then engine.playShow +
// seek(elapsed) once the audio element is loaded enough to accept a seek.

import React, { useEffect, useState } from 'react';
import { AudioEngine, fetchSourceTracks, getEngine, type ResumeSnapshot } from './engine';

export function ResumePrompt() {
  const [snap, setSnap] = useState<ResumeSnapshot | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Read once on mount. Only surface if the engine is idle (nothing has
  // played this session) — otherwise the user is already listening and a
  // "Resume" nudge would be nonsense.
  useEffect(() => {
    const engine = getEngine();
    if (engine.getContext()) return;
    const s = AudioEngine.loadResumeState();
    if (s) setSnap(s);
  }, []);

  // Auto-hide the moment the engine starts playing something (via any code
  // path — the user might click a track directly instead of resuming).
  useEffect(() => {
    if (!snap) return;
    const engine = getEngine();
    const off = engine.on((ev) => {
      if (ev.type === 'showchange' || ev.type === 'trackchange') {
        setSnap(null);
      }
    });
    return off;
  }, [snap]);

  if (!snap) return null;

  const venueName =
    snap.show.venue?.name ||
    snap.show.label ||
    snap.show.date ||
    'last show';

  const trackLabel = `Track ${snap.trackIdx + 1}`;

  const onDismiss = () => {
    AudioEngine.clearResumeState();
    setSnap(null);
  };

  const onResume = async () => {
    if (!snap || busy) return;
    setBusy(true);
    setError(null);
    let tracks;
    try {
      tracks = await fetchSourceTracks(snap.source.id);
    } catch (e) {
      // Source likely removed / renamed on Archive.org, or offline. Wipe the
      // stale snapshot so we don't offer it again next reload.
      AudioEngine.clearResumeState();
      setBusy(false);
      setError('Source unavailable');
      setTimeout(() => setSnap(null), 1500);
      return;
    }
    // Track list may have shifted (re-transferred with different split). Clamp
    // the saved index rather than fail outright — same-position track is a
    // reasonable best-effort.
    if (!tracks.length) {
      AudioEngine.clearResumeState();
      setBusy(false);
      setError('No tracks');
      setTimeout(() => setSnap(null), 1500);
      return;
    }
    const idx = Math.min(snap.trackIdx, tracks.length - 1);
    const engine = getEngine();
    engine.playShow(snap.show, snap.source, tracks, idx);
    // Seek once audio has loaded enough. First 'timeupdate' after playShow
    // confirms the element is playing and can accept a seek reliably.
    const targetElapsed = snap.elapsed;
    let sought = false;
    const off = engine.on((ev) => {
      if (sought) return;
      if (ev.type === 'timeupdate' && ev.duration > 0) {
        sought = true;
        engine.seek(Math.min(targetElapsed, Math.max(0, ev.duration - 1)));
        off();
      }
    });
    // Fallback: if timeupdate never fires (buffering stall), try after 1.5s.
    setTimeout(() => {
      if (!sought) {
        sought = true;
        engine.seek(targetElapsed);
        off();
      }
    }, 1500);
    setSnap(null);
  };

  return (
    <div
      role="dialog"
      aria-label="Resume last playback"
      style={{
        position: 'fixed',
        left: 16,
        bottom: 96,
        zIndex: 60,
        maxWidth: 360,
        display: 'flex',
        alignItems: 'center',
        gap: 10,
        padding: '10px 12px',
        borderRadius: 8,
        background: 'var(--surface, #1b1b1b)',
        color: 'var(--text, #eee)',
        border: '1px solid var(--border, #333)',
        boxShadow: '0 8px 24px rgba(0,0,0,0.35)',
        fontFamily: 'Space Mono, monospace',
        fontSize: 12,
      }}>
      <button
        type="button"
        onClick={onResume}
        disabled={busy}
        aria-label={`Resume ${venueName}`}
        style={{
          background: 'var(--accent, #e8332a)',
          color: '#fff',
          border: 'none',
          padding: '6px 10px',
          borderRadius: 6,
          cursor: busy ? 'default' : 'pointer',
          fontFamily: 'inherit',
          fontSize: 12,
          opacity: busy ? 0.6 : 1,
        }}>
        {busy ? '…' : '▶'}
      </button>
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={{ opacity: 0.7, fontSize: 10, textTransform: 'uppercase', letterSpacing: 1 }}>
          {error ?? 'Resume'}
        </div>
        <div
          style={{
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
          }}>
          <strong>{venueName}</strong>
          {'  ·  '}
          {trackLabel}
        </div>
      </div>
      <button
        type="button"
        onClick={onDismiss}
        aria-label="Dismiss"
        style={{
          background: 'transparent',
          color: 'var(--muted, #888)',
          border: '1px solid var(--border, #333)',
          padding: '4px 8px',
          borderRadius: 6,
          cursor: 'pointer',
          fontFamily: 'inherit',
          fontSize: 11,
        }}>
        ✕
      </button>
    </div>
  );
}
