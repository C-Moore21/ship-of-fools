import React, { useCallback, useEffect, useState } from 'react';
import { useSofAudio } from '../audio/useSofAudio';
import { fetchBlindReveal, fetchBlindTrack, type BlindReveal } from './api';

export interface BlindTestModalProps {
  open: boolean;
  onClose: () => void;
}

type Phase = 'loading' | 'playing' | 'rated' | 'revealed' | 'error';

const MYSTERY_ID = '__blindtest_mystery__';

export function BlindTestModal({ open, onClose }: BlindTestModalProps) {
  const audio = useSofAudio();
  const [phase, setPhase] = useState<Phase>('loading');
  const [error, setError] = useState<string | null>(null);
  const [rating, setRating] = useState<number>(0);
  const [reveal, setReveal] = useState<BlindReveal | null>(null);

  const loadNext = useCallback(async () => {
    setPhase('loading');
    setError(null);
    setRating(0);
    setReveal(null);
    try {
      const { track_url } = await fetchBlindTrack();
      if (!track_url) throw new Error('No track returned');
      // Feed the audio engine a fully anonymized show so the player bar
      // doesn't leak the answer. On reveal we don't reload playback — the
      // user is meant to keep listening while the reveal is up.
      audio.playShow(
        { date: '????-??-??', label: 'Mystery Track', venue: {} },
        { id: MYSTERY_ID, source_type: 'UNK' },
        [{ id: MYSTERY_ID, title: 'Mystery Track', duration: 0, mp3_url: track_url }],
        0,
      );
      setPhase('playing');
    } catch (e) {
      setError(String(e instanceof Error ? e.message : e));
      setPhase('error');
    }
  }, [audio]);

  useEffect(() => {
    if (open) loadNext();
    // On close, stop playback so the mystery track doesn't keep going.
    return () => {
      if (!open) return;
      if (audio.playing) audio.pause();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const doReveal = useCallback(async () => {
    try {
      const r = await fetchBlindReveal();
      setReveal(r);
      setPhase('revealed');
    } catch (e) {
      setError(String(e instanceof Error ? e.message : e));
      setPhase('error');
    }
  }, []);

  const submitRating = (n: number) => {
    setRating(n);
    setPhase('rated');
  };

  const handleClose = () => {
    if (audio.playing && audio.track?.id === MYSTERY_ID) audio.pause();
    onClose();
  };

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[200] flex items-start justify-center bg-black/70 p-4 md:p-8"
      onClick={(e) => {
        if (e.target === e.currentTarget) handleClose();
      }}
      role="dialog"
      aria-modal="true"
      aria-label="Blind Test"
    >
      <div className="mt-8 flex w-full max-w-[520px] flex-col overflow-hidden rounded-sm border border-line bg-bg shadow-2xl">
        <header className="flex items-start justify-between gap-4 border-b border-line px-4 py-3">
          <div>
            <h2 className="font-display text-xl text-chalk">Blind Test</h2>
            <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-muted">
              Listen · Rate · Reveal
            </p>
          </div>
          <button
            type="button"
            onClick={handleClose}
            className="text-2xl leading-none text-muted transition-colors hover:text-ink"
            aria-label="Close"
          >
            ×
          </button>
        </header>

        <div className="space-y-4 p-4">
          {phase === 'loading' && (
            <div className="py-10 text-center font-mono text-[11px] text-muted">
              Finding a mystery track…
            </div>
          )}
          {phase === 'error' && (
            <div className="space-y-3">
              <div className="rounded-sm border border-accent/50 bg-accent/10 px-3 py-2 text-[11px] text-accent">
                {error}
              </div>
              <button
                type="button"
                onClick={loadNext}
                className="rounded-sm border border-line bg-surface2 px-3 py-1.5 font-mono text-[10px] uppercase tracking-widest text-ink hover:border-ink"
              >
                Try again
              </button>
            </div>
          )}

          {(phase === 'playing' || phase === 'rated' || phase === 'revealed') && (
            <>
              <div className="rounded-sm border border-line bg-surface2 p-3">
                <div className="mb-2 font-mono text-[9px] uppercase tracking-widest text-muted">
                  Mystery Track
                </div>
                <div className="flex items-center gap-3">
                  <button
                    type="button"
                    onClick={audio.toggle}
                    className="flex h-10 w-10 items-center justify-center rounded-full bg-accent text-chalk transition-transform hover:bg-accent-hover active:scale-95"
                    aria-label={audio.playing ? 'Pause' : 'Play'}
                  >
                    {audio.playing ? '❚❚' : '▶'}
                  </button>
                  <div className="flex-1 font-mono text-[10px] text-muted">
                    {audio.playing ? 'Playing…' : 'Paused'}
                  </div>
                </div>
                <p className="mt-2 font-mono text-[9px] text-muted">
                  Listen for a moment, then rate before you reveal.
                </p>
              </div>

              <div>
                <div className="mb-1.5 font-mono text-[10px] uppercase tracking-widest text-muted">
                  Your rating
                </div>
                <div className="flex items-center gap-1">
                  {[1, 2, 3, 4, 5].map((n) => (
                    <button
                      key={n}
                      type="button"
                      onClick={() => submitRating(n)}
                      className={`h-9 w-9 rounded-sm text-xl leading-none transition-colors ${
                        n <= rating ? 'text-amber' : 'text-line hover:text-muted'
                      }`}
                      aria-label={`${n} star${n > 1 ? 's' : ''}`}
                    >
                      ★
                    </button>
                  ))}
                </div>
              </div>

              {phase !== 'revealed' && (
                <button
                  type="button"
                  onClick={doReveal}
                  disabled={rating === 0}
                  className="rounded-sm bg-accent px-4 py-2 font-mono text-[11px] uppercase tracking-widest text-chalk transition-colors hover:bg-accent-hover disabled:cursor-not-allowed disabled:bg-line disabled:text-muted"
                >
                  {rating === 0 ? 'Rate first, then reveal' : 'Reveal'}
                </button>
              )}

              {phase === 'revealed' && reveal && (
                <div className="space-y-2 rounded-sm border border-royal/50 bg-royal/10 p-3">
                  <div className="font-display text-lg text-chalk">{reveal.show_date}</div>
                  <div className="text-[11px] text-ink">{reveal.venue || '—'}</div>
                  <div className="font-mono text-[10px] text-muted">
                    Track: "{reveal.track_title}"
                  </div>
                  <div className="font-mono text-[9px] text-muted">
                    You rated it {rating}★
                  </div>
                  <button
                    type="button"
                    onClick={loadNext}
                    className="mt-2 rounded-sm border border-line bg-surface2 px-3 py-1.5 font-mono text-[10px] uppercase tracking-widest text-ink hover:border-ink"
                  >
                    Next mystery →
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
