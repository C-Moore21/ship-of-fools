import { useCallback, useEffect, useMemo, useState } from 'react';
import type { Show, Track } from '../types/archive';

export interface PlayerState {
  track: Track | null;
  show: Show | null;
  isPlaying: boolean;
  elapsed: number;
  play: (show: Show, track: Track) => void;
  toggle: () => void;
  next: () => void;
  prev: () => void;
  seek: (seconds: number) => void;
}

export function usePlayer(initialShow: Show): PlayerState {
  const [show, setShow] = useState<Show | null>(initialShow);
  const [trackId, setTrackId] = useState<string | null>(
    (initialShow.tracks[12] ?? initialShow.tracks[0])?.id ?? null
  );
  const [isPlaying, setIsPlaying] = useState(false);
  const [elapsed, setElapsed] = useState(214);

  const track = useMemo(
    () => show?.tracks.find((t) => t.id === trackId) ?? null,
    [show, trackId]
  );

  useEffect(() => {
    if (!isPlaying || !track) return;
    const id = window.setInterval(() => {
      setElapsed((prev) => prev + 1 >= track.duration ? 0 : prev + 1);
    }, 1000);
    return () => window.clearInterval(id);
  }, [isPlaying, track]);

  const play = useCallback((nextShow: Show, nextTrack: Track) => {
    setShow(nextShow);
    setTrackId((current) => {
      if (current === nextTrack.id) return current;
      setElapsed(0);
      return nextTrack.id;
    });
    setIsPlaying(true);
  }, []);

  const step = useCallback(
    (delta: number) => {
      if (!show || !track) return;
      const index = show.tracks.findIndex((t) => t.id === track.id);
      const nextTrack = show.tracks[index + delta];
      if (!nextTrack) return;
      setTrackId(nextTrack.id);
      setElapsed(0);
    },
    [show, track]
  );

  return {
    track,
    show,
    isPlaying,
    elapsed,
    play,
    toggle: () => setIsPlaying((p) => !p),
    next: () => step(1),
    prev: () => step(-1),
    seek: setElapsed
  };
}