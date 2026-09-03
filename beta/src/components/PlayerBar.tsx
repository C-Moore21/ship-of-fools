import React from 'react';
import { PauseIcon, PlayIcon, SkipBackIcon, SkipForwardIcon, Volume2Icon } from 'lucide-react';
import type { Show, Track } from '../types/archive';
import { Visualizer } from './Visualizer';
import { formatDuration, formatDate } from '../utils/format';

type VisualizerMode = 'bars' | 'radial' | 'off';

interface PlayerBarProps {
  show: Show | null;
  track: Track | null;
  isPlaying: boolean;
  elapsed: number;
  visualizer: VisualizerMode;
  onToggle: () => void;
  onPrev: () => void;
  onNext: () => void;
  onSeek: (seconds: number) => void;
}

export function PlayerBar({
  show,
  track,
  isPlaying,
  elapsed,
  visualizer,
  onToggle,
  onPrev,
  onNext,
  onSeek
}: PlayerBarProps) {
  const hasTrack = Boolean(show && track);
  const progress = hasTrack ? elapsed / track!.duration * 100 : 0;

  return (
    <footer className="flex h-[76px] shrink-0 items-center gap-4 border-t border-line bg-surface px-4 md:gap-6 md:px-6">
      <div className="flex min-w-0 flex-1 items-center gap-3 md:w-[280px] md:flex-none">
        <span
          className={`h-10 w-1 shrink-0 rounded-sm ${isPlaying ? 'bg-accent' : 'bg-line'}`}
          aria-hidden="true" />
        
        <div className="min-w-0">
          <p className="truncate font-display text-sm text-chalk">
            {hasTrack ? track!.title : 'Nothing queued'}
          </p>
          <p className="truncate text-[11px] text-muted">
            {hasTrack ? `${formatDate(show!.date).numeric} · ${show!.venue}` : 'Pick a show to start'}
          </p>
        </div>
      </div>

      <div className="flex flex-1 flex-col items-center gap-1.5">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onPrev}
            disabled={!hasTrack}
            aria-label="Previous track"
            className="flex h-8 w-8 items-center justify-center rounded-sm text-muted transition-colors duration-150 ease-archive hover:text-ink disabled:opacity-40">
            
            <SkipBackIcon className="h-4 w-4 fill-current" />
          </button>
          <button
            type="button"
            onClick={onToggle}
            disabled={!hasTrack}
            aria-label={isPlaying ? 'Pause' : 'Play'}
            className="flex h-9 w-9 items-center justify-center rounded-full bg-accent text-chalk transition-transform duration-100 ease-archive hover:bg-accent-hover active:scale-95 disabled:opacity-40">
            
            {isPlaying ?
            <PauseIcon className="h-4 w-4 fill-current" /> :

            <PlayIcon className="ml-0.5 h-4 w-4 fill-current" />
            }
          </button>
          <button
            type="button"
            onClick={onNext}
            disabled={!hasTrack}
            aria-label="Next track"
            className="flex h-8 w-8 items-center justify-center rounded-sm text-muted transition-colors duration-150 ease-archive hover:text-ink disabled:opacity-40">
            
            <SkipForwardIcon className="h-4 w-4 fill-current" />
          </button>
        </div>

        <div className="flex w-full max-w-[520px] items-center gap-3">
          <span className="w-9 shrink-0 text-right font-mono text-[10px] tabular-nums text-muted">
            {formatDuration(elapsed)}
          </span>
          <div className="relative h-1 flex-1 rounded-sm bg-line">
            <div
              className="absolute inset-y-0 left-0 rounded-sm bg-accent"
              style={{ width: `${progress}%` }} />
            
            <input
              type="range"
              min={0}
              max={hasTrack ? track!.duration : 100}
              value={elapsed}
              onChange={(e) => onSeek(Number(e.target.value))}
              disabled={!hasTrack}
              aria-label="Seek"
              className="absolute inset-x-0 -top-2 h-5 w-full cursor-pointer appearance-none bg-transparent" />
            
          </div>
          <span className="w-9 shrink-0 font-mono text-[10px] tabular-nums text-muted">
            {hasTrack ? formatDuration(track!.duration) : '0:00'}
          </span>
        </div>
      </div>

      <div className="hidden w-[280px] items-center justify-end gap-4 md:flex">
        {visualizer !== 'off' && <Visualizer mode={visualizer} active={isPlaying} />}
        <div className="flex items-center gap-2">
          <Volume2Icon className="h-4 w-4 text-muted" />
          <div className="h-1 w-20 rounded-sm bg-line">
            <div className="h-full w-2/3 rounded-sm bg-muted" />
          </div>
        </div>
      </div>
    </footer>);

}