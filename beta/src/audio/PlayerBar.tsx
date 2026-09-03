// Drop-in PlayerBar wired to useSofAudio. Same visual layout as the existing
// beta/src/components/PlayerBar.tsx — but no props. All state comes from the
// audio engine singleton, so this component can be mounted anywhere in the
// app and always reflects "what's actually playing".

import React from 'react';
import { PauseIcon, PlayIcon, SkipBackIcon, SkipForwardIcon, Volume2Icon, VolumeXIcon } from 'lucide-react';
import { formatDuration } from '../utils/format';
import { useSofAudio, useSofAudioTime } from './useSofAudio';

export function PlayerBar() {
  const {
    show,
    track,
    playing,
    volume,
    toggle,
    next,
    prev,
    seek,
    setVolume,
    toggleMute,
    snapToPlayingShow,
    tripEnabled,
    trip,
    toggleTrip,
    airplayActive,
  } = useSofAudio();
  const { elapsed, duration } = useSofAudioTime();

  const hasTrack = !!track;
  const effectiveDuration = duration || track?.duration || 0;
  const progress = hasTrack && effectiveDuration ? (elapsed / effectiveDuration) * 100 : 0;

  const tripLabel =
    trip.status === 'ready'
      ? `→ ${trip.dest.date}${trip.dest.venue?.name ? ' @ ' + trip.dest.venue.name : ''}`
      : trip.status === 'resolving'
        ? 'resolving next show…'
        : trip.status === 'end-of-run'
          ? "last night of this run · won't auto-advance"
          : trip.status === 'capped'
            ? 'Trip ended after 3 shows'
            : 'keep playing into the next night when the encore ends';

  return (
    <footer className="flex h-[76px] shrink-0 items-center gap-4 border-t border-line bg-surface px-4 md:gap-6 md:px-6">
      <button
        type="button"
        onClick={snapToPlayingShow}
        disabled={!hasTrack}
        title="Jump back to the playing show"
        className="flex min-w-0 flex-1 items-center gap-3 text-left md:w-[280px] md:flex-none disabled:cursor-default">
        <span
          className={`h-10 w-1 shrink-0 rounded-sm ${playing ? 'bg-accent' : 'bg-line'}`}
          aria-hidden="true" />
        <div className="min-w-0">
          <p className="truncate font-display text-sm text-chalk">
            {hasTrack ? track!.title : 'Nothing queued'}
          </p>
          <p className="truncate text-[11px] text-muted">
            {show ? show.label : 'Pick a show to start'}
            {airplayActive ? ' · AirPlay' : ''}
          </p>
        </div>
      </button>

      <div className="flex flex-1 flex-col items-center gap-1.5">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={prev}
            disabled={!hasTrack}
            aria-label="Previous track"
            className="flex h-8 w-8 items-center justify-center rounded-sm text-muted transition-colors duration-150 ease-archive hover:text-ink disabled:opacity-40">
            <SkipBackIcon className="h-4 w-4 fill-current" />
          </button>
          <button
            type="button"
            onClick={toggle}
            disabled={!hasTrack}
            aria-label={playing ? 'Pause' : 'Play'}
            className="flex h-9 w-9 items-center justify-center rounded-full bg-accent text-chalk transition-transform duration-100 ease-archive hover:bg-accent-hover active:scale-95 disabled:opacity-40">
            {playing ? (
              <PauseIcon className="h-4 w-4 fill-current" />
            ) : (
              <PlayIcon className="ml-0.5 h-4 w-4 fill-current" />
            )}
          </button>
          <button
            type="button"
            onClick={next}
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
              max={effectiveDuration || 100}
              value={elapsed}
              onChange={(e) => seek(Number(e.target.value))}
              disabled={!hasTrack}
              aria-label="Seek"
              className="absolute inset-x-0 -top-2 h-5 w-full cursor-pointer appearance-none bg-transparent" />
          </div>
          <span className="w-9 shrink-0 font-mono text-[10px] tabular-nums text-muted">
            {formatDuration(effectiveDuration)}
          </span>
        </div>
      </div>

      <div className="hidden w-[280px] items-center justify-end gap-4 md:flex">
        <button
          type="button"
          onClick={toggleTrip}
          title={tripLabel}
          className={`rounded-sm px-2 py-1 text-[10px] font-mono uppercase tracking-wide transition-colors ${
            tripEnabled ? 'bg-accent text-chalk' : 'text-muted hover:text-ink'
          }`}>
          🚌 {tripEnabled ? 'Trip' : 'Trip mode'}
        </button>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={toggleMute}
            aria-label={volume === 0 ? 'Unmute' : 'Mute'}
            className="text-muted hover:text-ink">
            {volume === 0 ? (
              <VolumeXIcon className="h-4 w-4" />
            ) : (
              <Volume2Icon className="h-4 w-4" />
            )}
          </button>
          <input
            type="range"
            min={0}
            max={1}
            step={0.01}
            value={volume}
            onChange={(e) => setVolume(Number(e.target.value))}
            aria-label="Volume"
            className="h-1 w-20 cursor-pointer appearance-none rounded-sm bg-line" />
        </div>
      </div>
    </footer>
  );
}
