import React from 'react';
import { useSofAudio } from '../audio/useSofAudio';

/**
 * Small floating pill above the player bar that surfaces Trip Mode's next
 * destination. Renders nothing when Trip is off. Consumes state from
 * useSofAudio() — the audio module owns resolution + auto-advance.
 */
export function TripBanner() {
  const { trip, toggleTrip } = useSofAudio();

  if (trip.status === 'off') return null;

  let title = '';
  let subtitle = '';

  switch (trip.status) {
    case 'resolving':
      title = 'Trip Mode';
      subtitle = `Finding next show after ${trip.seedDate}…`;
      break;
    case 'ready': {
      const v = trip.dest.venue || {};
      const venueBits = [v.name, v.city, v.state].filter(Boolean).join(', ');
      title = `Next: ${trip.dest.date}`;
      subtitle = venueBits || 'Next-night show queued';
      break;
    }
    case 'end-of-run':
      title = 'End of tour run';
      subtitle = `No next-night show after ${trip.seedDate}`;
      break;
    case 'capped':
      title = 'Trip complete';
      subtitle = 'Played 3 shows in a row. Toggle back on to keep rolling.';
      break;
  }

  return (
    <div
      className="pointer-events-auto fixed inset-x-0 bottom-[84px] z-[150] mx-auto flex max-w-[92vw] items-center gap-3 rounded-sm border border-royal bg-surface2/95 px-3 py-2 shadow-2xl backdrop-blur-sm md:bottom-[92px] md:max-w-[520px]"
      role="status"
      aria-live="polite"
    >
      <span className="text-base leading-none">🚌</span>
      <div className="min-w-0 flex-1">
        <div className="truncate font-mono text-[10px] uppercase tracking-widest text-royal-bright">
          {title}
        </div>
        <div className="truncate text-[11px] text-ink">{subtitle}</div>
      </div>
      <button
        type="button"
        onClick={toggleTrip}
        className="shrink-0 rounded-sm border border-line bg-surface px-2.5 py-1 font-mono text-[9px] uppercase tracking-widest text-muted transition-colors hover:border-ink hover:text-ink"
      >
        Turn off
      </button>
    </div>
  );
}
