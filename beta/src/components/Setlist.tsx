import React from 'react';
import { PlayIcon } from 'lucide-react';
import type { Show, Track } from '../types/archive';
import { RarityMark, GapBadge } from './Badges';
import { formatDuration, formatClock } from '../utils/format';
import { RatingStars, useTrackRatings } from '../auth-and-social';

const SET_NAMES: Record<Track['set'], string> = {
  I: 'Set I',
  II: 'Set II',
  E: 'Encore'
};

interface SetlistProps {
  show: Show;
  currentTrackId?: string;
  isPlaying: boolean;
  compact: boolean;
  onPlay: (track: Track) => void;
  /** Whether the current visitor has a session — gates the per-track star widget. */
  loggedIn?: boolean;
  /** Archive.org source id backing this setlist; ratings are keyed per source. */
  sourceId?: string;
  /** Called when a logged-out user clicks stars, so the parent can open a login modal. */
  onRequestLogin?: () => void;
}

function SetlistImpl({
  show,
  currentTrackId,
  isPlaying,
  compact,
  onPlay,
  loggedIn = false,
  sourceId,
  onRequestLogin,
}: SetlistProps) {
  const sets: Track['set'][] = ['I', 'II', 'E'];
  const { ratings, setRating } = useTrackRatings(sourceId ?? null, loggedIn);
  const rowRefs = React.useRef(new Map<string, HTMLLIElement>());

  // Auto-scroll to the playing track when it advances — only when the track
  // actually belongs to this show (parent gates via currentTrackId; we
  // double-check against show.tracks so browsing other shows during playback
  // never hijacks the scroll position).
  React.useEffect(() => {
    if (!currentTrackId) return;
    const belongs = show.tracks.some((t) => t.id === currentTrackId);
    if (!belongs) return;
    const el = rowRefs.current.get(currentTrackId);
    if (!el) return;
    // rAF so we scroll after the row has painted (fixes first-render case
    // where the ref exists but layout hasn't settled).
    const raf = requestAnimationFrame(() => {
      try {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      } catch (e) {
        try { el.scrollIntoView({ block: 'center' }); } catch (_) {}
      }
    });
    return () => cancelAnimationFrame(raf);
  }, [currentTrackId, show]);

  return (
    <div className="space-y-8">
      {sets.map((set) => {
        const tracks = show.tracks.filter((t) => t.set === set);
        if (tracks.length === 0) return null;
        const seconds = tracks.reduce((sum, t) => sum + t.duration, 0);

        return (
          <section key={set} aria-labelledby={`set-${set}`}>
            <div className="flex items-baseline gap-3 border-b border-line pb-2">
              <h3
                id={`set-${set}`}
                className="font-display text-sm font-bold uppercase tracking-[0.2em] text-chalk">
                
                {SET_NAMES[set]}
              </h3>
              <span className="text-[11px] text-muted">
                {tracks.length} songs · {formatClock(seconds)}
              </span>
            </div>

            <ul>
              {tracks.map((track, i) => {
                const isCurrent = track.id === currentTrackId;
                return (
                  <li
                    key={track.id}
                    ref={(el) => {
                      if (el) rowRefs.current.set(track.id, el);
                      else rowRefs.current.delete(track.id);
                    }}>

                    <button
                      type="button"
                      onClick={() => onPlay(track)}
                      aria-current={isCurrent ? 'true' : undefined}
                      className={`group flex w-full items-center gap-3 border-l-2 pr-2 text-left transition-colors duration-150 ease-archive ${
                      compact ? 'py-1.5 pl-3' : 'py-2.5 pl-3'} ${

                      isCurrent ?
                      'border-l-accent bg-surface2' :
                      'border-l-transparent hover:bg-surface2/50'}`
                      }>
                      
                      <span className="relative flex h-5 w-5 shrink-0 items-center justify-center">
                        <span
                          className={`font-mono text-[11px] tabular-nums text-muted ${
                          isCurrent ? 'opacity-0' : 'group-hover:opacity-0'}`
                          }>
                          
                          {String(i + 1).padStart(2, '0')}
                        </span>
                        <PlayIcon
                          className={`absolute h-3 w-3 transition-opacity duration-150 ease-archive ${
                          isCurrent ?
                          'fill-accent text-accent opacity-100' :
                          'fill-ink text-ink opacity-0 group-hover:opacity-100'}`
                          } />
                        
                      </span>

                      <span className="min-w-0 flex-1">
                        <span
                          className={`block truncate font-display text-[15px] ${
                          isCurrent ? 'text-chalk' : 'text-ink'}`
                          }>
                          
                          {track.title}
                          {track.segue && <span className="ml-1.5 text-accent">&gt;</span>}
                        </span>
                        {isCurrent && isPlaying &&
                        <span className="text-[10px] uppercase tracking-[0.14em] text-accent">
                            Now playing
                          </span>
                        }
                      </span>

                      {track.gap &&
                      <span className="hidden shrink-0 sm:block">
                          <GapBadge gap={track.gap} />
                        </span>
                      }

                      <span className="hidden w-[104px] shrink-0 justify-end md:flex">
                        {track.rarity !== 'common' && track.rarity !== 'solid' && (
                          <RarityMark rarity={track.rarity} />
                        )}
                      </span>

                      {loggedIn && sourceId && (
                        <span
                          className="hidden shrink-0 sm:inline-flex"
                          // Stop the row's onPlay firing when the user clicks a star.
                          onClick={(e) => e.stopPropagation()}
                        >
                          <RatingStars
                            value={ratings[track.id] || 0}
                            size={12}
                            onChange={(next) =>
                              setRating(track.id, next, {
                                trackTitle: track.title,
                                showDate: show.id,
                              })
                            }
                            onRequestLogin={onRequestLogin}
                          />
                        </span>
                      )}

                      <span className="w-12 shrink-0 text-right font-mono text-[11px] tabular-nums text-muted">
                        {formatDuration(track.duration)}
                      </span>
                    </button>
                  </li>);

              })}
            </ul>
          </section>);

      })}
    </div>);

}

export const Setlist = React.memo(SetlistImpl);
