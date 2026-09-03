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

type SetKey = Track['set'];

// ── TrackRow ──────────────────────────────────────────────────────────────
// Extracted + React.memo'd so a currentTrackId change only re-renders the two
// affected rows (old-current and new-current) rather than all 25+ rows and
// their nested RatingStars widgets. Onclick / onStarChange are closed over
// stable props, keeping the memo effective.
interface TrackRowProps {
  track: Track;
  index: number;
  isCurrent: boolean;
  isPlaying: boolean;
  compact: boolean;
  loggedIn: boolean;
  sourceId?: string;
  rating: number;
  showId: string;
  onPlay: (track: Track) => void;
  onRate: (trackId: string, stars: number, meta: { trackTitle: string; showDate: string }) => void;
  onRequestLogin?: () => void;
  registerRow: (id: string, el: HTMLLIElement | null) => void;
}

const TrackRowImpl = ({
  track,
  index,
  isCurrent,
  isPlaying,
  compact,
  loggedIn,
  sourceId,
  rating,
  showId,
  onPlay,
  onRate,
  onRequestLogin,
  registerRow,
}: TrackRowProps) => {
  // Stable ref callback per row: relies on registerRow being stable
  // (parent creates it once via useCallback).
  const setRef = React.useCallback(
    (el: HTMLLIElement | null) => registerRow(track.id, el),
    [track.id, registerRow],
  );

  const onClick = React.useCallback(() => onPlay(track), [onPlay, track]);
  const onStarChange = React.useCallback(
    (next: number) =>
      onRate(track.id, next, { trackTitle: track.title, showDate: showId }),
    [onRate, track.id, track.title, showId],
  );
  const stopBubble = React.useCallback((e: React.MouseEvent) => e.stopPropagation(), []);

  return (
    <li ref={setRef}>
      <button
        type="button"
        onClick={onClick}
        aria-current={isCurrent ? 'true' : undefined}
        className={`group flex w-full items-center gap-3 border-l-2 pr-2 text-left transition-colors duration-150 ease-archive ${
          compact ? 'py-1.5 pl-3' : 'py-2.5 pl-3'
        } ${
          isCurrent
            ? 'border-l-accent bg-surface2'
            : 'border-l-transparent hover:bg-surface2/50'
        }`}>
        <span className="relative flex h-5 w-5 shrink-0 items-center justify-center">
          <span
            className={`font-mono text-[11px] tabular-nums text-muted ${
              isCurrent ? 'opacity-0' : 'group-hover:opacity-0'
            }`}>
            {String(index + 1).padStart(2, '0')}
          </span>
          <PlayIcon
            className={`absolute h-3 w-3 transition-opacity duration-150 ease-archive ${
              isCurrent
                ? 'fill-accent text-accent opacity-100'
                : 'fill-ink text-ink opacity-0 group-hover:opacity-100'
            }`} />
        </span>

        <span className="min-w-0 flex-1">
          <span
            className={`block truncate font-display text-[15px] ${
              isCurrent ? 'text-chalk' : 'text-ink'
            }`}>
            {track.title}
            {track.segue && <span className="ml-1.5 text-accent">&gt;</span>}
          </span>
          {isCurrent && isPlaying && (
            <span className="text-[10px] uppercase tracking-[0.14em] text-accent">
              Now playing
            </span>
          )}
        </span>

        {track.gap && (
          <span className="hidden shrink-0 sm:block">
            <GapBadge gap={track.gap} />
          </span>
        )}

        <span className="hidden w-[104px] shrink-0 justify-end md:flex">
          {track.rarity !== 'common' && track.rarity !== 'solid' && (
            <RarityMark rarity={track.rarity} />
          )}
        </span>

        {loggedIn && sourceId && (
          <span
            className="hidden shrink-0 sm:inline-flex"
            onClick={stopBubble}
          >
            <RatingStars
              value={rating}
              size={12}
              onChange={onStarChange}
              onRequestLogin={onRequestLogin}
            />
          </span>
        )}

        <span className="w-12 shrink-0 text-right font-mono text-[11px] tabular-nums text-muted">
          {formatDuration(track.duration)}
        </span>
      </button>
    </li>
  );
};

const TrackRow = React.memo(TrackRowImpl);

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
  const { ratings, setRating } = useTrackRatings(sourceId ?? null, loggedIn);
  const rowRefs = React.useRef(new Map<string, HTMLLIElement>());

  // Stable registerRow so per-row ref callbacks don't churn every render.
  const registerRow = React.useCallback((id: string, el: HTMLLIElement | null) => {
    if (el) rowRefs.current.set(id, el);
    else rowRefs.current.delete(id);
  }, []);

  // Bucket tracks by set once per show.tracks change — used to be recomputed
  // on every render (including every currentTrackId change).
  const grouped = React.useMemo(() => {
    const buckets: { key: SetKey; tracks: Track[]; seconds: number }[] = [];
    for (const set of ['I', 'II', 'E'] as SetKey[]) {
      const tracks = show.tracks.filter((t) => t.set === set);
      if (tracks.length === 0) continue;
      const seconds = tracks.reduce((sum, t) => sum + t.duration, 0);
      buckets.push({ key: set, tracks, seconds });
    }
    return buckets;
  }, [show.tracks]);

  // Per-set index lookup so TrackRow gets a stable primitive (not recomputed).
  const trackIndexInSet = React.useMemo(() => {
    const map = new Map<string, number>();
    for (const b of grouped) b.tracks.forEach((t, i) => map.set(t.id, i));
    return map;
  }, [grouped]);

  // Auto-scroll to the playing track when it advances — only when the track
  // actually belongs to this show. Deps narrowed from `show` (whole object) to
  // just the tracks array, so unrelated show re-renders (weather landing, avg
  // rating updating) don't retrigger scroll.
  React.useEffect(() => {
    if (!currentTrackId) return;
    const belongs = show.tracks.some((t) => t.id === currentTrackId);
    if (!belongs) return;
    const el = rowRefs.current.get(currentTrackId);
    if (!el) return;
    const raf = requestAnimationFrame(() => {
      try {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      } catch (e) {
        try { el.scrollIntoView({ block: 'center' }); } catch (_) {}
      }
    });
    return () => cancelAnimationFrame(raf);
  }, [currentTrackId, show.tracks]);

  return (
    <div className="space-y-8">
      {grouped.map(({ key, tracks, seconds }) => (
        <section key={key} aria-labelledby={`set-${key}`}>
          <div className="flex items-baseline gap-3 border-b border-line pb-2">
            <h3
              id={`set-${key}`}
              className="font-display text-sm font-bold uppercase tracking-[0.2em] text-chalk">
              {SET_NAMES[key]}
            </h3>
            <span className="text-[11px] text-muted">
              {tracks.length} songs · {formatClock(seconds)}
            </span>
          </div>

          <ul>
            {tracks.map((track) => (
              <TrackRow
                key={track.id}
                track={track}
                index={trackIndexInSet.get(track.id) ?? 0}
                isCurrent={track.id === currentTrackId}
                isPlaying={isPlaying}
                compact={compact}
                loggedIn={loggedIn}
                sourceId={sourceId}
                rating={ratings[track.id] || 0}
                showId={show.id}
                onPlay={onPlay}
                onRate={setRating}
                onRequestLogin={onRequestLogin}
                registerRow={registerRow}
              />
            ))}
          </ul>
        </section>
      ))}
    </div>
  );
}

export const Setlist = React.memo(SetlistImpl);
