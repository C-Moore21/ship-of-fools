import React from 'react';
import { CheckIcon, DiscAlbumIcon, RadioIcon, StarIcon } from 'lucide-react';
import type { Show } from '../types/archive';
import { formatDate } from '../utils/format';
import { prefetchShow, type ReleaseInfo, type ReleaseMap } from '../hooks/useSofData';

interface ShowListProps {
  year: number;
  shows: Show[];
  selectedId: string;
  playingShowId?: string;
  compact: boolean;
  onSelect: (show: Show) => void;
  /**
   * date → official-release info. Keyed by `show.id` (which is the ISO date
   * `YYYY-MM-DD`, matching the backend's `_id` for releases_cache). Pass a
   * STABLE reference — React.memo shallow-compares this prop.
   */
  releaseMap?: ReleaseMap;
  /**
   * Set of show dates the logged-in visitor has previously listened to. Same
   * stability caveat as `releaseMap`.
   */
  listenedDates?: ReadonlySet<string>;
}

function ShowListImpl({
  year,
  shows,
  selectedId,
  playingShowId,
  compact,
  onSelect,
  releaseMap,
  listenedDates
}: ShowListProps) {
  return (
    <section
      aria-label={`Shows in ${year}`}
      className="flex h-full w-full shrink-0 flex-col border-r border-line bg-surface md:w-[300px] lg:w-[340px]">
      
      <div className="flex items-baseline justify-between border-b border-line px-4 py-2.5">
        <h2 className="font-display text-base font-bold text-chalk">{year}</h2>
        <p className="text-[10px] uppercase tracking-[0.14em] text-muted">
          {shows.length} shows
        </p>
      </div>

      <ul className="flex-1 overflow-y-auto">
        {shows.map((show) => {
          const isSelected = show.id === selectedId;
          const date = formatDate(show.date);
          const release: ReleaseInfo | undefined = releaseMap?.[show.id];
          const listened = listenedDates?.has(show.id) ?? false;
          return (
            <li key={show.id}>
              <button
                type="button"
                onClick={() => onSelect(show)}
                onMouseEnter={() => prefetchShow(show.id)}
                onFocus={() => prefetchShow(show.id)}
                aria-current={isSelected ? 'true' : undefined}
                className={`flex w-full items-start gap-3 border-b border-line/60 border-l-2 text-left transition-colors duration-100 ease-archive ${
                compact ? 'px-4 py-2' : 'px-4 py-3'} ${

                isSelected ?
                'border-l-accent bg-surface2' :
                'border-l-transparent hover:bg-surface2/50'}`
                }>
                
                <div className="min-w-0 flex-1">
                  <p
                    className={`font-mono text-xs tabular-nums ${
                    isSelected ? 'text-chalk' : 'text-ink'}`
                    }>
                    
                    {date.numeric}
                    {listened && (
                      <span
                        className="ml-2 inline-flex items-center text-moss"
                        title="You've listened to this show"
                        aria-label="Previously listened"
                      >
                        <CheckIcon className="h-3 w-3" strokeWidth={2.5} />
                      </span>
                    )}
                    {show.id === playingShowId &&
                    <span className="ml-2 text-[10px] uppercase tracking-[0.12em] text-accent">
                        Playing
                      </span>
                    }
                  </p>
                  {release && (
                    <p
                      className="mt-0.5 inline-flex items-center gap-1 rounded-sm border border-gold/40 px-1.5 py-[1px] text-[9px] uppercase tracking-[0.1em] text-gold"
                      title={`Officially released as ${release.name}${release.year ? ` (${release.year})` : ''}`}
                    >
                      <DiscAlbumIcon className="h-2.5 w-2.5" />
                      {release.short}
                    </p>
                  )}
                  <p
                    className={`truncate font-display text-sm ${
                    isSelected ? 'text-chalk' : 'text-ink/90'}`
                    }>
                    
                    {show.venue}
                  </p>
                  {!compact &&
                  <p className="truncate text-[11px] text-muted">{show.city}</p>
                  }
                </div>
                <div className="flex shrink-0 flex-col items-end gap-1 pt-0.5">
                  {show.avgRating > 0 && (
                    <span className="flex items-center gap-1 font-mono text-[11px] tabular-nums text-gold">
                      <StarIcon className="h-3 w-3 fill-current" />
                      {show.avgRating.toFixed(1)}
                    </span>
                  )}
                  {show.soundboard && (
                    <span
                      className="flex items-center gap-1 text-[9px] uppercase tracking-[0.1em] text-royal-bright"
                      title="Soundboard source"
                    >
                      <RadioIcon className="h-2.5 w-2.5" />
                      SBD
                    </span>
                  )}
                </div>
              </button>
            </li>);

        })}
      </ul>
    </section>);

}

export const ShowList = React.memo(ShowListImpl);
