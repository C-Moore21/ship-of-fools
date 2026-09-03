import React from 'react';
import { RadioIcon, StarIcon } from 'lucide-react';
import type { Show } from '../types/archive';
import { formatDate } from '../utils/format';

interface ShowListProps {
  year: number;
  shows: Show[];
  selectedId: string;
  playingShowId?: string;
  compact: boolean;
  onSelect: (show: Show) => void;
}

export function ShowList({
  year,
  shows,
  selectedId,
  playingShowId,
  compact,
  onSelect
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
          return (
            <li key={show.id}>
              <button
                type="button"
                onClick={() => onSelect(show)}
                aria-current={isSelected ? 'true' : undefined}
                className={`flex w-full items-start gap-3 border-b border-line/60 border-l-2 text-left transition-colors duration-150 ease-archive ${
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
                    {show.id === playingShowId &&
                    <span className="ml-2 text-[10px] uppercase tracking-[0.12em] text-accent">
                        Playing
                      </span>
                    }
                  </p>
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
                  <span className="flex items-center gap-1 font-mono text-[11px] tabular-nums text-gold">
                    <StarIcon className="h-3 w-3 fill-current" />
                    {show.avgRating.toFixed(1)}
                  </span>
                  {show.soundboard &&
                  <span
                    className="flex items-center gap-1 text-[9px] uppercase tracking-[0.1em] text-royal-bright"
                    title="Soundboard source">
                    
                      <RadioIcon className="h-2.5 w-2.5" />
                      SBD
                    </span>
                  }
                </div>
              </button>
            </li>);

        })}
      </ul>
    </section>);

}