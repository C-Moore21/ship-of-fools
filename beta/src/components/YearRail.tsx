import React from 'react'
import type { YearEntry } from '../types/archive'
import { prefetchShowsForYear } from '../hooks/useSofData'

interface YearRailProps {
  years: YearEntry[]
  totalShows: number
  selected: number
  onSelect: (year: number) => void
}

function YearRailImpl({ years, totalShows, selected, onSelect }: YearRailProps) {
  const max = Math.max(1, ...years.map((y) => y.shows))
  return (
    <nav
      aria-label="Years"
      className="flex h-full w-[92px] shrink-0 flex-col border-r border-line bg-surface"
    >
      <div className="border-b border-line px-3 py-2.5">
        <p className="font-mono text-[10px] leading-tight text-muted">
          <span className="text-ink">{years.length}</span> yrs
        </p>
        <p className="mt-0.5 font-mono text-[10px] leading-tight text-muted">
          <span className="text-ink">{totalShows.toLocaleString()}</span> shows
        </p>
      </div>
      <ul className="flex-1 overflow-y-auto py-1">
        {years.map((entry) => {
          const isSelected = entry.year === selected
          return (
            <li key={entry.year}>
              <button
                type="button"
                onClick={() => onSelect(entry.year)}
                onMouseEnter={() => prefetchShowsForYear(entry.year)}
                onFocus={() => prefetchShowsForYear(entry.year)}
                aria-current={isSelected ? 'true' : undefined}
                className={`flex w-full items-center gap-2 border-l-2 px-3 py-[5px] text-left transition-colors duration-100 ease-archive ${
                  isSelected
                    ? 'border-accent bg-surface2 text-chalk'
                    : 'border-transparent text-muted hover:bg-surface2/60 hover:text-ink'
                }`}
              >
                <span className="font-mono text-xs tabular-nums">{entry.year}</span>
                <span
                  className={`ml-auto h-[3px] rounded-sm ${isSelected ? 'bg-accent' : 'bg-line'}`}
                  style={{ width: `${8 + (entry.shows / max) * 26}px` }}
                  aria-hidden="true"
                />
              </button>
            </li>
          )
        })}
      </ul>
    </nav>
  )
}

export const YearRail = React.memo(YearRailImpl)

