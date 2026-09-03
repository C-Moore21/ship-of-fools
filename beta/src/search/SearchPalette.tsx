import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react'
import { SearchIcon } from 'lucide-react'
import { useSearch } from './useSearch'
import type { SearchResult, ShowResult, SongResult, VenueResult } from './api'

export interface SearchPaletteProps {
  open: boolean
  onClose: () => void
  onJumpToShow: (date: string) => void
  onJumpToSong: (songId: string) => void
}

interface Group {
  key: 'shows' | 'songs' | 'venues'
  label: string
  items: SearchResult[]
}

const PLACEHOLDER =
  'Search shows, venues, or dates (1977, 1977-05-08, Madison Square Garden)'

function groupResults(results: SearchResult[]): Group[] {
  const shows: ShowResult[] = []
  const songs: SongResult[] = []
  const venues: VenueResult[] = []
  for (const r of results) {
    if (r.kind === 'show') shows.push(r)
    else if (r.kind === 'song') songs.push(r)
    else if (r.kind === 'venue') venues.push(r)
  }
  const groups: Group[] = []
  if (shows.length) groups.push({ key: 'shows', label: 'Shows', items: shows })
  if (songs.length) groups.push({ key: 'songs', label: 'Songs', items: songs })
  if (venues.length) groups.push({ key: 'venues', label: 'Venues', items: venues })
  return groups
}

export function SearchPalette({
  open,
  onClose,
  onJumpToShow,
  onJumpToSong,
}: SearchPaletteProps) {
  const [query, setQuery] = useState('')
  const [activeIdx, setActiveIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement | null>(null)
  const listRef = useRef<HTMLDivElement | null>(null)
  const rowRefs = useRef<(HTMLButtonElement | null)[]>([])

  const { results, loading, error, tooShort } = useSearch(query)
  const groups = useMemo(() => groupResults(results), [results])
  const flat: SearchResult[] = useMemo(
    () => groups.flatMap((g) => g.items),
    [groups],
  )

  // Reset local state whenever the modal opens.
  useEffect(() => {
    if (!open) return
    setQuery('')
    setActiveIdx(0)
    // Focus after paint so the input exists in the DOM.
    const raf = requestAnimationFrame(() => {
      inputRef.current?.focus()
      inputRef.current?.select()
    })
    return () => cancelAnimationFrame(raf)
  }, [open])

  // Clamp / reset active index when results change.
  useEffect(() => {
    setActiveIdx((i) => {
      if (!flat.length) return 0
      if (i >= flat.length) return 0
      return i
    })
  }, [flat.length])

  // Keep the active row scrolled into view.
  useEffect(() => {
    const el = rowRefs.current[activeIdx]
    if (el) el.scrollIntoView({ block: 'nearest' })
  }, [activeIdx])

  const pick = useCallback(
    (r: SearchResult) => {
      if (r.kind === 'show') onJumpToShow(r.id)
      else if (r.kind === 'song') onJumpToSong(r.id)
      else if (r.kind === 'venue') {
        // No dedicated venue route — best-effort: run the venue's name as a
        // fresh query so the user can pick a show from that venue.
        setQuery(r.name)
        return
      }
      onClose()
    },
    [onClose, onJumpToShow, onJumpToSong],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Escape') {
        e.preventDefault()
        onClose()
        return
      }
      if (!flat.length) return
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        setActiveIdx((i) => (i + 1) % flat.length)
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        setActiveIdx((i) => (i <= 0 ? flat.length - 1 : i - 1))
      } else if (e.key === 'Enter') {
        e.preventDefault()
        const r = flat[activeIdx] ?? flat[0]
        if (r) pick(r)
      } else if (e.key === 'Home') {
        e.preventDefault()
        setActiveIdx(0)
      } else if (e.key === 'End') {
        e.preventDefault()
        setActiveIdx(flat.length - 1)
      }
    },
    [flat, activeIdx, pick, onClose],
  )

  if (!open) return null

  // Reset row-ref array each render; children fill it in on mount.
  rowRefs.current = []

  let runningIdx = 0

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Search"
      className="fixed inset-0 z-[100] flex items-start justify-center bg-black/70 pt-4 sm:pt-20"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose()
      }}
    >
      <div
        className="flex max-h-[90dvh] w-full max-w-[92vw] flex-col border-t-[3px] border-royal bg-surface shadow-[0_10px_40px_rgba(0,0,0,0.6)] sm:max-h-[70vh] sm:w-[640px]"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-3 border-b border-line px-4 py-3">
          <SearchIcon className="h-4 w-4 shrink-0 text-muted" aria-hidden />
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Search shows, venues, dates..."
            aria-label="Search"
            autoComplete="off"
            spellCheck={false}
            className="flex-1 bg-transparent font-mono text-base text-ink placeholder:text-muted focus:outline-none"
          />
          <button
            type="button"
            onClick={onClose}
            className="hidden shrink-0 rounded-sm border border-line px-2 py-[2px] font-mono text-[10px] uppercase tracking-[0.15em] text-muted transition-colors hover:border-royal-light/60 hover:text-ink sm:block"
            aria-label="Close search"
          >
            esc
          </button>
        </div>

        <div
          ref={listRef}
          role="listbox"
          aria-label="Search results"
          className="min-h-0 flex-1 overflow-y-auto"
        >
          {error ? (
            <div className="px-4 py-6 text-center font-mono text-xs text-accent">
              {error}
            </div>
          ) : !query.trim() ? (
            <div className="px-4 py-6 text-center font-mono text-xs text-muted">
              {PLACEHOLDER}
            </div>
          ) : tooShort ? (
            <div className="px-4 py-6 text-center font-mono text-xs text-muted">
              Keep typing...
            </div>
          ) : loading && !flat.length ? (
            <div className="px-4 py-6 text-center font-mono text-xs text-muted">
              Searching...
            </div>
          ) : !flat.length ? (
            <div className="px-4 py-6 text-center font-mono text-xs text-muted">
              No results
            </div>
          ) : (
            groups.map((g) => (
              <section key={g.key} className="py-1">
                <div className="px-4 pb-1 pt-2 font-mono text-[10px] uppercase tracking-[0.18em] text-muted">
                  {g.label}
                </div>
                {g.items.map((r) => {
                  const myIdx = runningIdx++
                  const isActive = myIdx === activeIdx
                  return (
                    <ResultRow
                      key={`${r.kind}:${r.id}:${myIdx}`}
                      result={r}
                      active={isActive}
                      onMouseEnter={() => setActiveIdx(myIdx)}
                      onClick={() => pick(r)}
                      registerRef={(el) => {
                        rowRefs.current[myIdx] = el
                      }}
                    />
                  )
                })}
              </section>
            ))
          )}
        </div>

        <div className="hidden items-center justify-between border-t border-line px-4 py-2 font-mono text-[10px] uppercase tracking-[0.15em] text-muted sm:flex">
          <span>
            <kbd className="mr-1 rounded-sm border border-line px-1">↑↓</kbd>
            navigate
          </span>
          <span>
            <kbd className="mr-1 rounded-sm border border-line px-1">↵</kbd>
            open
          </span>
          <span>
            <kbd className="mr-1 rounded-sm border border-line px-1">esc</kbd>
            close
          </span>
        </div>
      </div>
    </div>
  )
}

interface RowProps {
  result: SearchResult
  active: boolean
  onMouseEnter: () => void
  onClick: () => void
  registerRef: (el: HTMLButtonElement | null) => void
}

function ResultRow({ result, active, onMouseEnter, onClick, registerRef }: RowProps) {
  const activeClasses = active
    ? 'bg-surface2 border-l-royal-bright'
    : 'border-l-transparent hover:bg-surface2/60'

  if (result.kind === 'show') {
    return (
      <button
        ref={registerRef}
        type="button"
        role="option"
        aria-selected={active}
        onMouseEnter={onMouseEnter}
        onClick={onClick}
        className={`flex w-full items-baseline gap-4 border-l-2 px-4 py-2 text-left transition-colors ${activeClasses}`}
      >
        <span className="w-[92px] shrink-0 font-mono text-xs text-chalk">
          {result.display_date}
        </span>
        <span className="flex-1 truncate font-display text-sm text-ink">
          {result.venue.name || '—'}
        </span>
        <span className="hidden shrink-0 truncate font-mono text-[11px] text-muted sm:block sm:max-w-[220px]">
          {result.venue.location}
        </span>
      </button>
    )
  }

  if (result.kind === 'song') {
    return (
      <button
        ref={registerRef}
        type="button"
        role="option"
        aria-selected={active}
        onMouseEnter={onMouseEnter}
        onClick={onClick}
        className={`flex w-full items-baseline gap-4 border-l-2 px-4 py-2 text-left transition-colors ${activeClasses}`}
      >
        <span className="w-[92px] shrink-0 font-mono text-[10px] uppercase tracking-[0.15em] text-muted">
          Song
        </span>
        <span className="flex-1 truncate font-display text-sm text-ink">
          {result.label}
        </span>
      </button>
    )
  }

  // venue
  return (
    <button
      ref={registerRef}
      type="button"
      role="option"
      aria-selected={active}
      onMouseEnter={onMouseEnter}
      onClick={onClick}
      className={`flex w-full items-baseline gap-4 border-l-2 px-4 py-2 text-left transition-colors ${activeClasses}`}
    >
      <span className="w-[92px] shrink-0 font-mono text-[10px] uppercase tracking-[0.15em] text-muted">
        Venue
      </span>
      <span className="flex-1 truncate font-display text-sm text-ink">
        {result.name}
      </span>
      <span className="hidden shrink-0 truncate font-mono text-[11px] text-muted sm:block sm:max-w-[220px]">
        {result.location}
      </span>
    </button>
  )
}
