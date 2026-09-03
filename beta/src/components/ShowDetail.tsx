import React, { useEffect, useMemo, useRef, useState } from 'react'
import {
  ChevronDownIcon,
  ChevronLeftIcon,
  CloudIcon,
  HeadphonesIcon,
  RadioIcon,
  StarIcon,
} from 'lucide-react'
import type { Show, Track } from '../types/archive'
import { Setlist } from './Setlist'
import { formatDate, formatClock } from '../utils/format'

interface ShowDetailProps {
  show: Show
  tourShows: Show[]
  currentTrackId?: string
  isPlaying: boolean
  compact: boolean
  onPlay: (track: Track) => void
  onSelectShow: (show: Show) => void
  onBack: () => void
}

interface SourceOption {
  id: string
  label: string
  soundboard: boolean
  rating?: number
}

/**
 * Placeholder list of alternate sources. Real API will hand back the full
 * candidate list for a given show date; the picker UI stays identical.
 */
function fakeAlternateSources(show: Show): SourceOption[] {
  const primary: SourceOption = {
    id: 'primary',
    label: show.source,
    soundboard: show.soundboard,
    rating: show.avgRating,
  }
  return [
    primary,
    { id: 'aud-nak', label: 'AUD · Nakamichi 700s', soundboard: false, rating: 4.2 },
    { id: 'sbd-matrix', label: 'SBD Matrix · Miller Remaster', soundboard: true, rating: 4.7 },
    { id: 'aud-schoeps', label: 'AUD · Schoeps MK4 (Silberman)', soundboard: false, rating: 4.1 },
  ]
}

function SourcePicker({ show }: { show: Show }) {
  const sources = useMemo(() => fakeAlternateSources(show), [show])
  const [open, setOpen] = useState(false)
  const [pickedId, setPickedId] = useState<string>('primary')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => setPickedId('primary'), [show.id])

  useEffect(() => {
    if (!open) return
    const off = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', off)
    return () => document.removeEventListener('mousedown', off)
  }, [open])

  const active = sources.find((s) => s.id === pickedId) ?? sources[0]

  return (
    <div ref={ref} className="relative flex items-start gap-2.5">
      <RadioIcon className="mt-[3px] h-3.5 w-3.5 shrink-0 text-muted" />
      <div className="min-w-0 flex-1">
        <dt className="text-[10px] uppercase tracking-[0.14em] text-muted">
          Source · {sources.length} available
        </dt>
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          aria-haspopup="listbox"
          aria-expanded={open}
          className="mt-0.5 flex w-full items-center gap-1.5 text-left"
        >
          <span
            className={`h-1.5 w-1.5 shrink-0 rounded-full ${
              active.soundboard ? 'bg-royal-bright' : 'bg-gold-light'
            }`}
            aria-hidden="true"
          />
          <span className="truncate font-mono text-xs text-ink">{active.label}</span>
          <ChevronDownIcon
            className={`h-3 w-3 shrink-0 text-muted transition-transform duration-150 ease-archive ${
              open ? 'rotate-180 text-ink' : ''
            }`}
          />
        </button>
        {open && (
          <ul
            role="listbox"
            className="absolute left-0 top-full z-30 mt-1 max-h-64 w-full min-w-[260px] overflow-y-auto rounded-sm border border-line bg-surface2 py-1 shadow-lg"
          >
            {sources.map((s) => {
              const isPicked = s.id === pickedId
              return (
                <li key={s.id}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={isPicked}
                    onClick={() => {
                      setPickedId(s.id)
                      setOpen(false)
                    }}
                    className={`flex w-full items-center gap-2 px-3 py-2 text-left transition-colors duration-150 ease-archive ${
                      isPicked ? 'bg-surface text-chalk' : 'text-ink hover:bg-surface/70'
                    }`}
                  >
                    <span
                      className={`h-1.5 w-1.5 shrink-0 rounded-full ${
                        s.soundboard ? 'bg-royal-bright' : 'bg-gold-light'
                      }`}
                      aria-hidden="true"
                    />
                    <span className="min-w-0 flex-1 truncate font-mono text-[11px]">
                      {s.label}
                    </span>
                    {s.rating != null && (
                      <span className="shrink-0 font-mono text-[11px] text-gold">
                        {s.rating.toFixed(1)}
                      </span>
                    )}
                  </button>
                </li>
              )
            })}
          </ul>
        )}
      </div>
    </div>
  )
}

export function ShowDetail({
  show,
  tourShows,
  currentTrackId,
  isPlaying,
  compact,
  onPlay,
  onSelectShow,
  onBack,
}: ShowDetailProps) {
  const date = formatDate(show.date)
  const runtime = show.tracks.reduce((sum, t) => sum + t.duration, 0)

  // Non-source meta cells (Listens intentionally removed per user request).
  const meta = [
    { Icon: CloudIcon, label: 'Show day', value: `${show.weather} · ${show.tempF}°F` },
    { Icon: HeadphonesIcon, label: 'Runtime', value: formatClock(runtime) },
  ]

  return (
    <main className="flex h-full min-w-0 flex-1 flex-col overflow-y-auto bg-bg">
      <div className="mx-auto w-full max-w-[1180px] px-5 pb-16 pt-6 md:px-9">
        <button
          type="button"
          onClick={onBack}
          className="mb-5 flex items-center gap-1 text-[11px] uppercase tracking-[0.14em] text-muted transition-colors duration-150 ease-archive hover:text-ink md:hidden"
        >
          <ChevronLeftIcon className="h-3.5 w-3.5" />
          Shows
        </button>

        <header className="flex flex-wrap items-end justify-between gap-6 border-b border-line pb-6">
          <div className="min-w-0">
            <p className="text-[10px] uppercase tracking-[0.2em] text-muted">
              {show.tourRun} · {show.era}
            </p>
            <h1 className="mt-2 font-display text-4xl font-black leading-none tracking-tight text-chalk md:text-6xl">
              {date.numeric}
            </h1>
            <p className="mt-3 font-display text-xl text-ink md:text-2xl">{show.venue}</p>
            <p className="text-sm text-muted">
              {show.city} · {date.long}
            </p>
          </div>

          <div className="shrink-0 text-right">
            <p className="font-display text-4xl font-black leading-none text-gold">
              {show.avgRating.toFixed(1)}
            </p>
            <div className="mt-1.5 flex justify-end gap-0.5">
              {[1, 2, 3, 4, 5].map((n) => (
                <StarIcon
                  key={n}
                  className={`h-3.5 w-3.5 ${
                    n <= Math.round(show.avgRating) ? 'fill-gold text-gold' : 'text-line'
                  }`}
                />
              ))}
            </div>
            <p className="mt-1 text-[11px] text-muted">{show.ratingCount} ratings</p>
          </div>
        </header>

        <dl className="grid grid-cols-1 gap-x-6 gap-y-4 border-b border-line py-4 sm:grid-cols-2 lg:grid-cols-3">
          <SourcePicker show={show} />
          {meta.map(({ Icon, label, value }) => (
            <div key={label} className="flex items-start gap-2.5">
              <Icon className="mt-[3px] h-3.5 w-3.5 shrink-0 text-muted" />
              <div className="min-w-0">
                <dt className="text-[10px] uppercase tracking-[0.14em] text-muted">{label}</dt>
                <dd className="truncate font-mono text-xs text-ink">{value}</dd>
              </div>
            </div>
          ))}
        </dl>

        <div className="mt-8 grid gap-10 xl:grid-cols-[minmax(0,1fr)_240px]">
          <Setlist
            show={show}
            currentTrackId={currentTrackId}
            isPlaying={isPlaying}
            compact={compact}
            onPlay={onPlay}
          />

          <aside className="space-y-8">
            <section>
              <h3 className="border-b border-line pb-2 text-[10px] uppercase tracking-[0.18em] text-muted">
                Your note
              </h3>
              <p className="pt-3 text-[13px] leading-relaxed text-ink/85">
                {show.note ?? 'No note yet — add one while you listen.'}
              </p>
              <button
                type="button"
                className="mt-3 text-[11px] uppercase tracking-[0.12em] text-royal-bright transition-colors duration-150 ease-archive hover:text-chalk"
              >
                {show.note ? 'Edit note' : 'Add note'}
              </button>
            </section>

            <section>
              <h3 className="border-b border-line pb-2 text-[10px] uppercase tracking-[0.18em] text-muted">
                On this tour
              </h3>
              <ul className="pt-1">
                {tourShows.map((other) => (
                  <li key={other.id}>
                    <button
                      type="button"
                      onClick={() => onSelectShow(other)}
                      className="flex w-full items-baseline gap-2 py-1.5 text-left transition-colors duration-150 ease-archive hover:text-chalk"
                    >
                      <span className="font-mono text-[11px] tabular-nums text-muted">
                        {formatDate(other.date).numeric}
                      </span>
                      <span className="truncate text-[12px] text-ink/80">{other.city}</span>
                      <span className="ml-auto font-mono text-[11px] text-gold">
                        {other.avgRating.toFixed(1)}
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          </aside>
        </div>
      </div>
    </main>
  )
}
