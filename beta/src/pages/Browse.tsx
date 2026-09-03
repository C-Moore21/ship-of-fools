import React, { useMemo, useState } from 'react'
import { DicesIcon, StarIcon, PlayIcon } from 'lucide-react'
import { AppHeader } from '../components/AppHeader'
import { YearRail } from '../components/YearRail'
import { ShowList } from '../components/ShowList'
import { ShowDetail } from '../components/ShowDetail'
import { PlayerBar } from '../components/PlayerBar'
import { shows } from '../data/shows'
import { usePlayer } from '../hooks/usePlayer'
import { useScreenInit } from '../useScreenInit.js'
import { formatDate } from '../utils/format'
import type { Show, Track } from '../types/archive'

type VisualizerMode = 'bars' | 'radial' | 'off'

const TABS = ['Browse', 'Rated', 'Stats', 'History', 'Leaderboard']

interface BrowseProps {
  compact: boolean
  visualizer: VisualizerMode
}

/**
 * Pick a stable "today's" show — highest-rated available for a preview.
 * Real API will replace this with the same signal the classic UI uses.
 */
function pickTodaysShow(): Show {
  return [...shows].sort((a, b) => b.avgRating - a.avgRating)[0]
}

function TodaysBanner({ pick, onPlay }: { pick: Show; onPlay: (s: Show) => void }) {
  const date = formatDate(pick.date)
  return (
    <div className="flex shrink-0 items-center gap-4 border-b border-line bg-surface2 px-4 py-2.5 md:px-6">
      <span className="hidden shrink-0 rounded-sm border border-gold/40 bg-gold/10 px-2 py-0.5 text-[9px] uppercase tracking-[0.18em] text-gold-light md:inline">
        Today's Recommended Show
      </span>
      <span className="shrink-0 text-[9px] uppercase tracking-[0.18em] text-gold-light md:hidden">
        Today
      </span>
      <div className="flex min-w-0 flex-1 items-baseline gap-3">
        <span className="shrink-0 font-mono text-[13px] tabular-nums text-chalk">
          {date.numeric}
        </span>
        <span className="truncate font-display text-sm text-ink md:text-base">
          {pick.venue}
        </span>
        <span className="hidden truncate text-[11px] text-muted lg:inline">{pick.city}</span>
      </div>
      <span className="flex shrink-0 items-center gap-1 font-mono text-[11px] tabular-nums text-gold">
        <StarIcon className="h-3 w-3 fill-current" />
        {pick.avgRating.toFixed(1)}
      </span>
      <button
        type="button"
        onClick={() => onPlay(pick)}
        className="flex shrink-0 items-center gap-1.5 rounded-sm border border-accent/60 bg-accent/10 px-2.5 py-1 text-[10px] uppercase tracking-[0.14em] text-accent transition-colors duration-150 ease-archive hover:bg-accent hover:text-chalk"
      >
        <PlayIcon className="h-3 w-3 fill-current" />
        Play
      </button>
    </div>
  )
}

export function Browse({ compact, visualizer }: BrowseProps) {
  const screenInit = useScreenInit()
  const initialShow = shows.find((s) => s.id === screenInit.showId) ?? shows[0]

  const [year, setYear] = useState<number>(screenInit.year ?? 1977)
  const [selected, setSelected] = useState<Show>(initialShow)
  const [mobilePane, setMobilePane] = useState<'list' | 'detail'>('detail')
  const player = usePlayer(initialShow)

  const todaysPick = useMemo(pickTodaysShow, [])

  const yearShows = useMemo(
    () =>
      shows
        .filter((s) => s.date.startsWith(String(year)))
        .sort((a, b) => a.date.localeCompare(b.date)),
    [year],
  )

  const tourShows = useMemo(
    () =>
      shows.filter((s) => s.tourRun === selected.tourRun && s.id !== selected.id).slice(0, 6),
    [selected],
  )

  const selectShow = (show: Show) => {
    setSelected(show)
    setMobilePane('detail')
  }

  const rollGambler = () => {
    const pick = shows[Math.floor(Math.random() * shows.length)]
    setYear(Number(pick.date.slice(0, 4)))
    selectShow(pick)
    player.play(pick, pick.tracks[0])
  }

  const playShow = (s: Show) => {
    setYear(Number(s.date.slice(0, 4)))
    selectShow(s)
    player.play(s, s.tracks[0])
  }

  const playTrack = (track: Track) => player.play(selected, track)

  const exitBeta = () => {
    try {
      localStorage.removeItem('sof_beta')
    } catch {}
    window.location.href = '/'
  }

  return (
    <div className="flex h-full w-full flex-col bg-bg">
      <AppHeader unread={3} onGambler={rollGambler} onExitBeta={exitBeta} />
      <TodaysBanner pick={todaysPick} onPlay={playShow} />

      <nav
        aria-label="Sections"
        className="flex h-9 shrink-0 items-center gap-1 border-b border-line bg-surface px-3 md:px-5"
      >
        {TABS.map((tab, i) => (
          <button
            key={tab}
            type="button"
            aria-current={i === 0 ? 'page' : undefined}
            className={`h-full border-b-2 px-3 text-[11px] uppercase tracking-[0.14em] transition-colors duration-150 ease-archive ${
              i === 0
                ? 'border-accent text-chalk'
                : 'border-transparent text-muted hover:text-ink'
            }`}
          >
            {tab}
          </button>
        ))}
      </nav>

      <div className="flex min-h-0 flex-1">
        <div className="hidden md:flex">
          <YearRail selected={year} onSelect={setYear} />
        </div>

        <div className={`${mobilePane === 'list' ? 'flex' : 'hidden'} w-full md:flex md:w-auto`}>
          <ShowList
            year={year}
            shows={yearShows}
            selectedId={selected.id}
            playingShowId={player.show?.id}
            compact={compact}
            onSelect={selectShow}
          />
        </div>

        <div className={`${mobilePane === 'detail' ? 'flex' : 'hidden'} min-w-0 flex-1 md:flex`}>
          {yearShows.length === 0 ? (
            <div className="flex flex-1 flex-col items-center justify-center gap-4 px-6 text-center">
              <p className="font-display text-2xl text-ink">Nothing cached for {year} yet</p>
              <p className="max-w-sm text-[13px] leading-relaxed text-muted">
                This year hasn't been pulled from the archive on this device. Roll the Gambler
                for a show that's ready to play.
              </p>
              <button
                type="button"
                onClick={rollGambler}
                className="flex items-center gap-2 rounded-sm border border-gold/60 px-4 py-2 text-[11px] uppercase tracking-[0.16em] text-gold transition-colors duration-150 ease-archive hover:border-gold hover:text-gold-light"
              >
                <DicesIcon className="h-3.5 w-3.5" />
                Roll
              </button>
            </div>
          ) : (
            <ShowDetail
              show={selected}
              tourShows={tourShows}
              currentTrackId={player.track?.id}
              isPlaying={player.isPlaying}
              compact={compact}
              onPlay={playTrack}
              onSelectShow={selectShow}
              onBack={() => setMobilePane('list')}
            />
          )}
        </div>
      </div>

      <PlayerBar
        show={player.show}
        track={player.track}
        isPlaying={player.isPlaying}
        elapsed={player.elapsed}
        visualizer={visualizer}
        onToggle={player.toggle}
        onPrev={player.prev}
        onNext={player.next}
        onSeek={player.seek}
      />
    </div>
  )
}
