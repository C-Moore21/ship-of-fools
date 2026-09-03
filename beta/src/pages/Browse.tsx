import React, { useEffect, useMemo, useState } from 'react'
import { DicesIcon, StarIcon, PlayIcon } from 'lucide-react'
import { AppHeader } from '../components/AppHeader'
import { YearRail } from '../components/YearRail'
import { ShowList } from '../components/ShowList'
import { ShowDetail } from '../components/ShowDetail'
import { PlayerBar } from '../audio/PlayerBar'
import { useSofAudio, setSnapHandler } from '../audio/useSofAudio'
import { fetchSourceTracks } from '../audio/engine'
import type { AudioShow, AudioSource } from '../audio/types'
import { useScreenInit } from '../useScreenInit.js'
import { formatDate } from '../utils/format'
import { totalShows as fallbackTotalShows } from '../data/years'
import { useShow, useShowsForYear, useSources, useTodaysPick, useYears } from '../hooks/useSofData'
import { LoungePanel, useLounge } from '../lounge'
import { LoginModal, useAuth } from '../auth-and-social'
import type { Show, Track } from '../types/archive'

type VisualizerMode = 'bars' | 'radial' | 'off'

const TABS = ['Browse', 'Rated', 'Stats', 'History', 'Leaderboard']

interface BrowseProps {
  compact: boolean
  visualizer: VisualizerMode
}

const EMPTY_SHOW: Show = {
  id: '', date: '', venue: '', city: '', era: '', tourRun: '',
  avgRating: 0, ratingCount: 0, listeners: 0, soundboard: false,
  source: '', weather: '', tempF: 0, tracks: [],
}

function showToAudio(s: Show): AudioShow {
  return {
    date: s.date,
    label: `${formatDate(s.date).numeric} · ${s.venue}`,
    venue: { name: s.venue, city: s.city },
  }
}

function TodaysBanner({
  pick, otherSources, onPlay, onSelect,
}: {
  pick: Show
  otherSources: number
  onPlay: (s: Show) => void
  onSelect: (s: Show) => void
}) {
  const date = formatDate(pick.date)
  return (
    <div className="flex shrink-0 items-center gap-4 border-b border-line bg-surface2 px-4 py-2.5 md:px-6">
      <span className="hidden shrink-0 rounded-sm border border-gold/40 bg-gold/10 px-2 py-0.5 text-[9px] uppercase tracking-[0.18em] text-gold-light md:inline">
        Today's Recommended Show
      </span>
      <span className="shrink-0 text-[9px] uppercase tracking-[0.18em] text-gold-light md:hidden">Today</span>
      <button
        type="button"
        onClick={() => onSelect(pick)}
        className="flex min-w-0 flex-1 items-baseline gap-3 text-left transition-colors duration-150 ease-archive hover:text-chalk"
        title="Open this show"
      >
        <span className="shrink-0 font-mono text-[13px] tabular-nums text-chalk">{date.numeric}</span>
        <span className="truncate font-display text-sm text-ink md:text-base">{pick.venue}</span>
        <span className="hidden truncate text-[11px] text-muted lg:inline">{pick.city}</span>
        {otherSources > 0 && (
          <span className="hidden shrink-0 text-[10px] uppercase tracking-[0.14em] text-royal-bright md:inline">
            + {otherSources} other source{otherSources === 1 ? '' : 's'}
          </span>
        )}
      </button>
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

function EmptyState({ message, onRoll }: { message: string; onRoll: () => void }) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-4 px-6 text-center">
      <p className="font-display text-2xl text-ink">{message}</p>
      <button
        type="button"
        onClick={onRoll}
        className="flex items-center gap-2 rounded-sm border border-gold/60 px-4 py-2 text-[11px] uppercase tracking-[0.16em] text-gold transition-colors duration-150 ease-archive hover:border-gold hover:text-gold-light"
      >
        <DicesIcon className="h-3.5 w-3.5" />
        Roll
      </button>
    </div>
  )
}

export function Browse({ compact, visualizer: _visualizer }: BrowseProps) {
  const screenInit = useScreenInit()

  const { data: years } = useYears()
  const totalShowsLive = years?.reduce((n, y) => n + y.shows, 0) || fallbackTotalShows

  const [year, setYear] = useState<number>(screenInit.year ?? 1977)
  const [selectedId, setSelectedId] = useState<string | null>(screenInit.showId ?? null)
  const [mobilePane, setMobilePane] = useState<'list' | 'detail'>('detail')
  const [loginOpen, setLoginOpen] = useState(false)

  const auth = useAuth()
  const lounge = useLounge({ currentUser: auth.user })

  const { data: yearShowsRaw, loading: showsLoading } = useShowsForYear(year)
  const yearShows = useMemo(() => yearShowsRaw ?? [], [yearShowsRaw])

  useEffect(() => {
    if (yearShows.length === 0) return
    if (selectedId && yearShows.some((s) => s.id === selectedId)) return
    setSelectedId(yearShows[0].id)
  }, [yearShows, selectedId])

  const shallowSelected = useMemo(
    () => yearShows.find((s) => s.id === selectedId) ?? null,
    [yearShows, selectedId],
  )

  const { data: hydrated } = useShow(shallowSelected)
  const selected: Show = hydrated ?? shallowSelected ?? EMPTY_SHOW

  const { data: today } = useTodaysPick()

  // Audio engine — real playback (replaces mock usePlayer)
  const audio = useSofAudio()

  // Hook up the audio module's "snap back to playing show" so the classic
  // player-bar behavior (click track/show label -> nav) works here too.
  useEffect(() => {
    setSnapHandler((ctx) => {
      const y = Number(ctx.show.date.slice(0, 4))
      if (!Number.isNaN(y)) setYear(y)
      setSelectedId(ctx.show.date)
      setMobilePane('detail')
    })
    return () => setSnapHandler(null)
  }, [])

  // Fetch the best source for the selected show so we can play from setlist.
  // useShow already picks & attaches, but for `playShow` we need the raw source
  // + AudioTrack[]. We re-fetch tracks via the audio module (which has its own
  // adapter to AudioTrack).
  const { data: sources } = useSources(selected.id || null)
  const primarySource: AudioSource | null = useMemo(() => {
    if (!sources || sources.length === 0) return null
    const best = [...sources].sort((a, b) => (b.rating ?? -1) - (a.rating ?? -1))[0]
    return { id: best.id, source_type: best.soundboard ? 'SBD' : 'AUD', title: best.label }
  }, [sources])

  const tourShows = useMemo(
    () => yearShows.filter((s) => s.id !== selected.id).slice(0, 6),
    [yearShows, selected.id],
  )

  const selectShow = (show: Show) => {
    setSelectedId(show.id)
    setMobilePane('detail')
  }

  const rollGambler = () => {
    if (yearShows.length === 0) return
    const pick = yearShows[Math.floor(Math.random() * yearShows.length)]
    selectShow(pick)
  }

  async function startPlayback(show: Show, source: AudioSource, trackIdx: number) {
    try {
      const tracks = await fetchSourceTracks(source.id)
      if (tracks.length === 0) return
      const safeIdx = Math.max(0, Math.min(trackIdx, tracks.length - 1))
      audio.playShow(showToAudio(show), source, tracks, safeIdx)
    } catch (e) {
      console.error('playback failed:', e)
    }
  }

  const playShow = async (s: Show) => {
    const y = Number(s.date.slice(0, 4))
    if (!Number.isNaN(y)) setYear(y)
    selectShow(s)
    // Pull the best source for the show we're jumping into.
    try {
      const res = await fetch(`/api/shows/${s.id}/sources`, { credentials: 'include' })
      const data = await res.json()
      const raw = (data.sources || data || [])[0]
      if (!raw) return
      const src: AudioSource = {
        id: raw.identifier || raw.id,
        source_type: raw.source_type,
        title: raw.title,
      }
      await startPlayback(s, src, 0)
    } catch (e) {
      console.error('play show failed:', e)
    }
  }

  const playTrack = (track: Track) => {
    if (!primarySource) return
    const idx = selected.tracks.findIndex((t) => t.id === track.id)
    startPlayback(selected, primarySource, Math.max(0, idx))
  }

  const exitBeta = () => {
    try { localStorage.removeItem('sof_beta') } catch {}
    window.location.href = '/'
  }

  return (
    <div className="flex h-full w-full flex-col bg-bg">
      <AppHeader
        user={auth.user}
        onLoginClick={() => setLoginOpen(true)}
        onLogoutClick={() => auth.logout()}
        onLoungeClick={() => lounge.toggle()}
        loungeUnread={lounge.unread}
        loungeVisible={lounge.member}
        onGambler={rollGambler}
        onExitBeta={exitBeta}
      />

      {today && (
        <TodaysBanner
          pick={today.show}
          otherSources={today.otherSources}
          onPlay={playShow}
          onSelect={selectShow}
        />
      )}

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
              i === 0 ? 'border-accent text-chalk' : 'border-transparent text-muted hover:text-ink'
            }`}
          >
            {tab}
          </button>
        ))}
      </nav>

      <div className="flex min-h-0 flex-1">
        <div className="hidden md:flex">
          <YearRail
            years={years ?? []}
            totalShows={totalShowsLive}
            selected={year}
            onSelect={setYear}
          />
        </div>

        <div className={`${mobilePane === 'list' ? 'flex' : 'hidden'} w-full md:flex md:w-auto`}>
          <ShowList
            year={year}
            shows={yearShows}
            selectedId={selected.id}
            playingShowId={audio.show?.date}
            compact={compact}
            onSelect={selectShow}
          />
        </div>

        <div className={`${mobilePane === 'detail' ? 'flex' : 'hidden'} min-w-0 flex-1 md:flex`}>
          {showsLoading && yearShows.length === 0 ? (
            <div className="flex flex-1 items-center justify-center text-[12px] uppercase tracking-[0.18em] text-muted">
              Loading {year}…
            </div>
          ) : yearShows.length === 0 ? (
            <EmptyState message={`Nothing cached for ${year} yet`} onRoll={rollGambler} />
          ) : !selected.id ? (
            <div className="flex flex-1 items-center justify-center text-[12px] uppercase tracking-[0.18em] text-muted">
              Pick a show
            </div>
          ) : (
            <ShowDetail
              show={selected}
              tourShows={tourShows}
              currentTrackId={audio.track?.id}
              isPlaying={audio.playing && audio.show?.date === selected.id}
              compact={compact}
              onPlay={playTrack}
              onSelectShow={selectShow}
              onBack={() => setMobilePane('list')}
              onRequestLogin={() => setLoginOpen(true)}
            />
          )}
        </div>
      </div>

      <PlayerBar />

      <LoungePanel
        currentUser={auth.user}
        controller={lounge}
        onOpenShow={(date) => {
          const y = Number(date.slice(0, 4))
          if (!Number.isNaN(y)) setYear(y)
          setSelectedId(date)
          setMobilePane('detail')
          if (lounge.open) lounge.toggle()
        }}
      />

      <LoginModal
        open={loginOpen}
        onClose={() => setLoginOpen(false)}
        auth={auth}
      />
    </div>
  )
}
