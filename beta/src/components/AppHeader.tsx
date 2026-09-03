import React from 'react'
import {
  SearchIcon,
  TelescopeIcon,
  RouteIcon,
  CalendarDaysIcon,
  HeadphonesIcon,
  DicesIcon,
  MessageSquareIcon,
} from 'lucide-react'
import { Stealie } from './Stealie'

const LAUNCHERS = [
  { id: 'search', label: 'Search', Icon: SearchIcon },
  { id: 'observatory', label: 'Observatory', Icon: TelescopeIcon },
  { id: 'runs', label: 'Tour Runs', Icon: RouteIcon },
  { id: 'today', label: 'Today in History', Icon: CalendarDaysIcon },
  { id: 'blind', label: 'Blind Test', Icon: HeadphonesIcon },
  { id: 'gambler', label: 'Gambler — random show', Icon: DicesIcon },
]

interface AppHeaderProps {
  user: string | null
  onLoginClick: () => void
  onLogoutClick: () => void
  onLoungeClick: () => void
  loungeUnread: number
  loungeVisible: boolean
  onGambler: () => void
  onExitBeta?: () => void
  onSearchClick?: () => void
  onObservatoryClick?: () => void
  onTodayClick?: () => void
  onBlindTestClick?: () => void
}

function AppHeaderImpl({
  user,
  onLoginClick,
  onLogoutClick,
  onLoungeClick,
  loungeUnread,
  loungeVisible,
  onGambler,
  onExitBeta,
  onSearchClick,
  onObservatoryClick,
  onTodayClick,
  onBlindTestClick,
}: AppHeaderProps) {
  const handlerFor = (id: string): (() => void) | undefined => {
    if (id === 'gambler') return onGambler
    if (id === 'search') return onSearchClick
    if (id === 'observatory') return onObservatoryClick
    if (id === 'today') return onTodayClick
    if (id === 'blind') return onBlindTestClick
    return undefined
  }
  return (
    <header className="flex h-14 shrink-0 items-center gap-6 border-b border-line bg-surface px-4 md:px-6">
      <a href="#browse" className="flex items-center gap-3">
        <Stealie className="h-8 w-8" />
        <span className="leading-none">
          <span className="block font-display text-lg font-black tracking-tight text-chalk">
            Ship of Fools
          </span>
          <span className="mt-[3px] hidden text-[10px] uppercase tracking-[0.18em] text-muted sm:block">
            Grateful Dead · Live Concert Archive
          </span>
        </span>
      </a>

      <button
        type="button"
        onClick={onSearchClick}
        className="group ml-auto hidden h-9 min-w-[260px] items-center gap-2 rounded-sm border border-line bg-bg px-3 text-left text-xs text-muted transition-colors duration-150 ease-archive hover:border-royal-light/60 hover:text-ink lg:flex"
      >
        <SearchIcon className="h-3.5 w-3.5" />
        <span className="truncate">Search shows, venues, dates…</span>
        <kbd className="ml-auto rounded-sm border border-line px-1.5 py-[1px] text-[10px] text-muted">/</kbd>
      </button>

      <nav aria-label="Tools" className="ml-auto flex items-center gap-1 lg:ml-0">
        {LAUNCHERS.map(({ id, label, Icon }) => (
          <button
            key={id}
            type="button"
            title={label}
            aria-label={label}
            onClick={handlerFor(id)}
            className={`flex h-8 w-8 items-center justify-center rounded-sm border border-transparent text-muted transition-colors duration-150 ease-archive hover:border-line hover:bg-surface2 ${
              id === 'gambler' ? 'hover:text-gold-light' : 'hover:text-ink'
            } ${id === 'search' ? 'lg:hidden' : ''}`}
          >
            <Icon className="h-4 w-4" />
          </button>
        ))}
        {loungeVisible && (
          <button
            type="button"
            onClick={onLoungeClick}
            title="The Lounge — private chat"
            aria-label={`The Lounge, ${loungeUnread} unread`}
            className="relative flex h-8 w-8 items-center justify-center rounded-sm border border-transparent text-muted transition-colors duration-150 ease-archive hover:border-line hover:bg-surface2 hover:text-ink"
          >
            <MessageSquareIcon className="h-4 w-4" />
            {loungeUnread > 0 && (
              <span className="absolute right-0.5 top-0.5 flex h-3.5 min-w-[14px] items-center justify-center rounded-full bg-accent px-1 text-[9px] font-bold text-chalk">
                {loungeUnread > 99 ? '99+' : loungeUnread}
              </span>
            )}
          </button>
        )}
      </nav>

      <div className="ml-2 hidden items-center gap-3 border-l border-line pl-4 md:flex">
        {user ? (
          <>
            <span className="text-[11px] text-muted">
              <span className="text-ink">{user}</span>
            </span>
            {onExitBeta && (
              <button
                type="button"
                onClick={onExitBeta}
                title="Return to the classic UI"
                className="text-[11px] uppercase tracking-[0.12em] text-royal-bright transition-colors duration-150 ease-archive hover:text-chalk"
              >
                ← Classic
              </button>
            )}
            <button
              type="button"
              onClick={onLogoutClick}
              className="text-[11px] uppercase tracking-[0.12em] text-muted transition-colors duration-150 ease-archive hover:text-accent"
            >
              Log Out
            </button>
          </>
        ) : (
          <>
            {onExitBeta && (
              <button
                type="button"
                onClick={onExitBeta}
                title="Return to the classic UI"
                className="text-[11px] uppercase tracking-[0.12em] text-royal-bright transition-colors duration-150 ease-archive hover:text-chalk"
              >
                ← Classic
              </button>
            )}
            <button
              type="button"
              onClick={onLoginClick}
              className="rounded-sm border border-accent px-3 py-1 text-[11px] uppercase tracking-[0.12em] text-accent transition-colors duration-150 ease-archive hover:bg-accent hover:text-chalk"
            >
              Log In
            </button>
          </>
        )}
      </div>
    </header>
  )
}

export const AppHeader = React.memo(AppHeaderImpl)
