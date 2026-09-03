import React, { useMemo, useState } from 'react';
import { ChevronDownIcon, ChevronRightIcon } from 'lucide-react';
import { useAuth } from '../auth-and-social';
import type { ShowRating, TrackRating } from './api';
import { useRatedData } from './useSectionData';
import {
  EmptyState,
  ErrorBlock,
  LoadingBlock,
  PanelHeader,
  PanelShell,
  Stars,
} from './_shared';

interface GroupedShow {
  date: string;
  venue: string;
  showStars: number;
  tracks: TrackRating[];
}

function groupRatings(shows: ShowRating[], tracks: TrackRating[]): GroupedShow[] {
  const map = new Map<string, GroupedShow>();
  for (const s of shows) {
    map.set(s.show_id, {
      date: s.show_id,
      venue: s.venue || s.show_id,
      showStars: s.stars,
      tracks: [],
    });
  }
  for (const t of tracks) {
    const key = t.show_date;
    if (!key) continue;
    if (!map.has(key)) {
      map.set(key, { date: key, venue: key, showStars: 0, tracks: [] });
    }
    map.get(key)!.tracks.push(t);
  }
  return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
}

interface RatedPanelProps {
  onOpenShow: (date: string) => void;
}

export function RatedPanel({ onOpenShow }: RatedPanelProps) {
  const { user } = useAuth();
  const { data, loading, error } = useRatedData(!!user);
  const [open, setOpen] = useState<Record<string, boolean>>({});

  const groups = useMemo<GroupedShow[]>(
    () => (data ? groupRatings(data.showRatings, data.trackRatings) : []),
    [data],
  );

  const header = (
    <PanelHeader
      eyebrow="My Collection"
      title="Rated Shows & Tracks"
      right={
        groups.length > 0 ? (
          <span className="font-mono text-[11px] tabular-nums text-muted">
            {groups.length} show{groups.length === 1 ? '' : 's'}
          </span>
        ) : null
      }
    />
  );

  if (!user) {
    return (
      <PanelShell header={header}>
        <EmptyState title="Log in to see your rated shows" hint="Track ratings live with your account" />
      </PanelShell>
    );
  }
  if (loading) return <PanelShell header={header}><LoadingBlock /></PanelShell>;
  if (error && error !== 'unauthorized') return <PanelShell header={header}><ErrorBlock message={error} /></PanelShell>;
  if (groups.length === 0) {
    return (
      <PanelShell header={header}>
        <EmptyState title="No ratings yet" hint="Star a show or track to start your collection" />
      </PanelShell>
    );
  }

  return (
    <PanelShell header={header}>
      <ul className="divide-y divide-line">
        {groups.map((g, idx) => {
          const isOpen = !!open[g.date];
          return (
            <li key={g.date} className={idx % 2 === 0 ? 'bg-bg' : 'bg-surface/50'}>
              <div className="flex items-center gap-3 px-4 py-2.5 md:px-6">
                <button
                  type="button"
                  onClick={() => setOpen((o) => ({ ...o, [g.date]: !o[g.date] }))}
                  className="flex shrink-0 items-center justify-center text-muted hover:text-ink"
                  aria-label={isOpen ? 'Collapse' : 'Expand'}
                >
                  {isOpen ? (
                    <ChevronDownIcon className="h-3.5 w-3.5" />
                  ) : (
                    <ChevronRightIcon className="h-3.5 w-3.5" />
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => onOpenShow(g.date)}
                  className="flex min-w-0 flex-1 items-baseline gap-3 text-left"
                >
                  <span className="shrink-0 font-mono text-[12px] tabular-nums text-chalk">
                    {g.date}
                  </span>
                  <span className="truncate font-display text-sm text-ink">{g.venue}</span>
                  {g.tracks.length > 0 && (
                    <span className="hidden shrink-0 text-[10px] uppercase tracking-[0.14em] text-muted md:inline">
                      {g.tracks.length} track{g.tracks.length === 1 ? '' : 's'} rated
                    </span>
                  )}
                </button>
                {g.showStars > 0 && <Stars value={g.showStars} />}
              </div>

              {isOpen && g.tracks.length > 0 && (
                <ul className="border-t border-line/60 bg-surface2/60">
                  {g.tracks.map((t) => (
                    <li
                      key={t.track_id}
                      className="flex items-center gap-3 px-10 py-1.5 text-[12px] md:px-14"
                    >
                      <span className="min-w-0 flex-1 truncate text-ink">
                        {t.track_title || t.track_id}
                      </span>
                      <Stars value={t.stars} size={10} />
                    </li>
                  ))}
                </ul>
              )}
              {isOpen && g.tracks.length === 0 && (
                <div className="border-t border-line/60 bg-surface2/60 px-10 py-2 text-[11px] text-muted md:px-14">
                  No tracks rated
                </div>
              )}
            </li>
          );
        })}
      </ul>
    </PanelShell>
  );
}
