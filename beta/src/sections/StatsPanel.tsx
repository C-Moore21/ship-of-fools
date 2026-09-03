import React, { useMemo, useState } from 'react';
import { useAuth } from '../auth-and-social';
import { useStatsData } from './useSectionData';
import {
  EmptyState,
  ErrorBlock,
  LoadingBlock,
  PanelHeader,
  PanelShell,
  fmtHMS,
} from './_shared';

interface StatsPanelProps {
  onOpenShow: (date: string) => void;
}

function StatTile({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex flex-col gap-1 rounded-sm border border-line bg-surface2 px-3 py-3">
      <div className="font-mono text-xl tabular-nums text-chalk">{value}</div>
      <div className="text-[9px] uppercase tracking-[0.16em] text-muted">{label}</div>
    </div>
  );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <h3 className="mb-2 mt-6 font-display text-sm uppercase tracking-[0.14em] text-ink">
      {children}
    </h3>
  );
}

export function StatsPanel({ onOpenShow }: StatsPanelProps) {
  const { user } = useAuth();
  const [year, setYear] = useState<string>('');
  const { data, loading, error } = useStatsData(!!user, year || undefined);

  // Derive "favorite year" (most seconds by year) from top_shows; endpoint doesn't
  // surface it directly, so we approximate by summing show seconds by year prefix.
  const favoriteYear = useMemo<string | null>(() => {
    if (!data || !data.top_shows.length) return null;
    const bucket = new Map<string, number>();
    for (const s of data.top_shows) {
      const y = (s.show_date || '').slice(0, 4);
      if (!/^\d{4}$/.test(y)) continue;
      bucket.set(y, (bucket.get(y) || 0) + s.seconds);
    }
    let best: [string, number] | null = null;
    for (const kv of bucket) if (!best || kv[1] > best[1]) best = kv;
    return best ? best[0] : null;
  }, [data]);

  const header = (
    <PanelHeader
      eyebrow="Personal"
      title="My Stats"
      right={
        data && data.years.length > 0 ? (
          <select
            value={year}
            onChange={(e) => setYear(e.target.value)}
            className="rounded-sm border border-line bg-surface2 px-2 py-1 font-mono text-[11px] tabular-nums text-ink focus:border-accent focus:outline-none"
          >
            <option value="">All years</option>
            {data.years.map((y) => (
              <option key={y} value={y}>
                {y}
              </option>
            ))}
          </select>
        ) : null
      }
    />
  );

  if (!user) {
    return (
      <PanelShell header={header}>
        <EmptyState title="Log in to see your stats" hint="Your listening rolls up here" />
      </PanelShell>
    );
  }
  if (loading && !data) return <PanelShell header={header}><LoadingBlock /></PanelShell>;
  if (error && error !== 'unauthorized') return <PanelShell header={header}><ErrorBlock message={error} /></PanelShell>;
  if (!data) return <PanelShell header={header}><EmptyState title="No data" /></PanelShell>;

  const distinctShows = data.top_shows.length; // top-N cap = 10 in backend; treat as "top shows"
  const topShow = data.top_shows[0];

  return (
    <PanelShell header={header}>
      <div className="px-4 py-4 md:px-6 md:py-6">
        <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
          <StatTile label="Time Listened" value={fmtHMS(data.total_seconds)} />
          <StatTile label="Sessions" value={data.total_listens.toLocaleString()} />
          <StatTile label="Day Streak" value={data.streak} />
          <StatTile label="Favorite Year" value={favoriteYear ?? '—'} />
        </div>

        {topShow && (
          <div className="mt-4 rounded-sm border border-line bg-surface2/60 px-3 py-3">
            <div className="text-[9px] uppercase tracking-[0.16em] text-muted">Most Listened Show</div>
            <button
              type="button"
              onClick={() => onOpenShow(topShow.show_date)}
              className="mt-1 flex items-baseline gap-3 text-left"
            >
              <span className="font-mono text-sm tabular-nums text-chalk">{topShow.show_date}</span>
              <span className="font-mono text-[11px] tabular-nums text-gold-light">
                {fmtHMS(topShow.seconds)}
              </span>
            </button>
          </div>
        )}

        {data.by_era.length > 0 && (
          <>
            <SectionTitle>By Era</SectionTitle>
            <ul className="space-y-1">
              {(() => {
                const max = data.by_era.reduce((n, e) => Math.max(n, e.seconds), 0) || 1;
                return data.by_era.map((e) => (
                  <li key={e.era} className="flex items-center gap-3 text-[12px]">
                    <span className="w-32 shrink-0 text-ink">{e.era}</span>
                    <span className="relative h-2 flex-1 overflow-hidden rounded-sm bg-surface2">
                      <span
                        className="absolute inset-y-0 left-0 bg-accent/70"
                        style={{ width: `${(100 * e.seconds) / max}%` }}
                      />
                    </span>
                    <span className="w-16 shrink-0 text-right font-mono tabular-nums text-muted">
                      {fmtHMS(e.seconds)}
                    </span>
                  </li>
                ));
              })()}
            </ul>
          </>
        )}

        {data.top_shows.length > 0 && (
          <>
            <SectionTitle>Top Shows</SectionTitle>
            <ul className="divide-y divide-line rounded-sm border border-line">
              {data.top_shows.map((s, i) => (
                <li
                  key={s.show_date}
                  className={`flex items-center gap-3 px-3 py-2 ${i % 2 === 0 ? 'bg-surface' : 'bg-surface2/60'}`}
                >
                  <span className="w-6 shrink-0 font-mono text-[10px] tabular-nums text-muted">
                    {i + 1}
                  </span>
                  <button
                    type="button"
                    onClick={() => onOpenShow(s.show_date)}
                    className="flex-1 text-left font-mono text-[12px] tabular-nums text-ink hover:text-chalk"
                  >
                    {s.show_date}
                  </button>
                  <span className="font-mono text-[11px] tabular-nums text-gold-light">
                    {fmtHMS(s.seconds)}
                  </span>
                </li>
              ))}
            </ul>
          </>
        )}

        {data.top_songs.length > 0 && (
          <>
            <SectionTitle>Top Songs</SectionTitle>
            <ul className="divide-y divide-line rounded-sm border border-line">
              {data.top_songs.slice(0, 10).map((s, i) => (
                <li
                  key={s.norm}
                  className={`flex items-center gap-3 px-3 py-2 ${i % 2 === 0 ? 'bg-surface' : 'bg-surface2/60'}`}
                >
                  <span className="w-6 shrink-0 font-mono text-[10px] tabular-nums text-muted">
                    {i + 1}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-[12px] text-ink">{s.title}</span>
                  <span className="font-mono text-[10px] tabular-nums text-muted">
                    {s.show_count} show{s.show_count === 1 ? '' : 's'}
                  </span>
                  <span className="font-mono text-[11px] tabular-nums text-gold-light">
                    {fmtHMS(s.seconds)}
                  </span>
                </li>
              ))}
            </ul>
          </>
        )}

        <div className="pb-4 text-[9px] uppercase tracking-[0.14em] text-muted">
          Total distinct top shows: {distinctShows}
        </div>
      </div>
    </PanelShell>
  );
}
