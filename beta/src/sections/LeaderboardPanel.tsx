import React, { useState } from 'react';
import { useAuth } from '../auth-and-social';
import type { LeaderboardPeriod } from './api';
import { useLeaderboardData } from './useSectionData';
import {
  EmptyState,
  ErrorBlock,
  LoadingBlock,
  PanelHeader,
  PanelShell,
  fmtHMS,
} from './_shared';

const PERIODS: { key: LeaderboardPeriod; label: string }[] = [
  { key: 'week', label: 'Week' },
  { key: 'month', label: 'Month' },
  { key: 'all', label: 'All-time' },
];

function rankBadge(i: number): { symbol: string; className: string } {
  if (i === 0) return { symbol: '1', className: 'bg-gold/20 text-gold-light border-gold/40' };
  if (i === 1) return { symbol: '2', className: 'bg-ink/10 text-ink border-line' };
  if (i === 2) return { symbol: '3', className: 'bg-amber/15 text-amber border-amber/30' };
  return { symbol: String(i + 1), className: 'bg-surface2 text-muted border-line' };
}

interface LeaderboardPanelProps {
  onOpenShow: (date: string) => void;
}

export function LeaderboardPanel({ onOpenShow }: LeaderboardPanelProps) {
  const { user } = useAuth();
  const [period, setPeriod] = useState<LeaderboardPeriod>('week');
  const { data, loading, error } = useLeaderboardData(!!user, period);

  const header = (
    <PanelHeader
      eyebrow="Community"
      title="Leaderboard"
      right={
        <div className="flex items-center gap-1 rounded-sm border border-line bg-surface2 p-0.5">
          {PERIODS.map((p) => (
            <button
              key={p.key}
              type="button"
              onClick={() => setPeriod(p.key)}
              className={`rounded-sm px-2 py-1 text-[10px] uppercase tracking-[0.14em] transition-colors duration-150 ease-archive ${
                period === p.key
                  ? 'bg-accent/20 text-chalk'
                  : 'text-muted hover:text-ink'
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      }
    />
  );

  if (loading && !data) return <PanelShell header={header}><LoadingBlock /></PanelShell>;
  if (error && error !== 'unauthorized') return <PanelShell header={header}><ErrorBlock message={error} /></PanelShell>;

  const rows = data?.rows ?? [];
  const controversials = data?.controversials ?? [];

  return (
    <PanelShell header={header}>
      <div className="px-4 py-4 md:px-6 md:py-6">
        {/* Top listeners */}
        <section>
          <h3 className="mb-2 font-display text-sm uppercase tracking-[0.14em] text-ink">
            Top Listeners
          </h3>
          {!user ? (
            <div className="rounded-sm border border-line bg-surface2 px-4 py-6 text-center text-[11px] uppercase tracking-[0.14em] text-muted">
              Log in to see the community leaderboard
            </div>
          ) : rows.length === 0 ? (
            <div className="rounded-sm border border-line bg-surface2 px-4 py-6 text-center text-[11px] uppercase tracking-[0.14em] text-muted">
              No listening logged this {period === 'all' ? 'time' : period}
            </div>
          ) : (
            <ul className="divide-y divide-line rounded-sm border border-line">
              {rows.map((r, i) => {
                const badge = rankBadge(i);
                const isMe = r.username === user;
                return (
                  <li
                    key={r.username || i}
                    className={`flex items-center gap-3 px-3 py-2.5 ${
                      i % 2 === 0 ? 'bg-surface' : 'bg-surface2/60'
                    } ${isMe ? 'ring-1 ring-inset ring-royal/40' : ''}`}
                  >
                    <span
                      className={`flex h-6 w-6 shrink-0 items-center justify-center rounded-sm border font-mono text-[10px] tabular-nums ${badge.className}`}
                    >
                      {badge.symbol}
                    </span>
                    <span className="min-w-0 flex-1 truncate text-[13px] text-ink">
                      {r.display_name || r.username}
                      {isMe && (
                        <span className="ml-2 text-[9px] uppercase tracking-[0.14em] text-royal-bright">
                          you
                        </span>
                      )}
                    </span>
                    <span className="hidden font-mono text-[11px] tabular-nums text-gold-light md:inline">
                      {fmtHMS(r.seconds)}
                    </span>
                    <span className="w-14 shrink-0 text-right font-mono text-[11px] tabular-nums text-ink">
                      {r.shows} <span className="text-[9px] uppercase tracking-[0.12em] text-muted">shows</span>
                    </span>
                    {r.streak > 0 && (
                      <span className="w-10 shrink-0 text-right font-mono text-[11px] tabular-nums text-amber">
                        {r.streak}d
                      </span>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
        </section>

        {/* Controversials */}
        <section className="mt-8">
          <h3 className="font-display text-sm uppercase tracking-[0.14em] text-ink">
            The Controversials
          </h3>
          <p className="mb-2 text-[10px] uppercase tracking-[0.14em] text-muted">
            Shows where the community is most divided
          </p>
          {controversials.length === 0 ? (
            <EmptyState title="Not enough ratings yet" />
          ) : (
            <ul className="divide-y divide-line rounded-sm border border-line">
              {controversials.map((c, i) => (
                <li
                  key={c.show_id}
                  className={`flex items-center gap-3 px-3 py-2 ${
                    i % 2 === 0 ? 'bg-surface' : 'bg-surface2/60'
                  }`}
                >
                  <button
                    type="button"
                    onClick={() => onOpenShow(c.show_id)}
                    className="flex min-w-0 flex-1 items-baseline gap-3 text-left"
                  >
                    <span className="shrink-0 font-mono text-[12px] tabular-nums text-chalk">
                      {c.show_id}
                    </span>
                    <span className="truncate text-[12px] text-ink">{c.venue || '—'}</span>
                  </button>
                  <span
                    className="font-mono text-[11px] tabular-nums text-gold-light"
                    title="Average rating"
                  >
                    {Number(c.avg).toFixed(1)}★
                  </span>
                  <span
                    className="w-14 shrink-0 text-right font-mono text-[11px] tabular-nums text-accent"
                    title="Standard deviation of ratings"
                  >
                    ±{Number(c.stddev).toFixed(1)}
                  </span>
                  <span className="hidden w-14 shrink-0 text-right font-mono text-[10px] tabular-nums text-muted md:inline">
                    {c.count} rtgs
                  </span>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </PanelShell>
  );
}
