import React, { useMemo, useState } from 'react';
import { ChevronDownIcon, ChevronRightIcon } from 'lucide-react';
import { useAuth } from '../auth-and-social';
import type { ListenRow } from './api';
import { useHistoryData } from './useSectionData';
import {
  EmptyState,
  ErrorBlock,
  LoadingBlock,
  PanelHeader,
  PanelShell,
  fmtHMS,
} from './_shared';

interface Group {
  showDate: string;
  totalSeconds: number;
  latestTs: string;
  tracks: { title: string; count: number; seconds: number }[];
}

function groupByShow(rows: ListenRow[]): Group[] {
  const order: string[] = [];
  const map = new Map<string, Group>();
  for (const r of rows) {
    const key = r.show_date || r.show_id || '—';
    if (!map.has(key)) {
      map.set(key, { showDate: key, totalSeconds: 0, latestTs: r.ts || '', tracks: [] });
      order.push(key);
    }
    const g = map.get(key)!;
    g.totalSeconds += r.seconds || 0;
    if ((r.ts || '') > g.latestTs) g.latestTs = r.ts || g.latestTs;
    const title = r.track_title || r.track_id || '—';
    const last = g.tracks[g.tracks.length - 1];
    if (last && last.title === title) {
      last.count += 1;
      last.seconds += r.seconds || 0;
    } else {
      g.tracks.push({ title, count: 1, seconds: r.seconds || 0 });
    }
  }
  return order.map((k) => map.get(k)!);
}

interface HistoryPanelProps {
  onOpenShow: (date: string) => void;
}

export function HistoryPanel({ onOpenShow }: HistoryPanelProps) {
  const { user } = useAuth();
  const { data, loading, error } = useHistoryData(!!user);
  const groups = useMemo(() => (data ? groupByShow(data) : []), [data]);
  const [open, setOpen] = useState<Record<string, boolean>>({});

  // Auto-open the most recent show once data lands.
  const [primed, setPrimed] = useState(false);
  React.useEffect(() => {
    if (primed || groups.length === 0) return;
    setOpen({ [groups[0].showDate]: true });
    setPrimed(true);
  }, [groups, primed]);

  const header = (
    <PanelHeader
      eyebrow="Recent"
      title="Listening History"
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
        <EmptyState title="Log in to see your history" hint="Every listen you've logged shows up here" />
      </PanelShell>
    );
  }
  if (loading) return <PanelShell header={header}><LoadingBlock /></PanelShell>;
  if (error && error !== 'unauthorized') return <PanelShell header={header}><ErrorBlock message={error} /></PanelShell>;
  if (groups.length === 0) {
    return (
      <PanelShell header={header}>
        <EmptyState title="No history yet" hint="Play a show and it will land here" />
      </PanelShell>
    );
  }

  return (
    <PanelShell header={header}>
      <ul className="divide-y divide-line">
        {groups.map((g, idx) => {
          const isOpen = !!open[g.showDate];
          return (
            <li key={g.showDate + idx} className={idx % 2 === 0 ? 'bg-bg' : 'bg-surface/50'}>
              <div className="flex items-center gap-3 px-4 py-2.5 md:px-6">
                <button
                  type="button"
                  onClick={() => setOpen((o) => ({ ...o, [g.showDate]: !o[g.showDate] }))}
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
                  onClick={() => onOpenShow(g.showDate)}
                  className="flex min-w-0 flex-1 items-baseline gap-3 text-left"
                >
                  <span className="shrink-0 font-mono text-[12px] tabular-nums text-chalk underline decoration-accent/40 underline-offset-2 hover:decoration-accent">
                    {g.showDate}
                  </span>
                  <span className="text-[10px] uppercase tracking-[0.14em] text-muted">
                    {g.tracks.length} track{g.tracks.length === 1 ? '' : 's'} · {fmtHMS(g.totalSeconds)}
                  </span>
                </button>
              </div>

              {isOpen && (
                <ul className="border-t border-line/60 bg-surface2/60">
                  {g.tracks.map((t, i) => (
                    <li
                      key={`${t.title}-${i}`}
                      className="flex items-center gap-3 px-10 py-1.5 text-[12px] md:px-14"
                    >
                      <span className="min-w-0 flex-1 truncate text-ink">
                        {t.title}
                        {t.count > 1 && (
                          <span className="ml-2 font-mono text-[9px] tabular-nums text-royal-bright">
                            x{t.count}
                          </span>
                        )}
                      </span>
                      <span className="font-mono text-[10px] tabular-nums text-muted">
                        {fmtHMS(t.seconds)}
                      </span>
                    </li>
                  ))}
                </ul>
              )}
            </li>
          );
        })}
      </ul>
    </PanelShell>
  );
}
