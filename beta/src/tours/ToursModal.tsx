// Tour Runs modal — mirrors classic ⟳ launcher. Two views:
//   1. Era accordion + list of runs (default)
//   2. Per-run detail: progress bar, show list, cohort leaderboard
//
// The host provides `open`, `onClose`, and `onOpenShow(date)` so this file
// stays free of app-wide navigation concerns.
import React, { useMemo, useState } from 'react';
import { useTours, useTourProgress } from './useTours';
import type { TourEra, TourRun } from './api';

export interface ToursModalProps {
  open: boolean;
  onClose: () => void;
  /** Navigates the host UI to a specific show date. Closes the modal on use. */
  onOpenShow?: (showDate: string) => void;
}

export function ToursModal({ open, onClose, onOpenShow }: ToursModalProps) {
  const { eras, loading, error } = useTours(open);
  const [selected, setSelected] = useState<{ id: string; name: string } | null>(null);

  // Reset detail view whenever the modal closes.
  React.useEffect(() => { if (!open) setSelected(null); }, [open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[200] flex items-start justify-center bg-black/70 p-4 md:p-8"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
      role="dialog"
      aria-modal="true"
      aria-label="Tour Runs"
    >
      <div className="mt-8 flex max-h-[85vh] w-full max-w-[720px] flex-col overflow-hidden rounded-sm border border-line bg-bg shadow-2xl">
        <header className="flex items-start justify-between gap-4 border-b border-line px-4 py-3">
          <div>
            <h2 className="font-display text-xl text-chalk">Tour Runs</h2>
            <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-muted">
              {selected ? selected.name : 'Multi-night runs across the eras'}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-2xl leading-none text-muted transition-colors hover:text-ink"
            aria-label="Close"
          >×</button>
        </header>

        <div className="flex-1 overflow-y-auto p-3">
          {error && (
            <div className="rounded-sm border border-accent/50 bg-accent/10 px-3 py-2 text-[11px] text-accent">
              {error}
            </div>
          )}
          {!error && loading && !eras && (
            <div className="py-8 text-center font-mono text-[11px] text-muted">Loading…</div>
          )}
          {!error && eras && !selected && (
            <EraList
              eras={eras}
              onOpenRun={(r) => setSelected({ id: r.id, name: r.name })}
            />
          )}
          {!error && selected && (
            <RunDetail
              tourId={selected.id}
              onBack={() => setSelected(null)}
              onOpenShow={(d) => { onOpenShow?.(d); onClose(); }}
            />
          )}
        </div>
      </div>
    </div>
  );
}

// ── Era accordion ─────────────────────────────────────────────────────────
function EraList({ eras, onOpenRun }: {
  eras: TourEra[];
  onOpenRun: (r: TourRun) => void;
}) {
  const [openEra, setOpenEra] = useState<string | null>(eras[0]?.id ?? null);

  if (!eras.length) {
    return <div className="py-8 text-center text-[11px] text-muted">No tours defined.</div>;
  }
  return (
    <div className="flex flex-col gap-2">
      {eras.map(era => {
        const isOpen = openEra === era.id;
        return (
          <div key={era.id} className="rounded-sm border border-line bg-surface">
            <button
              type="button"
              onClick={() => setOpenEra(isOpen ? null : era.id)}
              className="flex w-full items-center justify-between px-3 py-2 text-left"
            >
              <span className="font-display text-base text-chalk">{era.name}</span>
              <span className="font-mono text-[11px] text-muted">{isOpen ? '▾' : '▸'}</span>
            </button>
            {isOpen && (
              <div className="grid grid-cols-1 gap-2 border-t border-line p-2 sm:grid-cols-2">
                {era.runs.map(r => (
                  <RunCard key={r.id} run={r} onOpen={() => onOpenRun(r)} />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function RunCard({ run, onOpen }: { run: TourRun; onOpen: () => void }) {
  const width = run.momentum != null
    ? `${Math.round((run.momentum / 5) * 100)}%`
    : '0%';
  return (
    <button
      type="button"
      onClick={onOpen}
      className="flex flex-col gap-1 rounded-sm border border-line bg-bg p-2 text-left hover:border-royal"
    >
      <div className="text-[12px] font-semibold text-ink">{run.name}</div>
      <div className="font-mono text-[10px] text-muted">
        {run.start.slice(0, 7)} – {run.end.slice(0, 7)}
      </div>
      {run.momentum != null && (
        <div className="mt-1 flex items-center gap-2">
          <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-line">
            <div className="h-full bg-accent" style={{ width }} />
          </div>
          <span className="font-mono text-[10px] text-muted">{run.momentum.toFixed(1)}★</span>
        </div>
      )}
    </button>
  );
}

// ── Run detail ────────────────────────────────────────────────────────────
function RunDetail({
  tourId, onBack, onOpenShow,
}: {
  tourId: string;
  onBack: () => void;
  onOpenShow: (date: string) => void;
}) {
  const { data, loading, error } = useTourProgress(tourId);

  const summary = useMemo(() => {
    if (!data) return null;
    const heardSet = new Set(data.my_progress || []);
    const total = data.shows.length;
    const heard = data.shows.filter(s => heardSet.has(s.date)).length;
    const pct = total > 0 ? Math.round((heard / total) * 100) : 0;
    return { heardSet, total, heard, pct };
  }, [data]);

  return (
    <div className="flex flex-col gap-3">
      <button
        type="button"
        onClick={onBack}
        className="self-start text-[11px] text-muted hover:text-ink"
      >← All Tours</button>

      {error && (
        <div className="rounded-sm border border-accent/50 bg-accent/10 px-3 py-2 text-[11px] text-accent">
          {error}
        </div>
      )}
      {loading && !data && (
        <div className="py-8 text-center font-mono text-[11px] text-muted">Loading…</div>
      )}

      {data && summary && (
        <>
          <div>
            <div className="font-display text-lg text-chalk">{data.tour.name}</div>
            <div className="font-mono text-[10px] text-muted">
              {data.tour.start} – {data.tour.end}
            </div>
          </div>

          <div>
            <div className="h-2 overflow-hidden rounded-full bg-line">
              <div
                className="h-full bg-accent transition-[width] duration-150 ease-archive"
                style={{ width: `${summary.pct}%` }}
              />
            </div>
            <div className="mt-1 text-[11px] text-muted">
              Heard <strong className="text-ink">{summary.heard}</strong> of{' '}
              <strong className="text-ink">{summary.total}</strong> shows ({summary.pct}%)
            </div>
          </div>

          <div>
            <div className="mb-1 font-mono text-[10px] uppercase tracking-widest text-muted">
              Shows in this run
            </div>
            <div className="flex flex-col divide-y divide-line rounded-sm border border-line">
              {data.shows.map(s => {
                const isHeard = summary.heardSet.has(s.date);
                const sub = [s.venue, s.location].filter(Boolean).join(' · ');
                return (
                  <button
                    key={s.date}
                    type="button"
                    onClick={() => onOpenShow(s.date)}
                    className="flex items-center gap-3 px-3 py-1.5 text-left text-[11px] hover:bg-surface"
                  >
                    <span className={isHeard ? 'text-accent' : 'text-muted'}>
                      {isHeard ? '✓' : '○'}
                    </span>
                    <span className="font-mono text-ink">{s.date}</span>
                    <span className="flex-1 truncate text-muted">{sub}</span>
                  </button>
                );
              })}
            </div>
          </div>

          {data.cohort && data.cohort.length > 0 && (
            <div>
              <div className="mb-1 font-mono text-[10px] uppercase tracking-widest text-muted">
                Cohort progress
              </div>
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-left font-mono text-[10px] uppercase text-muted">
                    <th className="py-1">Listener</th>
                    <th className="py-1">Progress</th>
                    <th className="py-1">Last show</th>
                  </tr>
                </thead>
                <tbody>
                  {data.cohort.map(u => (
                    <tr key={u.display_name} className="border-t border-line">
                      <td className="py-1 pr-2 text-ink">{u.display_name}</td>
                      <td className="py-1 pr-2 font-mono text-accent">
                        {u.completed_count} / {summary.total}
                      </td>
                      <td className="py-1 font-mono text-muted">{u.last_show || ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
