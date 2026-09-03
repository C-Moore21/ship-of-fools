import React, { useEffect, useMemo, useState } from 'react';
import {
  fetchTodayInHistory,
  groupTodayByShow,
  type TodayGroup,
  type TodayRecording,
} from './api';

export interface TodayInHistoryModalProps {
  open: boolean;
  onClose: () => void;
  /** Host UI navigates its main list to this show date. */
  onOpenShow?: (date: string) => void;
}

const SRC_BADGE_COLOR: Record<string, string> = {
  SBD: 'bg-royal/25 text-royal-bright',
  AUD: 'bg-gold/25 text-gold-light',
  MTX: 'bg-violet/25 text-violet',
  FM: 'bg-moss/25 text-moss',
  UNK: 'bg-line text-muted',
};

function SourceBadge({ type }: { type: string }) {
  const cls = SRC_BADGE_COLOR[type] || SRC_BADGE_COLOR.UNK;
  return (
    <span
      className={`inline-flex h-[18px] items-center rounded-sm px-1.5 font-mono text-[9px] tracking-wider ${cls}`}
    >
      {type || 'UNK'}
    </span>
  );
}

function RecRow({
  rec,
  onOpen,
}: {
  rec: TodayRecording;
  onOpen: (date: string) => void;
}) {
  const reviews = rec.avg_rating
    ? `${Number(rec.avg_rating).toFixed(1)}★ ${rec.num_reviews}r`
    : 'no reviews';
  const listens = (rec.community_listens ?? 0) > 0 ? ` · ${rec.community_listens}🎧` : '';
  return (
    <div className="grid grid-cols-[auto_1fr_auto_auto_auto] items-center gap-2 border-t border-line px-3 py-2 text-[11px]">
      <SourceBadge type={rec.source_type} />
      <div className="truncate font-mono text-[10px] text-muted">{rec.identifier}</div>
      <div className="font-mono text-[10px] text-muted">
        {reviews}
        {listens}
      </div>
      <div className="font-mono text-[10px] text-ink">{rec.score.toFixed(1)}</div>
      <button
        type="button"
        onClick={() => onOpen(rec.id)}
        className="flex h-6 w-6 items-center justify-center rounded-sm bg-accent text-chalk transition-colors hover:bg-accent-hover"
        title="Open this show"
      >
        ▶
      </button>
    </div>
  );
}

function Group({
  group,
  isOpen,
  onToggle,
  onOpen,
}: {
  group: TodayGroup;
  isOpen: boolean;
  onToggle: () => void;
  onOpen: (date: string) => void;
}) {
  return (
    <div className="rounded-sm border border-line bg-surface">
      <button
        type="button"
        onClick={onToggle}
        className="grid w-full grid-cols-[auto_auto_auto_1fr_auto] items-center gap-3 px-3 py-2 text-left"
      >
        <span className="font-mono text-[11px] text-muted">{isOpen ? '▾' : '▸'}</span>
        <span className="font-display text-lg text-chalk">{group.year}</span>
        <SourceBadge type={group.best.source_type} />
        <span className="truncate text-[12px] text-ink">
          {group.best.venue?.name || group.best.venue?.location || ''}
        </span>
        <span className="font-mono text-[10px] text-muted">{group.best.score.toFixed(1)}</span>
      </button>
      {isOpen && (
        <div>
          {group.recs.map((r) => (
            <RecRow key={r.identifier} rec={r} onOpen={onOpen} />
          ))}
        </div>
      )}
    </div>
  );
}

export function TodayInHistoryModal({ open, onClose, onOpenShow }: TodayInHistoryModalProps) {
  const [recs, setRecs] = useState<TodayRecording[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openIds, setOpenIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setRecs(null);
    setError(null);
    setOpenIds(new Set());
    fetchTodayInHistory()
      .then((r) => {
        if (cancelled) return;
        setRecs(r);
        // Auto-open the best (first) group.
        const first = r[0]?.id;
        if (first) setOpenIds(new Set([first]));
      })
      .catch((e) => {
        if (!cancelled) setError(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [open]);

  const groups = useMemo(() => (recs ? groupTodayByShow(recs) : []), [recs]);

  const monthDay = useMemo(() => {
    const d = new Date();
    return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric' });
  }, [open]);

  if (!open) return null;

  const handleOpen = (date: string) => {
    onOpenShow?.(date);
    onClose();
  };

  return (
    <div
      className="fixed inset-0 z-[200] flex items-start justify-center bg-black/70 p-4 md:p-8"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      role="dialog"
      aria-modal="true"
      aria-label="Today in History"
    >
      <div className="mt-8 flex max-h-[85vh] w-full max-w-[720px] flex-col overflow-hidden rounded-sm border border-line bg-bg shadow-2xl">
        <header className="flex items-start justify-between gap-4 border-b border-line px-4 py-3">
          <div>
            <h2 className="font-display text-xl text-chalk">Today in History</h2>
            <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-muted">
              {monthDay} — ranked by quality
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-2xl leading-none text-muted transition-colors hover:text-ink"
            aria-label="Close"
          >
            ×
          </button>
        </header>

        <div className="flex-1 space-y-2 overflow-y-auto p-3">
          {error && (
            <div className="rounded-sm border border-accent/50 bg-accent/10 px-3 py-2 text-[11px] text-accent">
              {error}
            </div>
          )}
          {!error && !recs && (
            <div className="py-8 text-center font-mono text-[11px] text-muted">Loading…</div>
          )}
          {!error && recs && !recs.length && (
            <div className="py-8 text-center text-[11px] text-muted">
              No shows found for today's date.
            </div>
          )}
          {groups.map((g) => (
            <Group
              key={g.showId}
              group={g}
              isOpen={openIds.has(g.showId)}
              onToggle={() =>
                setOpenIds((prev) => {
                  const next = new Set(prev);
                  if (next.has(g.showId)) next.delete(g.showId);
                  else next.add(g.showId);
                  return next;
                })
              }
              onOpen={handleOpen}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
