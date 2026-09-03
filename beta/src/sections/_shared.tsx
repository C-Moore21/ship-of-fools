import React from 'react';
import { StarIcon } from 'lucide-react';
import { formatClock, formatDate } from '../utils/format';

export function fmtHMS(seconds: number): string {
  if (!seconds || seconds < 0) return '0m';
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

export { formatClock, formatDate };

export function Stars({ value, size = 12 }: { value: number; size?: number }) {
  const rounded = Math.round(value * 2) / 2;
  const cells: React.ReactNode[] = [];
  for (let i = 1; i <= 5; i++) {
    let fill = 0;
    if (rounded >= i) fill = 1;
    else if (rounded >= i - 0.5) fill = 0.5;
    cells.push(
      <span
        key={i}
        className="relative inline-block"
        style={{ width: size, height: size }}
        aria-hidden
      >
        <StarIcon
          className="absolute inset-0 text-gold/25"
          style={{ width: size, height: size }}
        />
        {fill > 0 && (
          <span
            className="absolute inset-0 overflow-hidden"
            style={{ width: fill === 1 ? size : size / 2 }}
          >
            <StarIcon
              className="text-gold fill-current"
              style={{ width: size, height: size }}
            />
          </span>
        )}
      </span>,
    );
  }
  return (
    <span
      className="inline-flex items-center gap-0.5 align-middle"
      aria-label={`${rounded} stars`}
    >
      {cells}
    </span>
  );
}

export function PanelHeader({
  eyebrow,
  title,
  right,
}: {
  eyebrow?: string;
  title: string;
  right?: React.ReactNode;
}) {
  return (
    <div className="flex items-end justify-between gap-4 border-b border-line bg-surface px-4 py-3 md:px-6 md:py-4">
      <div>
        {eyebrow && (
          <div className="text-[10px] uppercase tracking-[0.18em] text-muted">
            {eyebrow}
          </div>
        )}
        <h2 className="font-display text-xl text-ink md:text-2xl">{title}</h2>
      </div>
      {right && <div className="shrink-0">{right}</div>}
    </div>
  );
}

export function PanelShell({
  children,
  header,
}: {
  children: React.ReactNode;
  header: React.ReactNode;
}) {
  return (
    <div className="flex h-full min-h-0 w-full flex-col bg-bg">
      {header}
      <div className="min-h-0 flex-1 overflow-y-auto">{children}</div>
    </div>
  );
}

export function EmptyState({
  title,
  hint,
}: {
  title: string;
  hint?: string;
}) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-2 px-6 py-16 text-center">
      <p className="font-display text-xl text-ink">{title}</p>
      {hint && <p className="text-[11px] uppercase tracking-[0.14em] text-muted">{hint}</p>}
    </div>
  );
}

export function LoadingBlock({ label = 'Loading' }: { label?: string }) {
  return (
    <div className="flex flex-1 items-center justify-center px-6 py-16 text-[11px] uppercase tracking-[0.18em] text-muted">
      {label}…
    </div>
  );
}

export function ErrorBlock({ message }: { message: string }) {
  return (
    <div className="flex flex-1 items-center justify-center px-6 py-16 text-[11px] uppercase tracking-[0.14em] text-accent">
      {message}
    </div>
  );
}
