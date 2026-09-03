// Per-song scatter plot: X = fractional year (1965–1995), Y = duration,
// dot size = review count, dot colour = source (SBD/AUD/MTX/FOB/UNK).
// Ported from classic buildObsChart() duration mode.

import React, { useMemo, useState } from 'react';
import { useHeatmap, useScatter } from './useObservatory';
import type { Performance } from './api';

interface Props {
  songId: string;
  onBack: () => void;
  onSelectSong: (songId: string) => void;
  onOpenShow?: (date: string) => void;
}

const SRC_COLORS: Record<string, string> = {
  SBD: '#c8a84b',
  AUD: '#2d65e0',
  MTX: '#7c4dff',
  FOB: '#2a9d6f',
  UNK: '#5a6e8a',
};

function dateToDecimal(dateStr: string): number {
  const [y, m, d] = dateStr.split('-').map(Number);
  return (y || 0) + ((m || 1) - 1) / 12 + ((d || 1) - 1) / 365;
}

function fmtDur(secs: number): string {
  const m = Math.floor(secs / 60);
  const s = Math.round(secs % 60);
  return `${m}:${String(s).padStart(2, '0')}`;
}

interface HoverInfo {
  p: Performance;
  x: number;
  y: number;
}

const Scatter: React.FC<Props> = ({ songId, onBack, onSelectSong, onOpenShow }) => {
  const { data: heatmap } = useHeatmap();
  const { data, loading, error, reload } = useScatter(songId);
  const [hover, setHover] = useState<HoverInfo | null>(null);

  const allSongs = heatmap?.songs || [];
  const curIdx = allSongs.findIndex(s => s.song_id === songId);
  const prev = curIdx > 0 ? allSongs[curIdx - 1] : null;
  const next = curIdx >= 0 && curIdx < allSongs.length - 1 ? allSongs[curIdx + 1] : null;

  const perfs = data?.performances || [];

  const geom = useMemo(() => {
    if (!perfs.length) return null;
    const decimals = perfs.map(p => dateToDecimal(p.date));
    const minDec = Math.min(...decimals);
    const maxDec = Math.max(...decimals);
    const xMin = Math.min(minDec, 1965);
    const xMax = Math.max(maxDec, 1995);
    const xSpan = Math.max(xMax - xMin, 1);
    const durs = perfs.map(p => p.duration);
    const minDur = Math.min(...durs);
    const maxDur = Math.max(...durs);
    const durSpan = Math.max(maxDur - minDur, 1);
    const maxRev = Math.max(...perfs.map(p => p.reviews), 1);
    return { decimals, xMin, xSpan, minDur, maxDur, durSpan, maxRev };
  }, [perfs]);

  const nav = (
    <div className="flex flex-wrap items-center gap-2 mb-3">
      <button
        onClick={onBack}
        className="px-2 py-1 text-[11px] font-mono border border-line text-ink hover:bg-surface2"
      >
        ← All songs
      </button>
      <span
        className="flex-1 font-display font-bold text-chalk text-sm truncate"
      >
        {data?.song || songId}
      </span>
      {prev && (
        <button
          onClick={() => onSelectSong(prev.song_id)}
          className="px-2 py-1 text-[11px] font-mono border border-line text-ink hover:bg-surface2"
        >
          ‹ {prev.label}
        </button>
      )}
      {next && (
        <button
          onClick={() => onSelectSong(next.song_id)}
          className="px-2 py-1 text-[11px] font-mono border border-line text-ink hover:bg-surface2"
        >
          {next.label} ›
        </button>
      )}
    </div>
  );

  if (loading) {
    return (
      <div>
        {nav}
        <div className="p-6 text-muted font-mono text-xs">
          Scanning the archive — this may take a moment
        </div>
      </div>
    );
  }
  if (error) {
    return (
      <div>
        {nav}
        <div className="p-6 text-muted font-mono text-xs">
          {error}
          <br />
          <button
            onClick={reload}
            className="mt-3 px-3 py-1 border border-line text-ink hover:bg-surface2"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }
  if (!perfs.length || !geom) {
    return (
      <div>
        {nav}
        <div className="p-6 text-muted font-mono text-xs">
          No duration data cached for &ldquo;{data?.song}&rdquo; yet — background indexing may still be running.
        </div>
      </div>
    );
  }

  const CHART_H = 260;
  const yLabels = [0, 0.25, 0.5, 0.75, 1].map(f => {
    const sec = geom.minDur + f * geom.durSpan;
    return { pct: f * 90 + 5, label: fmtDur(sec) };
  });
  const xTicks: number[] = [];
  for (let y = 1965; y <= 1995; y += 5) xTicks.push(y);

  const avgDur = Math.round(perfs.reduce((a, p) => a + p.duration, 0) / perfs.length);
  const longest = perfs.reduce((a, b) => (b.duration > a.duration ? b : a));
  const shortest = perfs.reduce((a, b) => (b.duration < a.duration ? b : a));

  return (
    <div className="text-ink">
      {nav}
      {/* Legend */}
      <div className="flex flex-wrap items-center gap-3 mb-3 text-[10px] font-mono text-muted">
        {(['SBD', 'AUD', 'MTX', 'FOB', 'UNK'] as const).map(k => (
          <div key={k} className="flex items-center gap-1">
            <span
              className="inline-block w-2 h-2 rounded-full"
              style={{ background: SRC_COLORS[k] }}
            />
            {k}
          </div>
        ))}
        <span className="ml-2">dot size = review count</span>
      </div>

      {/* Chart */}
      <div className="relative bg-surface2/40 border border-line" style={{ height: CHART_H }}>
        {yLabels.map(({ pct, label }) => (
          <span
            key={pct}
            className="absolute left-1 text-[9px] font-mono text-muted"
            style={{ bottom: `${pct}%` }}
          >
            {label}
          </span>
        ))}
        {xTicks.map(y => {
          const xPct = ((y - geom.xMin) / geom.xSpan) * 94 + 3;
          return (
            <span
              key={y}
              className="absolute bottom-0 -translate-x-1/2 text-[9px] font-mono text-muted"
              style={{ left: `${xPct}%` }}
            >
              {y}
            </span>
          );
        })}
        {perfs.map((p, i) => {
          const xPct = ((geom.decimals[i] - geom.xMin) / geom.xSpan) * 94 + 3;
          const yPct =
            geom.durSpan > 0 ? ((p.duration - geom.minDur) / geom.durSpan) * 90 + 5 : 50;
          const sz = Math.max(5, Math.min(16, 5 + (p.reviews / geom.maxRev) * 11));
          const col = SRC_COLORS[p.source] || SRC_COLORS.UNK;
          return (
            <button
              key={`${p.id}-${i}`}
              className="absolute rounded-full cursor-pointer hover:brightness-125"
              style={{
                left: `${xPct}%`,
                bottom: `${yPct}%`,
                width: sz,
                height: sz,
                marginLeft: -sz / 2,
                marginBottom: -sz / 2,
                background: col,
              }}
              onMouseEnter={e => setHover({ p, x: e.clientX, y: e.clientY })}
              onMouseMove={e =>
                setHover(h => (h ? { ...h, x: e.clientX, y: e.clientY } : h))
              }
              onMouseLeave={() => setHover(null)}
              onClick={() => onOpenShow?.(p.date)}
              aria-label={`${p.date} ${p.source}`}
            />
          );
        })}
      </div>

      {/* Stat row */}
      <div className="grid grid-cols-4 gap-2 mt-3 font-mono text-center">
        <Stat val={String(perfs.length)} label="Performances" />
        <Stat val={fmtDur(avgDur)} label="Avg Duration" />
        <Stat val={fmtDur(longest.duration)} label={`Longest · ${longest.date}`} />
        <Stat val={fmtDur(shortest.duration)} label={`Shortest · ${shortest.date}`} />
      </div>

      {hover && (
        <div
          className="pointer-events-none fixed z-[100] px-2 py-1 bg-bg border border-line text-[11px] font-mono text-ink shadow-lg"
          style={{ left: hover.x + 14, top: hover.y - 32 }}
        >
          <strong className="text-accent">{hover.p.date}</strong> · {fmtDur(hover.p.duration)} ·{' '}
          {hover.p.source} · {hover.p.reviews} review{hover.p.reviews === 1 ? '' : 's'}
        </div>
      )}
    </div>
  );
};

const Stat: React.FC<{ val: string; label: string }> = ({ val, label }) => (
  <div className="bg-surface2/40 border border-line p-2">
    <div className="text-chalk text-lg font-bold">{val}</div>
    <div className="text-muted text-[9px] tracking-wider mt-0.5">{label}</div>
  </div>
);

export default Scatter;
