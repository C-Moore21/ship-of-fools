// Song × Year rating heatmap grid, ported from the classic UI's buildHeatmap().
// Row = song, column = year (1965–1995). Cell colour = weighted avg rating;
// opacity = archive review volume. Clicking a row label or a cell drills into
// the Scatter view for that song.

import React, { useMemo, useState } from 'react';
import { useHeatmap } from './useObservatory';
import type { HeatmapSong } from './api';

interface Props {
  onSelectSong: (songId: string) => void;
}

const YEARS: number[] = (() => {
  const out: number[] = [];
  for (let y = 1965; y <= 1995; y++) out.push(y);
  return out;
})();

function ratingColor(rating: number, reviews: number): string {
  const t = Math.max(0, Math.min(1, (rating - 1) / 4));
  let r: number, g: number, b: number;
  if (t < 0.5) {
    const u = t * 2;
    r = Math.round(26 + (200 - 26) * u);
    g = Math.round(75 + (168 - 75) * u);
    b = Math.round(191 + (42 - 191) * u);
  } else {
    const u = (t - 0.5) * 2;
    r = Math.round(200 + (232 - 200) * u);
    g = Math.round(168 + (51 - 168) * u);
    b = 42;
  }
  const alpha = Math.min(0.95, 0.25 + (Math.log(reviews + 1) / Math.log(80)) * 0.7);
  return `rgba(${r},${g},${b},${alpha.toFixed(2)})`;
}

interface HoverInfo {
  label: string;
  year: number;
  rating: number;
  reviews: number;
  count: number;
  x: number;
  y: number;
}

const Heatmap: React.FC<Props> = ({ onSelectSong }) => {
  const { data, loading, error, reload } = useHeatmap();
  const [hover, setHover] = useState<HoverInfo | null>(null);

  const sortedSongs: HeatmapSong[] = useMemo(() => {
    const arr = data?.songs || [];
    return [...arr].sort((a, b) => a.label.localeCompare(b.label));
  }, [data]);

  const lookup = useMemo(() => {
    const out: Record<string, Record<string, HeatmapSong['years'][number]>> = {};
    (data?.songs || []).forEach(s => {
      out[s.song_id] = {};
      (s.years || []).forEach(yb => {
        out[s.song_id][String(yb.year)] = yb;
      });
    });
    return out;
  }, [data]);

  if (loading) {
    return <div className="p-6 text-muted font-mono text-xs">Warming up the observatory…</div>;
  }
  if (error) {
    return (
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
    );
  }
  if (!sortedSongs.length) {
    return <div className="p-6 text-muted font-mono text-xs">Heatmap still warming up in the background — check back in a minute.</div>;
  }

  return (
    <div className="text-ink relative">
      {/* Legend */}
      <div className="mb-3">
        <div
          className="h-2 rounded-sm"
          style={{
            background:
              'linear-gradient(to right, rgb(26,75,191), rgb(200,168,42), rgb(232,51,42))',
          }}
        />
        <div className="flex justify-between text-[9px] font-mono tracking-wider mt-1 text-muted">
          <span>Low rated ★1</span>
          <span>Average ★3</span>
          <span>Legendary ★5</span>
        </div>
        <div className="mt-1 text-[10px] text-muted font-mono">
          Opacity = archive review volume · Click any cell or song name to view scatter plot
        </div>
      </div>

      <div className="overflow-x-auto">
        <div className="min-w-[720px]">
          {/* Year header row */}
          <div className="flex items-end">
            <div className="w-40 shrink-0" />
            <div className="flex flex-1">
              {YEARS.map(y => (
                <div
                  key={y}
                  className="flex-1 text-[9px] font-mono text-muted text-center h-4"
                >
                  {y % 5 === 0 ? y : ''}
                </div>
              ))}
            </div>
          </div>

          {sortedSongs.map(s => (
            <div key={s.song_id} className="flex items-center border-b border-line/40">
              <button
                onClick={() => onSelectSong(s.song_id)}
                className="w-40 shrink-0 text-left pr-2 py-1 text-xs text-ink font-mono hover:text-accent truncate"
                title={s.label}
              >
                {s.label}
              </button>
              <div className="flex flex-1 h-5">
                {YEARS.map(y => {
                  const cell = lookup[s.song_id]?.[String(y)];
                  if (!cell || cell.reviews < 1) {
                    return (
                      <div
                        key={y}
                        className="flex-1 mx-[0.5px] bg-surface2/30"
                      />
                    );
                  }
                  const bg = ratingColor(cell.avg_rating, cell.reviews);
                  return (
                    <button
                      key={y}
                      onClick={() => onSelectSong(s.song_id)}
                      onMouseEnter={e =>
                        setHover({
                          label: s.label,
                          year: y,
                          rating: cell.avg_rating,
                          reviews: cell.reviews,
                          count: cell.count,
                          x: e.clientX,
                          y: e.clientY,
                        })
                      }
                      onMouseMove={e =>
                        setHover(h =>
                          h ? { ...h, x: e.clientX, y: e.clientY } : h,
                        )
                      }
                      onMouseLeave={() => setHover(null)}
                      className="flex-1 mx-[0.5px] cursor-pointer hover:brightness-125 transition-[filter]"
                      style={{ background: bg }}
                      aria-label={`${s.label} ${y}`}
                    />
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </div>

      {hover && (
        <div
          className="pointer-events-none fixed z-[100] px-2 py-1 bg-bg border border-line text-[11px] font-mono text-ink shadow-lg"
          style={{ left: hover.x + 14, top: hover.y - 32 }}
        >
          <strong className="text-accent">{hover.label}</strong> · {hover.year} · ★
          {hover.rating} · {hover.reviews} reviews · {hover.count} shows
        </div>
      )}
    </div>
  );
};

export default Heatmap;
