// Crow's Nest: canvas-based US map with era-coloured show clusters and a
// year/month/day time scrubber. Uses plain <canvas> — no D3, no Leaflet.
//
// React-specific quirks handled here:
//  * The canvas element is grabbed via useRef and mutated imperatively; React
//    never diff-renders inside <canvas>.
//  * A ResizeObserver on the wrapper syncs canvas.width to the CSS pixel
//    width so the drawing stays sharp on layout changes (modal open, window
//    resize, sidebar toggle).
//  * All non-React mutable state (viewport, drag flags, RAF ids) lives in
//    refs — putting it in useState would trigger redraws we don't want.
//  * The scrubber value is React state so the UI reflects it; every setValue
//    schedules a repaint via the same rAF pump used for playback.

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useShowsMap, useUsStates } from './useObservatory';
import type { GeoJSONFeatureCollection, MapShow } from './api';

interface Props {
  onOpenShow?: (date: string) => void;
}

type Mode = 'year' | 'month' | 'day';

const ERA_COLORS_MAP: Record<string, string> = {
  '1965': '#5a6e8a', '1966': '#5a6e8a', '1967': '#5a6e8a', '1968': '#5a6e8a', '1969': '#5a6e8a',
  '1970': '#2a9d6f', '1971': '#2a9d6f', '1972': '#2a9d6f', '1973': '#2a9d6f', '1974': '#2a9d6f',
  '1975': '#7c4dff',
  '1976': '#1a4bbf', '1977': '#1a4bbf', '1978': '#1a4bbf', '1979': '#1a4bbf',
  '1980': '#c8a84b', '1981': '#c8a84b', '1982': '#c8a84b', '1983': '#c8a84b', '1984': '#c8a84b', '1985': '#c8a84b',
  '1986': '#d97b1a', '1987': '#d97b1a', '1988': '#d97b1a', '1989': '#d97b1a', '1990': '#d97b1a',
  '1991': '#e8332a', '1992': '#e8332a', '1993': '#e8332a', '1994': '#e8332a', '1995': '#e8332a',
};
const ERA_LEGEND: Array<{ label: string; color: string }> = [
  { label: '65–69 Proto', color: '#5a6e8a' },
  { label: '70–74 Classic', color: '#2a9d6f' },
  { label: '75 Hiatus', color: '#7c4dff' },
  { label: '76–79 Comeback', color: '#1a4bbf' },
  { label: '80–85 Regular', color: '#c8a84b' },
  { label: '86–90 Brent', color: '#d97b1a' },
  { label: '91–95 Final', color: '#e8332a' },
];

const MAP_BOUNDS = { lngMin: -130, lngMax: 40, latMin: 15, latMax: 62 };
const H = 360;

interface Viewport { x: number; y: number; scale: number; }

function toMerc(la: number) {
  return Math.log(Math.tan(Math.PI / 4 + (la * Math.PI) / 360));
}

function project(lat: number, lng: number, W: number, vp: Viewport | null): [number, number] {
  const { lngMin, lngMax, latMin, latMax } = MAP_BOUNDS;
  const xBase = ((lng - lngMin) / (lngMax - lngMin)) * W;
  const yBase = H - ((toMerc(lat) - toMerc(latMin)) / (toMerc(latMax) - toMerc(latMin))) * H;
  if (!vp) return [xBase, yBase];
  return [xBase * vp.scale + vp.x, yBase * vp.scale + vp.y];
}

function fitNAViewport(W: number): Viewport {
  const { lngMin, lngMax, latMin, latMax } = MAP_BOUNDS;
  const naCxBase = ((-95 - lngMin) / (lngMax - lngMin)) * W;
  const naCyBase = H - ((toMerc(38) - toMerc(latMin)) / (toMerc(latMax) - toMerc(latMin))) * H;
  const scale = 1.9;
  return { scale, x: W / 2 - naCxBase * scale, y: H / 2 - naCyBase * scale };
}

interface Cluster {
  lat: number;
  lng: number;
  location: string;
  venue: string;
  shows: MapShow[];
}

function clusterShows(shows: MapShow[]): Cluster[] {
  const map: Record<string, Cluster> = {};
  shows.forEach(s => {
    const key = s.venue
      ? s.venue.toLowerCase() + '|' + s.lat.toFixed(1) + ',' + s.lng.toFixed(1)
      : s.lat.toFixed(2) + ',' + s.lng.toFixed(2);
    if (!map[key])
      map[key] = { lat: s.lat, lng: s.lng, location: s.location || '', venue: s.venue || '', shows: [] };
    map[key].shows.push(s);
  });
  return Object.values(map);
}

function drawGeoLayer(
  ctx: CanvasRenderingContext2D,
  gj: GeoJSONFeatureCollection | null,
  W: number,
  vp: Viewport,
  fillColor: string,
  strokeColor: string,
) {
  if (!gj || !gj.features) return;
  ctx.lineJoin = 'round';
  gj.features.forEach(f => {
    const geom = f.geometry;
    if (!geom) return;
    const polys =
      geom.type === 'Polygon'
        ? ([geom.coordinates] as number[][][][])
        : geom.type === 'MultiPolygon'
        ? (geom.coordinates as number[][][][])
        : [];
    polys.forEach(poly => {
      ctx.beginPath();
      poly.forEach(ring => {
        ring.forEach(([lng, lat], i) => {
          const [x, y] = project(lat, lng, W, vp);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.closePath();
      });
      if (fillColor) { ctx.fillStyle = fillColor; ctx.fill('evenodd'); }
      if (strokeColor) { ctx.strokeStyle = strokeColor; ctx.lineWidth = 0.6; ctx.stroke(); }
    });
  });
}

const CrowsNest: React.FC<Props> = ({ onOpenShow }) => {
  const { data: mapRes, loading, error, reload } = useShowsMap();
  const { data: statesGeo } = useUsStates();
  const shows = mapRes?.shows || [];

  // Sorted date list drives the scrubber index.
  const allDates = React.useMemo(
    () => [...shows.map(s => s.date)].sort(),
    [shows],
  );
  const maxIdx = Math.max(0, allDates.length - 1);

  const [scrubIdx, setScrubIdx] = useState(maxIdx);
  const [mode, setMode] = useState<Mode>('year');
  const [playing, setPlaying] = useState(false);
  const [hover, setHover] = useState<{ text: string; x: number; y: number } | null>(null);
  const [drill, setDrill] = useState<Cluster | null>(null);

  useEffect(() => { setScrubIdx(maxIdx); }, [maxIdx]);

  // ── canvas & imperative state ───────────────────────────────────────────
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const vpRef = useRef<Viewport>({ x: 0, y: 0, scale: 1 });
  const vpInitRef = useRef(false);
  const draggingRef = useRef(false);
  const dragStartRef = useRef({ x: 0, y: 0, vpX: 0, vpY: 0, moved: false });
  const scrubIdxRef = useRef(scrubIdx);
  const modeRef = useRef<Mode>(mode);
  const highlightRef = useRef<Cluster | null>(null);
  const playTimerRef = useRef<number | null>(null);

  useEffect(() => { scrubIdxRef.current = scrubIdx; }, [scrubIdx]);
  useEffect(() => { modeRef.current = mode; }, [mode]);
  useEffect(() => { highlightRef.current = drill; }, [drill]);

  const getVisible = useCallback((): MapShow[] => {
    const idx = scrubIdxRef.current;
    if (idx >= maxIdx || !allDates.length) return shows;
    const cutoff = allDates[idx];
    const m = modeRef.current;
    const prefix = m === 'year' ? cutoff.slice(0, 4) : m === 'month' ? cutoff.slice(0, 7) : cutoff;
    return shows.filter(s => {
      const k = m === 'year' ? s.date.slice(0, 4) : m === 'month' ? s.date.slice(0, 7) : s.date;
      return k <= prefix;
    });
  }, [allDates, shows, maxIdx]);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const W = canvas.offsetWidth || 840;
    if (canvas.width !== W) canvas.width = W;
    if (canvas.height !== H) canvas.height = H;
    if (!vpInitRef.current) {
      vpRef.current = fitNAViewport(W);
      vpInitRef.current = true;
    }
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const vp = vpRef.current;

    ctx.fillStyle = '#06111e';
    ctx.fillRect(0, 0, W, H);
    drawGeoLayer(ctx, statesGeo, W, vp, 'rgba(255,255,255,0.07)', 'rgba(255,255,255,0.17)');

    const visible = getVisible();
    const clusters = clusterShows(visible);
    const idx = scrubIdxRef.current;
    const isScrubbing = idx < maxIdx;

    if (isScrubbing && visible.length > 1) {
      ctx.beginPath();
      ctx.strokeStyle = 'rgba(232,51,42,0.15)';
      ctx.lineWidth = 1;
      let first = true;
      visible.forEach(s => {
        const [x, y] = project(s.lat, s.lng, W, vp);
        if (first) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        first = false;
      });
      ctx.stroke();
    }

    clusters.forEach(cl => {
      const [x, y] = project(cl.lat, cl.lng, W, vp);
      if (x < -20 || x > W + 20 || y < -20 || y > H + 20) return;
      const n = cl.shows.length;
      const avgRating = n ? cl.shows.reduce((a, s) => a + (s.rating || 0), 0) / n : 0;
      const latestYr = cl.shows.reduce((a, s) => (s.date > a ? s.date : a), cl.shows[0].date).slice(0, 4);
      const col = ERA_COLORS_MAP[latestYr] || '#5a6e8a';
      const isHilite = highlightRef.current === cl;
      const r = Math.max(4, Math.min(isHilite ? 14 : 10, 4 + (avgRating / 5) * 4 + Math.log(n + 1) * 1.8));
      const scrubCutoff = isScrubbing ? allDates[idx] : null;
      const isNew = !!(scrubCutoff && cl.shows.some(s => s.date === scrubCutoff));

      ctx.shadowBlur = isHilite ? 18 : isNew ? 14 : 8;
      ctx.shadowColor = col;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fillStyle = col + (isNew ? 'ff' : 'cc');
      ctx.fill();
      ctx.shadowBlur = 0;

      if (isHilite) {
        ctx.beginPath();
        ctx.arc(x, y, r + 3, 0, Math.PI * 2);
        ctx.strokeStyle = col;
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      if (n > 1 && r >= 6) {
        const fs = Math.min(10, Math.max(7, r));
        ctx.font = `bold ${fs}px monospace`;
        ctx.fillStyle = '#fff';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(n > 99 ? '99+' : String(n), x, y);
      }
    });
  }, [statesGeo, allDates, maxIdx, getVisible]);

  // Repaint whenever any dependency of `draw` changes.
  useEffect(() => { draw(); }, [draw, scrubIdx, mode, drill, shows, statesGeo]);

  // ResizeObserver on the wrapper — canvas.offsetWidth is CSS-pixels only.
  useEffect(() => {
    const el = wrapRef.current;
    if (!el || typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(() => {
      vpInitRef.current = false; // recompute NA fit on major size changes
      draw();
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [draw]);

  const hitCluster = useCallback((mx: number, my: number): Cluster | null => {
    const canvas = canvasRef.current;
    if (!canvas) return null;
    const W = canvas.width;
    const vp = vpRef.current;
    const clusters = clusterShows(getVisible());
    const thresh = Math.max(12, 16 / vp.scale) * vp.scale;
    let best: Cluster | null = null;
    let bestDist = thresh;
    clusters.forEach(cl => {
      const [x, y] = project(cl.lat, cl.lng, W, vp);
      const d = Math.hypot(x - mx, y - my);
      if (d < bestDist) { bestDist = d; best = cl; }
    });
    return best;
  }, [getVisible]);

  // ── mouse handlers ──────────────────────────────────────────────────────
  const onMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    if (draggingRef.current) {
      const dx = e.clientX - dragStartRef.current.x;
      const dy = e.clientY - dragStartRef.current.y;
      vpRef.current.x = dragStartRef.current.vpX + dx;
      vpRef.current.y = dragStartRef.current.vpY + dy;
      if (Math.hypot(dx, dy) > 3) dragStartRef.current.moved = true;
      draw();
      setHover(null);
      canvas.style.cursor = 'grabbing';
      return;
    }
    const rect = canvas.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvas.width / rect.width);
    const my = (e.clientY - rect.top) * (canvas.height / rect.height);
    const cl = hitCluster(mx, my);
    if (cl) {
      const text =
        cl.shows.length === 1
          ? `${cl.shows[0].date} · ${cl.location}${cl.shows[0].rating ? ` · ★${cl.shows[0].rating}` : ''}`
          : `${cl.venue || cl.location.split(',')[0]} · ${cl.shows.length} shows`;
      setHover({ text, x: e.clientX, y: e.clientY });
      canvas.style.cursor = 'pointer';
    } else {
      setHover(null);
      canvas.style.cursor = 'grab';
    }
  };

  const onMouseDown = (e: React.MouseEvent<HTMLCanvasElement>) => {
    draggingRef.current = true;
    dragStartRef.current = {
      x: e.clientX, y: e.clientY,
      vpX: vpRef.current.x, vpY: vpRef.current.y, moved: false,
    };
  };

  useEffect(() => {
    const up = () => { draggingRef.current = false; };
    window.addEventListener('mouseup', up);
    return () => window.removeEventListener('mouseup', up);
  }, []);

  const onClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (dragStartRef.current.moved) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvas.width / rect.width);
    const my = (e.clientY - rect.top) * (canvas.height / rect.height);
    const cl = hitCluster(mx, my);
    if (!cl) { setDrill(null); return; }
    setHover(null);
    if (cl.shows.length === 1) {
      onOpenShow?.(cl.shows[0].date);
    } else {
      setDrill(cl);
    }
  };

  const onWheel = (e: React.WheelEvent<HTMLCanvasElement>) => {
    e.preventDefault();
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvas.width / rect.width);
    const my = (e.clientY - rect.top) * (canvas.height / rect.height);
    const factor = e.deltaY < 0 ? 1.15 : 0.87;
    const vp = vpRef.current;
    const ns = Math.max(0.5, Math.min(25, vp.scale * factor));
    vp.x = mx - (mx - vp.x) * (ns / vp.scale);
    vp.y = my - (my - vp.y) * (ns / vp.scale);
    vp.scale = ns;
    draw();
  };

  // ── playback ────────────────────────────────────────────────────────────
  const togglePlay = () => {
    if (playing) {
      if (playTimerRef.current !== null) window.clearInterval(playTimerRef.current);
      playTimerRef.current = null;
      setPlaying(false);
      return;
    }
    let idx = scrubIdxRef.current >= maxIdx ? 0 : scrubIdxRef.current;
    setScrubIdx(idx);
    setPlaying(true);
    const ms = mode === 'year' ? 400 : mode === 'month' ? 100 : 30;
    playTimerRef.current = window.setInterval(() => {
      idx++;
      setScrubIdx(idx);
      if (idx >= maxIdx) {
        if (playTimerRef.current !== null) window.clearInterval(playTimerRef.current);
        playTimerRef.current = null;
        setPlaying(false);
      }
    }, ms);
  };

  useEffect(() => () => {
    if (playTimerRef.current !== null) window.clearInterval(playTimerRef.current);
  }, []);

  const resetScrubber = () => {
    if (playTimerRef.current !== null) window.clearInterval(playTimerRef.current);
    playTimerRef.current = null;
    setPlaying(false);
    setScrubIdx(0);
  };

  const fitZoom = () => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    vpRef.current = fitNAViewport(canvas.offsetWidth || 840);
    draw();
  };

  const scrubLabel = (() => {
    if (scrubIdx >= maxIdx || !allDates.length) return 'All';
    const c = allDates[scrubIdx];
    if (mode === 'year') return c.slice(0, 4);
    if (mode === 'month') return c.slice(0, 7);
    return c;
  })();

  if (loading) {
    return <div className="p-6 text-muted font-mono text-xs">Loading map…</div>;
  }
  if (error) {
    return (
      <div className="p-6 text-muted font-mono text-xs">
        Could not load map: {error}
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
  if (mapRes?.pending || !shows.length) {
    return (
      <div className="p-6 text-muted font-mono text-xs">
        Map data is being indexed in the background — should be ready in about a minute.
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

  return (
    <div ref={wrapRef} className="text-ink relative">
      <canvas
        ref={canvasRef}
        className="w-full block bg-[#06111e] border border-line"
        style={{ height: H }}
        onMouseMove={onMouseMove}
        onMouseDown={onMouseDown}
        onMouseLeave={() => setHover(null)}
        onClick={onClick}
        onWheel={onWheel}
      />

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-2 mt-3 font-mono text-[11px]">
        <button
          onClick={togglePlay}
          className="px-2 py-1 border border-line text-ink hover:bg-surface2"
        >
          {playing ? '⏸ Pause' : '▶ Play'}
        </button>
        <input
          type="range"
          min={0}
          max={maxIdx}
          step={1}
          value={scrubIdx}
          onChange={e => setScrubIdx(parseInt(e.target.value, 10))}
          className="flex-1 min-w-[200px] accent-accent"
        />
        <span className="text-chalk min-w-[70px] text-right">{scrubLabel}</span>
        <button
          onClick={resetScrubber}
          className="px-2 py-1 border border-line text-ink hover:bg-surface2"
        >
          ⟳ Reset
        </button>
        <button
          onClick={fitZoom}
          className="px-2 py-1 border border-line text-ink hover:bg-surface2"
        >
          ⊞ Fit
        </button>
      </div>

      <div className="flex justify-between items-center mt-2 flex-wrap gap-2">
        <div className="text-muted font-mono text-[10px]">
          {scrubIdx < maxIdx
            ? `${getVisible().length} shows through ${scrubLabel}`
            : `${shows.length} shows mapped`}
        </div>
        <div className="flex gap-1 items-center">
          <span className="text-[9px] text-muted font-mono">Scrub by:</span>
          {(['year', 'month', 'day'] as Mode[]).map(m => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={
                'px-2 py-0.5 text-[10px] font-mono border border-line ' +
                (mode === m ? 'bg-accent text-white' : 'text-ink hover:bg-surface2')
              }
            >
              {m[0].toUpperCase() + m.slice(1)}
            </button>
          ))}
        </div>
      </div>

      <div className="text-muted font-mono text-[9px] mt-1">
        scroll=zoom · drag=pan · click city=drill in
      </div>

      {/* Era legend */}
      <div className="flex flex-wrap gap-3 mt-2">
        {ERA_LEGEND.map(e => (
          <div key={e.label} className="flex items-center gap-1 text-[10px] font-mono text-muted">
            <span className="inline-block w-2 h-2 rounded-full" style={{ background: e.color }} />
            {e.label}
          </div>
        ))}
      </div>

      {/* Drill panel */}
      {drill && (
        <div className="mt-3 border border-line bg-surface2/40 p-3">
          <div className="flex items-center justify-between mb-2">
            <div>
              <span className="text-chalk font-display font-bold text-sm">
                {drill.location.split(',')[0]}
              </span>
              <span className="text-muted text-[11px] font-mono ml-2">
                {drill.shows.length} shows
              </span>
            </div>
            <button
              onClick={() => setDrill(null)}
              className="text-muted hover:text-ink text-xl leading-none"
              aria-label="Close"
            >
              ×
            </button>
          </div>
          <div className="max-h-52 overflow-y-auto">
            {[...drill.shows]
              .sort((a, b) => a.date.localeCompare(b.date))
              .map(s => (
                <button
                  key={s.date + s.venue}
                  className="w-full text-left px-2 py-1 text-[11px] font-mono text-ink hover:bg-surface2 flex justify-between"
                  onClick={() => onOpenShow?.(s.date)}
                >
                  <span>
                    {s.date}
                    {s.venue ? ` · ${s.venue}` : ''}
                  </span>
                  {s.rating ? <span className="text-accent">★{s.rating}</span> : null}
                </button>
              ))}
          </div>
        </div>
      )}

      {hover && (
        <div
          className="pointer-events-none fixed z-[100] px-2 py-1 bg-bg border border-line text-[11px] font-mono text-ink shadow-lg"
          style={{ left: hover.x + 14, top: hover.y - 32 }}
        >
          {hover.text}
        </div>
      )}
    </div>
  );
};

export default CrowsNest;
