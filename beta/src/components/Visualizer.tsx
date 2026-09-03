import React, { useEffect, useRef, useState } from 'react';

type VisualizerMode = 'bars' | 'radial';

interface VisualizerProps {
  mode: VisualizerMode;
  active: boolean;
}

const BAR_COUNT = 28;

/** Synthetic level meter — stands in for the analyser node on real audio. */
export function Visualizer({ mode, active }: VisualizerProps) {
  const [levels, setLevels] = useState<number[]>(() => new Array(BAR_COUNT).fill(0.12));
  const frame = useRef(0);

  useEffect(() => {
    if (!active) {
      setLevels(new Array(BAR_COUNT).fill(0.08));
      return;
    }
    let raf = 0;
    const tick = () => {
      frame.current += 1;
      if (frame.current % 4 === 0) {
        setLevels((prev) =>
        prev.map((value, i) => {
          const target =
          0.2 +
          Math.abs(Math.sin(frame.current / 26 + i / 3.4)) * (
          0.55 - Math.abs(i - BAR_COUNT / 2) / (BAR_COUNT * 1.6)) *
          1.5;
          return value + (target - value) * 0.35;
        })
        );
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [active]);

  if (mode === 'radial') {
    return (
      <div className="flex h-8 w-24 items-center justify-center" aria-hidden="true">
        <svg viewBox="0 0 100 100" className="h-8 w-8">
          {levels.slice(0, 20).map((value, i) => {
            const angle = i / 20 * Math.PI * 2;
            const r1 = 22;
            const r2 = 22 + value * 26;
            return (
              <line
                key={i}
                x1={50 + Math.cos(angle) * r1}
                y1={50 + Math.sin(angle) * r1}
                x2={50 + Math.cos(angle) * r2}
                y2={50 + Math.sin(angle) * r2}
                stroke={i % 5 === 0 ? '#e8332a' : '#2d65e0'}
                strokeWidth="4"
                strokeLinecap="round" />);


          })}
        </svg>
      </div>);

  }

  return (
    <div className="flex h-8 w-24 items-end gap-[2px]" aria-hidden="true">
      {levels.map((value, i) =>
      <span
        key={i}
        className={`w-full rounded-[1px] ${i % 7 === 3 ? 'bg-accent' : 'bg-royal-light'}`}
        style={{ height: `${Math.max(6, value * 100)}%`, opacity: 0.5 + value * 0.5 }} />

      )}
    </div>);

}