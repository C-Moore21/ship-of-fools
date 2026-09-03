// Observatory modal — top-level shell with three inner tabs (Heatmap,
// Crow's Nest, My Stats). Renders whichever body component is active.
// Integration lives outside this module; consumer supplies `open`/`onClose`.

import React, { useEffect, useState } from 'react';
import Heatmap from './Heatmap';
import Scatter from './Scatter';
import CrowsNest from './CrowsNest';

type Tab = 'heatmap' | 'crows-nest' | 'mystats';

interface Props {
  open: boolean;
  onClose: () => void;
  /** Called when the user clicks through to a specific show (date string). */
  onOpenShow?: (date: string) => void;
  /** Optional initial tab; defaults to 'heatmap'. */
  initialTab?: Tab;
}

const ObservatoryModal: React.FC<Props> = ({
  open,
  onClose,
  onOpenShow,
  initialTab = 'heatmap',
}) => {
  const [tab, setTab] = useState<Tab>(initialTab);
  const [scatterSong, setScatterSong] = useState<string | null>(null);

  // Escape closes.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  // Reset drill-down whenever the tab changes.
  useEffect(() => {
    if (tab !== 'heatmap') setScatterSong(null);
  }, [tab]);

  if (!open) return null;

  const openShow = (date: string) => {
    onOpenShow?.(date);
    onClose();
  };

  return (
    <div
      role="dialog"
      aria-modal="true"
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/70"
      onClick={onClose}
    >
      <div
        onClick={e => e.stopPropagation()}
        className="w-[min(1100px,95vw)] max-h-[90vh] flex flex-col bg-surface border border-line rounded-sm shadow-2xl"
      >
        {/* Header + tabs */}
        <div className="flex items-center justify-between px-4 py-2 border-b border-line">
          <div className="font-display text-chalk text-lg tracking-wide">
            The Observatory
          </div>
          <button
            onClick={onClose}
            aria-label="Close"
            className="text-muted hover:text-ink text-2xl leading-none px-2"
          >
            ×
          </button>
        </div>

        <div className="flex gap-1 px-3 pt-2 border-b border-line bg-bg/40">
          <TabButton active={tab === 'heatmap'} onClick={() => setTab('heatmap')}>
            ⬡ Song Heatmap
          </TabButton>
          <TabButton active={tab === 'crows-nest'} onClick={() => setTab('crows-nest')}>
            🗺 Crow&rsquo;s Nest
          </TabButton>
          <TabButton active={tab === 'mystats'} onClick={() => setTab('mystats')}>
            ◈ My Stats
          </TabButton>
        </div>

        <div className="flex-1 overflow-y-auto p-4">
          {tab === 'heatmap' &&
            (scatterSong ? (
              <Scatter
                songId={scatterSong}
                onBack={() => setScatterSong(null)}
                onSelectSong={id => setScatterSong(id)}
                onOpenShow={openShow}
              />
            ) : (
              <Heatmap onSelectSong={id => setScatterSong(id)} />
            ))}
          {tab === 'crows-nest' && <CrowsNest onOpenShow={openShow} />}
          {tab === 'mystats' && <MyStatsPlaceholder />}
        </div>
      </div>
    </div>
  );
};

const TabButton: React.FC<{
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}> = ({ active, onClick, children }) => (
  <button
    onClick={onClick}
    className={
      'px-3 py-1.5 text-[11px] font-mono tracking-wider border-b-2 -mb-[1px] ' +
      (active
        ? 'border-accent text-chalk'
        : 'border-transparent text-muted hover:text-ink')
    }
  >
    {children}
  </button>
);

// My Stats depends on /api/listens/stats and the classic listening-calendar
// widget — port it separately. Placeholder shows the endpoint contract so the
// integrating agent knows what to wire.
const MyStatsPlaceholder: React.FC = () => (
  <div className="p-6 text-muted font-mono text-xs">
    <div className="text-chalk font-display text-base mb-2">My Stats</div>
    <p>
      This tab is scaffolded but not yet ported from the classic UI. It should
      hit <code className="text-accent">/api/listens/stats</code>{' '}
      (optionally <code>?year=YYYY</code>) and render the era-affinity bar, top
      songs, top shows, and the listening calendar.
    </p>
  </div>
);

export default ObservatoryModal;
