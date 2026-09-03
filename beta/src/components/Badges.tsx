import React from 'react';
import type { Rarity, TrackGap } from '../types/archive';

const RARITY_STYLES: Record<Rarity, string> = {
  common: 'text-muted',
  solid: 'text-ink/70',
  rare: 'text-royal-bright',
  special: 'text-violet',
  legendary: 'text-gold'
};

const RARITY_LABEL: Record<Rarity, string> = {
  common: 'Common',
  solid: 'Solid',
  rare: 'Rare',
  special: 'Special',
  legendary: 'Legendary'
};

/** A rarity tier reads as a small tick mark + label — never a filled pill. */
export function RarityMark({ rarity }: {rarity: Rarity;}) {
  const filled = { common: 1, solid: 2, rare: 3, special: 4, legendary: 5 }[rarity];
  return (
    <span
      className={`inline-flex items-center gap-1.5 text-[10px] uppercase tracking-[0.14em] ${RARITY_STYLES[rarity]}`}
      title={`${RARITY_LABEL[rarity]} — played in ${100 - filled * 18}% of shows`}>
      
      <span className="flex items-end gap-[2px]" aria-hidden="true">
        {[0, 1, 2, 3, 4].map((i) =>
        <span
          key={i}
          className={`w-[2px] rounded-sm ${i < filled ? 'bg-current' : 'bg-line'}`}
          style={{ height: `${4 + i * 2}px` }} />

        )}
      </span>
      {RARITY_LABEL[rarity]}
    </span>);

}

const GAP_STYLES: Record<TrackGap['label'], string> = {
  Debut: 'border-moss/50 text-moss',
  Bust: 'border-gold/50 text-gold',
  Gap: 'border-line text-muted',
  Drought: 'border-amber/50 text-amber',
  Final: 'border-accent/60 text-accent'
};

export function GapBadge({ gap }: {gap: TrackGap;}) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-sm border px-1.5 py-[1px] text-[10px] uppercase tracking-[0.12em] ${GAP_STYLES[gap.label]}`}>
      
      {gap.label}
      {gap.shows > 0 && <span className="opacity-70">{gap.shows}</span>}
    </span>);

}