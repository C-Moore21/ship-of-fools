import React from 'react';

interface StealieProps {
  className?: string;
}

/** Steal-Your-Face mark: split red/blue circle with a white bolt. */
export function Stealie({ className = 'h-8 w-8' }: StealieProps) {
  return (
    <svg viewBox="0 0 64 64" className={className} role="img" aria-label="Ship of Fools">
      <defs>
        <clipPath id="stealie-clip">
          <circle cx="32" cy="32" r="30" />
        </clipPath>
      </defs>
      <g clipPath="url(#stealie-clip)">
        <rect x="0" y="0" width="64" height="64" fill="#1a4bbf" />
        <path d="M64 0 L64 64 L0 64 Z" fill="#e8332a" />
      </g>
      <circle cx="32" cy="32" r="30" fill="none" stroke="#f0f4ff" strokeWidth="3" />
      <path
        d="M36 2 L20 30 L30 30 L26 62 L46 30 L35 30 L44 2 Z"
        fill="#f0f4ff"
        stroke="#080c14"
        strokeWidth="1.5"
        clipPath="url(#stealie-clip)" />
      
    </svg>);

}