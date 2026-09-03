import React, { useState } from 'react';
import { Star } from 'lucide-react';

interface Props {
  value: number;                 // current rating, 0–5 in 0.5 steps
  onChange: (next: number) => void;
  disabled?: boolean;            // e.g. logged out — clicks bubble to onRequestLogin
  onRequestLogin?: () => void;
  size?: number;                 // pixel size of each star
  count?: number;                // default 5
  className?: string;
}

/**
 * Five-star widget with half-star granularity (hover left half -> .5,
 * right half -> full). Matches classic UI: clicking the same value clears it
 * (parent's setStars handles the toggle).
 */
function RatingStarsImpl({
  value,
  onChange,
  disabled = false,
  onRequestLogin,
  size = 20,
  count = 5,
  className = '',
}: Props) {
  const [hover, setHover] = useState<number | null>(null);
  const shown = hover ?? value;

  const handleClick = (v: number) => {
    if (disabled) {
      onRequestLogin?.();
      return;
    }
    onChange(v);
  };

  return (
    <div
      className={`inline-flex select-none ${className}`}
      onMouseLeave={() => setHover(null)}
      role="radiogroup"
      aria-label="Star rating"
    >
      {Array.from({ length: count }, (_, i) => {
        const idx = i + 1; // 1..count
        const fillLevel = Math.max(0, Math.min(1, shown - (idx - 1))); // 0, .5, 1
        return (
          <div
            key={idx}
            className="relative"
            style={{ width: size, height: size, cursor: disabled && !onRequestLogin ? 'default' : 'pointer' }}
          >
            {/* Empty background */}
            <Star
              size={size}
              className="absolute inset-0 text-border"
              strokeWidth={1.5}
            />
            {/* Filled overlay clipped to fillLevel */}
            {fillLevel > 0 && (
              <div
                className="absolute inset-0 overflow-hidden"
                style={{ width: `${fillLevel * 100}%` }}
              >
                <Star
                  size={size}
                  className="text-accent"
                  fill="currentColor"
                  strokeWidth={1.5}
                />
              </div>
            )}
            {/* Hover hit halves */}
            <button
              type="button"
              aria-label={`${idx - 0.5} stars`}
              className="absolute left-0 top-0 h-full w-1/2"
              onMouseEnter={() => setHover(idx - 0.5)}
              onClick={() => handleClick(idx - 0.5)}
              disabled={disabled && !onRequestLogin}
              style={{ background: 'transparent', border: 0, padding: 0 }}
            />
            <button
              type="button"
              aria-label={`${idx} stars`}
              className="absolute right-0 top-0 h-full w-1/2"
              onMouseEnter={() => setHover(idx)}
              onClick={() => handleClick(idx)}
              disabled={disabled && !onRequestLogin}
              style={{ background: 'transparent', border: 0, padding: 0 }}
            />
          </div>
        );
      })}
    </div>
  );
}

// Memoized: RatingStars is rendered per-row in the setlist (25+ instances) —
// re-rendering all of them whenever the ratings map identity changes is pure
// waste. Parents must pass a stable onChange (see Setlist TrackRow) for the
// memo to actually skip work.
export const RatingStars = React.memo(RatingStarsImpl);
export default RatingStars;
