import React from 'react';
import { Browse } from './pages/Browse';

interface AppProps {
  /** Row rhythm for the show list and setlist. */
  density?: 'comfortable' | 'compact';
  /** Level meter shown in the player bar. */
  visualizer?: 'bars' | 'radial' | 'off';
}

export function App({ density = 'comfortable', visualizer = 'bars' }: AppProps) {
  return <Browse compact={density === 'compact'} visualizer={visualizer} />;
}