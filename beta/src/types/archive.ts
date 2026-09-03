export type Rarity = 'common' | 'solid' | 'rare' | 'special' | 'legendary';

export type GapLabel = 'Debut' | 'Bust' | 'Gap' | 'Drought' | 'Final';

export interface TrackGap {
  label: GapLabel;
  /** Shows since the song was last played. */
  shows: number;
}

export interface Track {
  id: string;
  position: number;
  title: string;
  /** Seconds. */
  duration: number;
  set: 'I' | 'II' | 'E';
  /** Segues directly into the next track. */
  segue: boolean;
  rarity: Rarity;
  gap?: TrackGap;
}

export interface Show {
  id: string;
  date: string;
  venue: string;
  city: string;
  era: string;
  tourRun: string;
  avgRating: number;
  ratingCount: number;
  listeners: number;
  soundboard: boolean;
  source: string;
  weather: string;
  tempF: number;
  note?: string;
  tracks: Track[];
}

export interface YearEntry {
  year: number;
  shows: number;
  era: string;
}