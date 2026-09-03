/**
 * Deprecated static year list. Live data now flows through
 * `hooks/useSofData::useYears`. Kept as a compile-time stub with the
 * total-shows constant preserved (used as a bootstrap value before the
 * year list resolves).
 */
import type { YearEntry } from '../types/archive'
export const years: YearEntry[] = []
export const totalShows = 2318
