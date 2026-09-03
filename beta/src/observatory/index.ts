// Public barrel for the Observatory module.
// Consumer wiring: import { ObservatoryModal } from '@/observatory' and pass
// { open, onClose, onOpenShow }.

export { default as ObservatoryModal } from './ObservatoryModal';
export { default as Heatmap } from './Heatmap';
export { default as Scatter } from './Scatter';
export { default as CrowsNest } from './CrowsNest';

export {
  useHeatmap,
  useScatter,
  useShowsMap,
  useUsStates,
} from './useObservatory';

export {
  getHeatmap,
  getScatter,
  getShowsMap,
  getUsStates,
} from './api';

export type {
  HeatmapResponse,
  HeatmapSong,
  YearBucket,
  ScatterResponse,
  Performance,
  ShowsMapResponse,
  MapShow,
  GeoJSONFeatureCollection,
} from './api';
