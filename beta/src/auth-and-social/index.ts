// Public surface for the auth-and-social module.
// Integration step wires these into AppHeader / ShowDetail / Browse.

export * from './api';
export { useAuth } from './useAuth';
export type { UseAuth } from './useAuth';
export { useShowRating } from './useShowRating';
export type { UseShowRating } from './useShowRating';
export { useShowNote } from './useShowNote';
export type { UseShowNote, NoteStatus } from './useShowNote';
export { LoginModal } from './LoginModal';
export { RatingStars } from './RatingStars';
export { NoteEditor } from './NoteEditor';
