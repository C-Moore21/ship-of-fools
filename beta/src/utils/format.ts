export function formatDuration(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

export function formatClock(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor(seconds % 3600 / 60);
  if (h > 0) return `${h}h ${m.toString().padStart(2, '0')}m`;
  return `${m}m`;
}

const MONTHS = [
'January',
'February',
'March',
'April',
'May',
'June',
'July',
'August',
'September',
'October',
'November',
'December'];


/** "1977-05-08" -> { numeric: "05.08.77", long: "May 8, 1977" } */
export function formatDate(iso: string): {numeric: string;long: string;} {
  const [y, m, d] = iso.split('-');
  return {
    numeric: `${m}.${d}.${y.slice(2)}`,
    long: `${MONTHS[Number(m) - 1]} ${Number(d)}, ${y}`
  };
}