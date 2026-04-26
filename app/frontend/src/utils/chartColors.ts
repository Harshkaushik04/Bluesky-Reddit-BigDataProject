export function seriesColor(index: number): string {
  // Golden-angle based hue stepping gives high visual separation for many series.
  const hue = (index * 137.508) % 360;
  const saturation = 82;
  const lightness = 62;
  return `hsl(${hue.toFixed(0)} ${saturation}% ${lightness}%)`;
}

