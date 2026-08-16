/** Latency accumulator with exact percentiles over all recorded samples. */
export class Samples {
  private readonly values: number[] = [];

  record(value: number): void {
    this.values.push(value);
  }

  get count(): number {
    return this.values.length;
  }

  private sorted(): number[] {
    return [...this.values].sort((a, b) => a - b);
  }

  percentiles(): Record<string, number> {
    if (this.values.length === 0) {
      return { p50: 0, p90: 0, p95: 0, p99: 0, max: 0, mean: 0 };
    }
    const sorted = this.sorted();
    const at = (fraction: number): number =>
      sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(fraction * sorted.length) - 1))] as number;

    const sum = this.values.reduce((total, value) => total + value, 0);

    return {
      p50: round(at(0.5)),
      p90: round(at(0.9)),
      p95: round(at(0.95)),
      p99: round(at(0.99)),
      max: round(sorted[sorted.length - 1] as number),
      mean: round(sum / this.values.length),
    };
  }
}

export function round(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

export function formatNumber(value: number): string {
  return value.toLocaleString('en-US');
}
