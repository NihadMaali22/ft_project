import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { round } from './stats.ts';

const execFileAsync = promisify(execFile);

/**
 * Samples container CPU and memory during a run.
 *
 * The resource limits are the whole point of the exercise, so a throughput
 * number without the CPU figure beside it says nothing about headroom.
 * `docker stats` reports CPU as a percentage of one core, so the app container
 * saturates its 0.5 CPU quota at 50%.
 */

export interface ContainerSample {
  cpuPercent: number;
  memoryMiB: number;
  memoryLimitMiB: number;
}

export interface ContainerSummary {
  samples: number;
  cpuPercentMean: number;
  cpuPercentMax: number;
  memoryMiBMean: number;
  memoryMiBMax: number;
  memoryLimitMiB: number;
}

function parseMemory(value: string): number {
  const match = /^([\d.]+)\s*([A-Za-z]+)$/.exec(value.trim());
  if (match === null) return 0;
  const amount = Number(match[1]);
  switch ((match[2] as string).toLowerCase()) {
    case 'b':
      return amount / 1048576;
    case 'kib':
    case 'kb':
      return amount / 1024;
    case 'mib':
    case 'mb':
      return amount;
    case 'gib':
    case 'gb':
      return amount * 1024;
    default:
      return 0;
  }
}

interface DockerStatsLine {
  Name: string;
  CPUPerc: string;
  MemUsage: string;
}

export class ResourceMonitor {
  private readonly samples = new Map<string, ContainerSample[]>();
  private timer: NodeJS.Timeout | null = null;
  private polling = false;

  constructor(
    private readonly containers: string[],
    private readonly intervalMs = 2000,
  ) {
    for (const container of containers) this.samples.set(container, []);
  }

  start(): void {
    this.timer = setInterval(() => void this.poll(), this.intervalMs);
  }

  private async poll(): Promise<void> {
    // `docker stats --no-stream` takes about a second; skip overlapping polls
    // rather than queueing them up.
    if (this.polling) return;
    this.polling = true;

    try {
      const { stdout } = await execFileAsync('docker', [
        'stats',
        '--no-stream',
        '--format',
        '{{json .}}',
        ...this.containers,
      ]);

      for (const line of stdout.trim().split('\n')) {
        if (line.trim() === '') continue;
        const parsed = JSON.parse(line) as DockerStatsLine;
        const bucket = this.samples.get(parsed.Name);
        if (bucket === undefined) continue;

        const [used, limit] = parsed.MemUsage.split('/');
        bucket.push({
          cpuPercent: Number(parsed.CPUPerc.replace('%', '')) || 0,
          memoryMiB: parseMemory(used ?? '0B'),
          memoryLimitMiB: parseMemory(limit ?? '0B'),
        });
      }
    } catch {
      // A failed sample must never abort the load test.
    } finally {
      this.polling = false;
    }
  }

  stop(): Record<string, ContainerSummary> {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }

    const summary: Record<string, ContainerSummary> = {};

    for (const [name, samples] of this.samples) {
      if (samples.length === 0) {
        summary[name] = {
          samples: 0,
          cpuPercentMean: 0,
          cpuPercentMax: 0,
          memoryMiBMean: 0,
          memoryMiBMax: 0,
          memoryLimitMiB: 0,
        };
        continue;
      }

      const cpu = samples.map((sample) => sample.cpuPercent);
      const memory = samples.map((sample) => sample.memoryMiB);

      summary[name] = {
        samples: samples.length,
        cpuPercentMean: round(cpu.reduce((a, b) => a + b, 0) / cpu.length),
        cpuPercentMax: round(Math.max(...cpu)),
        memoryMiBMean: round(memory.reduce((a, b) => a + b, 0) / memory.length),
        memoryMiBMax: round(Math.max(...memory)),
        memoryLimitMiB: round(samples[0]?.memoryLimitMiB ?? 0),
      };
    }

    return summary;
  }

  /** Clears samples so the next phase measures independently. */
  reset(): void {
    for (const container of this.containers) this.samples.set(container, []);
  }
}
