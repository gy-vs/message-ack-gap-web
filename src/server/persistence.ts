import {renameSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import type {Snapshot} from './delivery';

/**
 * 快照文件持久化:先写临时文件再原子 rename,崩溃最多丢失最后一个未完成写。
 * DeliveryStore 的每一次状态推进都会触发 save;水位、离散确认集合因此
 * 不会因重启而回退。
 */
export class FilePersistence {
  private readonly tmp: string;
  constructor(private readonly file: string) {
    this.tmp = `${file}.tmp`;
  }
  load(): Snapshot | null {
    try {
      const raw = readFileSync(this.file, 'utf8');
      const snapshot = JSON.parse(raw) as Snapshot;
      if (snapshot.version !== 1) return null;
      return snapshot;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw error;
    }
  }
  save(snapshot: Snapshot): void {
    const target = resolve(this.file);
    writeFileSync(this.tmp, JSON.stringify(snapshot));
    renameSync(this.tmp, target);
  }
  get dir(): string {
    return dirname(this.file);
  }
}
