/**
 * AcknowledgmentTracker
 *
 * Maintains the *contiguous confirmed prefix* (committed watermark) separately
 * from a sparse set of confirmations that arrived ahead of a gap. The watermark
 * only advances when every sequence number before the next candidate has been
 * confirmed, so an out-of-order ack (e.g. 12 before 11) parks the sequence in
 * `ahead` instead of jumping the high-water mark and hiding the gap.
 *
 * All operations are O(1) in the size of the gap: acking seq N with a missing
 * seq 1 below the watermark never scans N entries.
 */
export type GapRange = {from: bigint; to: bigint};

/**
 * Tells `advance` which sequence numbers carry real, still-unconfirmed
 * messages (deliverable or in-flight), so it can leap over never-published
 * numbers without ever leaping over a live message.
 */
export interface HoleSkip {
  exists(seq: bigint): boolean;
  nextExistingAtOrAfter(seq: bigint): bigint | null;
}

export class AcknowledgmentTracker {
  /** Last sequence of the fully confirmed prefix. Nothing <= watermark re-delivers. */
  watermark: bigint;
  /** Confirmed sequence numbers strictly greater than the watermark. */
  readonly ahead: Set<bigint>;

  constructor(watermark: bigint = 0n, ahead: Iterable<bigint> = []) {
    this.watermark = watermark;
    this.ahead = new Set(ahead);
  }

  /**
   * Record confirmation of `seq`.
   * @returns true if this call newly recorded the sequence; false if it was
   *          already part of the committed prefix or the pending-ahead set.
   */
  confirm(seq: bigint): boolean {
    if (seq <= this.watermark) return false;
    if (this.ahead.has(seq)) return false;
    this.ahead.add(seq);
    return true;
  }

  /**
   * Collapse the prefix as far as possible: starting at watermark + 1, consume
   * consecutive entries from `ahead`, deleting each as the watermark overtakes
   * it. Without `holes`, a missing candidate stops the collapse (generic
   * semantics). Pass a `HoleSkip` to distinguish *never published* sequence
   * numbers (skipped over in O(1)) from published-but-unconfirmed messages
   * (which block the watermark). Each parked sequence is visited at most once
   * over the tracker's lifetime, so a single giant ack stays O(1) in the size
   * of the numeric gap. Never-published numbers can never be born later because
   * the broker allocates strictly increasing sequence numbers past the gap.
   */
  advance(holes?: HoleSkip): bigint {
    for (;;) {
      const next = this.watermark + 1n;
      if (this.ahead.has(next)) {
        this.ahead.delete(next);
        this.watermark = next;
        continue;
      }
      if (!holes) break;
      if (holes.exists(next)) break; // a real, unconfirmed message blocks the prefix
      const nextParked = this.smallestParked();
      if (nextParked === null) break;
      const nextExisting = holes.nextExistingAtOrAfter(next);
      if (nextExisting !== null && nextExisting < nextParked) break; // unacked message before the parked ack
      // Leap over the never-published hole, landing immediately before the
      // parked message; the next iteration consumes it normally.
      this.watermark = nextParked - 1n;
    }
    return this.watermark;
  }

  private smallestParked(): bigint | null {
    let min: bigint | null = null;
    for (const seq of this.ahead) if (min === null || seq < min) min = seq;
    return min;
  }

  /** Confirm then collapse; returns the resulting watermark. */
  confirmAndAdvance(seq: bigint): {watermark: bigint; advanced: boolean} {
    const wasNew = this.confirm(seq);
    const before = this.watermark;
    this.advance();
    return {watermark: this.watermark, advanced: wasNew && this.watermark > before};
  }

  /** Undo a confirmation parked ahead of the watermark (nack / timeout / cancel). */
  retract(seq: bigint): boolean {
    if (seq <= this.watermark) return false;
    return this.ahead.delete(seq);
  }

  isConfirmed(seq: bigint): boolean {
    return seq <= this.watermark || this.ahead.has(seq);
  }

  /** True while no confirmation beyond the watermark is parked. */
  get isContiguous(): boolean {
    return this.ahead.size === 0;
  }

  /**
   * Holes inside the confirmed range: missing sequence numbers between the
   * watermark and the largest parked confirmation. O(k log k) in the number of
   * parked sequences, independent of the numeric size of the gap.
   */
  holes(limit = 64): GapRange[] {
    if (this.ahead.size === 0) return [];
    const seqs = [...this.ahead].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const ranges: GapRange[] = [];
    let expect = this.watermark + 1n;
    for (const seq of seqs) {
      if (seq > expect) {
        ranges.push({from: expect, to: seq - 1n});
        if (ranges.length >= limit) return ranges;
      }
      expect = seq + 1n;
    }
    return ranges;
  }

  toJSON(): {watermark: string; ahead: string[]} {
    return {
      watermark: this.watermark.toString(),
      ahead: [...this.ahead].map(seq => seq.toString()),
    };
  }

  static fromJSON(data: unknown): AcknowledgmentTracker {
    const value = data as {watermark?: string; ahead?: string[]} | null | undefined;
    const watermark = value?.watermark ? BigInt(value.watermark) : 0n;
    const ahead = (value?.ahead ?? []).map(seq => BigInt(seq));
    return new AcknowledgmentTracker(watermark, ahead);
  }
}
