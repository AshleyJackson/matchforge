import type { MatchId, MatchRecord, PersistenceStore } from "./types.ts";

export class MemoryPersistenceStore implements PersistenceStore {
  private readonly matches = new Map<MatchId, MatchRecord>();

  async saveMatch(record: MatchRecord): Promise<void> {
    const existing = this.matches.get(record.matchId);
    if (existing && JSON.stringify(existing) !== JSON.stringify(record)) {
      throw new Error(`match ${record.matchId} already exists with different data`);
    }
    this.matches.set(record.matchId, { ...record, participantIds: [...record.participantIds] });
  }

  async getMatch(matchId: MatchId): Promise<MatchRecord | null> {
    const match = this.matches.get(matchId);
    return match ? { ...match, participantIds: [...match.participantIds] } : null;
  }

  async completeMatch(matchId: MatchId): Promise<void> {
    const match = this.matches.get(matchId);
    if (match) this.matches.set(matchId, { ...match, status: "ended" });
  }
}
