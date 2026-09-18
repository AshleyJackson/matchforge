import type { MatchId, MatchRecord, PersistenceStore } from "./types";
import { MatchRecordSchema } from "./schemas";

export class MemoryPersistenceStore implements PersistenceStore {
  private readonly matches = new Map<MatchId, MatchRecord>();

  async saveMatch(record: MatchRecord): Promise<void> {
    const validated = MatchRecordSchema.parse(record);
    const existing = this.matches.get(validated.matchId);
    if (existing && JSON.stringify(existing) !== JSON.stringify(validated)) {
      throw new Error(`match ${validated.matchId} already exists with different data`);
    }
    this.matches.set(validated.matchId, { ...validated, participantIds: [...validated.participantIds] });
  }

  async getMatch(matchId: MatchId): Promise<MatchRecord | null> {
    const match = this.matches.get(matchId);
    return match ? MatchRecordSchema.parse({ ...match, participantIds: [...match.participantIds] }) : null;
  }

  async completeMatch(matchId: MatchId): Promise<void> {
    const match = this.matches.get(matchId);
    if (match) this.matches.set(matchId, { ...match, status: "ended" });
  }
}
