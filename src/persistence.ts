import { z } from "zod";
import { MatchRecordSchema } from "./schemas";

export class MemoryPersistenceStore {
  private readonly matches = new Map<string, z.infer<typeof MatchRecordSchema>>();

  async saveMatch(record: z.infer<typeof MatchRecordSchema>): Promise<void> {
    const validated = MatchRecordSchema.parse(record);
    const existing = this.matches.get(validated.matchId);
    if (existing && JSON.stringify(existing) !== JSON.stringify(validated)) {
      throw new Error(`match ${validated.matchId} already exists with different data`);
    }
    this.matches.set(validated.matchId, { ...validated, participantIds: [...validated.participantIds] });
  }

  async getMatch(matchId: string): Promise<z.infer<typeof MatchRecordSchema> | null> {
    const match = this.matches.get(matchId);
    return match ? MatchRecordSchema.parse({ ...match, participantIds: [...match.participantIds] }) : null;
  }

  async completeMatch(matchId: string): Promise<void> {
    const match = this.matches.get(matchId);
    if (match) this.matches.set(matchId, { ...match, status: "ended" });
  }
}
