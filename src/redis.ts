import type {
  AtomicClaimInput,
  ClaimResult,
  CoordinationStore,
  MatchClaim,
  Participant,
  ParticipantId,
  PoolKey,
  WaitingEntry,
} from "./types";
import { cloneParticipant } from "./utils";

/**
 * Minimal Redis contract implemented by both Upstash Redis and native clients.
 *
 * The adapter deliberately depends on a structural contract instead of a
 * Redis package, keeping `matchforge` usable in Bun, Node, and edge runtimes.
 */
export interface RedisClient {
  get<T>(key: string): Promise<T | null>;
  set(
    key: string,
    value: unknown,
    options?: { ex?: number; nx?: boolean },
  ): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  zadd(key: string, value: { score: number; member: string }): Promise<unknown>;
  zrem(key: string, ...members: string[]): Promise<unknown>;
  zscore(key: string, member: string): Promise<number | null>;
  zrange<T extends string[]>(
    key: string,
    start: number,
    stop: number,
  ): Promise<T>;
  sadd(key: string, ...members: string[]): Promise<unknown>;
  srem(key: string, ...members: string[]): Promise<unknown>;
  smembers<T extends string[]>(key: string): Promise<T>;
  eval<T = unknown>(script: string, keys: string[], args: string[]): Promise<T>;
}

export interface RedisCoordinationStoreOptions {
  redis: RedisClient;
  namespace?: string;
  participantTtlSeconds?: number;
  claimTtlSeconds?: number;
  claimRetentionSeconds?: number;
}

/**
 * Redis coordination adapter for coarse, pre-partitioned pools.
 *
 * Redis Lua cannot execute arbitrary TypeScript compatibility or scoring
 * functions. Callers using this adapter must encode hard constraints in the
 * pool key and use FIFO selection. Kismet-specific filters can be represented
 * by partitioning pools or by adding a purpose-built adapter rule in a later
 * version.
 */
export class RedisCoordinationStore implements CoordinationStore {
  private readonly redis: RedisClient;
  private readonly namespace: string;
  private readonly participantTtlSeconds: number;
  private readonly claimTtlSeconds: number;
  private readonly claimRetentionSeconds: number;

  constructor(options: RedisCoordinationStoreOptions) {
    this.redis = options.redis;
    this.namespace = options.namespace ?? "matchforge:v1";
    this.participantTtlSeconds = options.participantTtlSeconds ?? 90;
    this.claimTtlSeconds = options.claimTtlSeconds ?? 30;
    this.claimRetentionSeconds = options.claimRetentionSeconds ?? 300;
  }

  async atomicClaim(input: AtomicClaimInput): Promise<ClaimResult> {
    if (
      input.compatibility ||
      input.score ||
      input.relax ||
      input.strategy.type !== "fifo"
    ) {
      return {
        status: "error",
        error: new Error(
          "RedisCoordinationStore supports FIFO claims over pre-partitioned pools only",
        ),
      };
    }

    const raw = await this.redis.eval<string[]>(
      CLAIM_SCRIPT,
      [this.participantKey(input.requesterId), this.waitingKey(input.poolKey)],
      [
        input.requesterId,
        String(input.now),
        String(Math.min(input.scanLimit, 100)),
        JSON.stringify(input.excludeIds),
        this.participantPrefix(),
        this.claimPrefix(),
        input.claimId,
        input.matchId,
        String(
          Math.max(
            1,
            Math.ceil(
              Math.min(input.claimTtlMs, this.claimTtlSeconds * 1000) / 1000,
            ),
          ),
        ),
        input.poolKey,
        String(this.participantTtlSeconds),
        String(this.claimRetentionSeconds),
      ],
    );

    if (!raw || raw[0] !== "matched") {
      if (raw?.[0] === "stale")
        return { status: "stale", reason: raw[1] ?? "requester is not queued" };
      return { status: "none" };
    }
    return { status: "matched", claim: JSON.parse(raw[1]!) as MatchClaim };
  }

  async putParticipant(participant: Participant): Promise<void> {
    await this.redis.set(this.participantKey(participant.id), participant, {
      ex: this.participantTtlSeconds,
    });
  }

  getParticipant(id: ParticipantId): Promise<Participant | null> {
    return this.redis.get<Participant>(this.participantKey(id));
  }

  async updateParticipant(
    id: ParticipantId,
    update: (participant: Participant) => Participant,
  ): Promise<Participant | null> {
    const current = await this.getParticipant(id);
    if (!current) return null;
    const next = update(cloneParticipant(current)!);
    await this.putParticipant(next);
    return cloneParticipant(next);
  }

  addToWaitingIndex(entry: WaitingEntry): Promise<void> {
    return this.redis
      .zadd(this.waitingKey(entry.poolKey), {
        score: entry.joinedAt,
        member: entry.participantId,
      })
      .then(() => undefined);
  }

  removeFromWaitingIndex(
    poolKey: PoolKey,
    participantId: ParticipantId,
  ): Promise<void> {
    return this.redis
      .zrem(this.waitingKey(poolKey), participantId)
      .then(() => undefined);
  }

  getClaim(claimId: string): Promise<MatchClaim | null> {
    return this.redis.get<MatchClaim>(this.claimKey(claimId));
  }

  async putClaim(claim: MatchClaim): Promise<void> {
    const seconds = Math.max(
      1,
      Math.ceil((claim.expiresAt - Date.now()) / 1000),
    );
    await this.redis.set(this.claimKey(claim.claimId), claim, {
      ex: seconds + this.claimRetentionSeconds,
    });
    await this.redis.sadd(this.claimIndexKey(), claim.claimId);
  }

  async getQueueDepth(poolKey: PoolKey): Promise<number> {
    const members = await this.redis.zrange<string[]>(
      this.waitingKey(poolKey),
      0,
      -1,
    );
    return members.length;
  }

  async recoverExpiredClaims(now: number): Promise<MatchClaim[]> {
    const claimIds = await this.redis.smembers<string[]>(this.claimIndexKey());
    const recovered: MatchClaim[] = [];
    for (const claimId of claimIds) {
      const claim = await this.getClaim(claimId);
      if (!claim) {
        await this.redis.srem(this.claimIndexKey(), claimId);
        continue;
      }
      if (claim.state !== "reserved") {
        await this.redis.srem(this.claimIndexKey(), claimId);
        continue;
      }
      if (claim.expiresAt > now) continue;
      const result = await this.redis.eval<string[]>(
        RECOVER_SCRIPT,
        [
          this.claimKey(claimId),
          this.waitingKey(claim.poolKey),
          this.participantKey(claim.participants[0]!.id),
          this.participantKey(claim.participants[1]!.id),
        ],
        [String(this.participantTtlSeconds), String(now)],
      );
      if (result?.[0] === "expired") {
        recovered.push(JSON.parse(result[1]!) as MatchClaim);
        await this.redis.srem(this.claimIndexKey(), claimId);
      }
    }
    return recovered;
  }

  private participantPrefix() {
    return `${this.namespace}:participant:`;
  }

  private claimPrefix() {
    return `${this.namespace}:claim:`;
  }

  private claimIndexKey() {
    return `${this.namespace}:claims`;
  }

  private participantKey(id: string) {
    return `${this.participantPrefix()}${id}`;
  }

  private claimKey(id: string) {
    return `${this.claimPrefix()}${id}`;
  }

  private waitingKey(pool: string) {
    return `${this.namespace}:waiting:${pool}`;
  }
}

const CLAIM_SCRIPT = `
local requester = redis.call('get', KEYS[1])
if not requester then return {'stale', 'requester is missing'} end
local me = cjson.decode(requester)
if me.state ~= 'waiting' then return {'stale', 'requester is not waiting'} end
if not redis.call('zscore', KEYS[2], ARGV[1]) then return {'stale', 'requester is not queued'} end

local excluded = {}
for _, id in ipairs(cjson.decode(ARGV[4])) do excluded[id] = true end
local members = redis.call('zrange', KEYS[2], 0, tonumber(ARGV[3]) - 1)
for _, id in ipairs(members) do
  if id ~= ARGV[1] and not excluded[id] then
    local raw = redis.call('get', ARGV[5] .. id)
    if not raw then
      redis.call('zrem', KEYS[2], id)
    else
      local candidate = cjson.decode(raw)
      if candidate.state == 'waiting' then
        local claim = {
          claimId = ARGV[7],
          matchId = ARGV[8],
          poolKey = ARGV[10],
          participants = {
            {id = ARGV[1], version = me.version, snapshot = me.preferences},
            {id = id, version = candidate.version, snapshot = candidate.preferences}
          },
          state = 'reserved',
          createdAt = tonumber(ARGV[2]),
          expiresAt = tonumber(ARGV[2]) + tonumber(ARGV[9]) * 1000,
          strategy = 'fifo',
          recoveryToken = ARGV[7]
        }
        redis.call('zrem', KEYS[2], ARGV[1], id)
        me.state = 'claimed'
        me.version = me.version + 1
        candidate.state = 'claimed'
        candidate.version = candidate.version + 1
        redis.call('set', KEYS[1], cjson.encode(me), 'EX', ARGV[11])
        redis.call('set', ARGV[5] .. id, cjson.encode(candidate), 'EX', ARGV[11])
        redis.call('set', ARGV[6] .. ARGV[7], cjson.encode(claim), 'EX', ARGV[9] + ARGV[12])
        return {'matched', cjson.encode(claim)}
      end
    end
  end
end
return {'none'}
`;

const RECOVER_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local claim = cjson.decode(raw)
if claim.state ~= 'reserved' or claim.expiresAt > tonumber(ARGV[2]) then return {'ignored'} end
for index, key in ipairs({KEYS[3], KEYS[4]}) do
  local participantRaw = redis.call('get', key)
  local snapshot = claim.participants[index]
  if participantRaw then
    local participant = cjson.decode(participantRaw)
    if participant.state == 'claimed' and participant.version == snapshot.version + 1 then
      participant.state = 'waiting'
      participant.version = participant.version + 1
      redis.call('set', key, cjson.encode(participant), 'EX', ARGV[1])
      redis.call('zadd', KEYS[2], participant.joinedAt, participant.id)
    end
  end
end
claim.state = 'expired'
redis.call('set', KEYS[1], cjson.encode(claim), 'EX', ARGV[1])
return {'expired', cjson.encode(claim)}
`;
