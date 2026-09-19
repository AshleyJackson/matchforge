import { z } from "zod";
import { SerializableFilterSchema } from "./schemas";
import {
  AtomicClaimInputSchema,
  ClaimResultSchema,
  MatchClaimSchema,
  ParticipantSchema,
  WaitingEntrySchema,
} from "./schemas";
import { cloneParticipant, stableSerialize } from "./utils";

/**
 * Minimal Redis contract implemented by both Upstash Redis and native clients.
 *
 * The adapter deliberately depends on a structural contract instead of a
 * Redis package, keeping `matchforge` usable in Bun, Node, and edge runtimes.
 */
export const RedisClientSchema = z.object({
  get: z.function({ input: [z.string()], output: z.promise(z.unknown().nullable()) }),
  set: z.function({ input: [z.string(), z.unknown(), z.unknown().optional()], output: z.promise(z.unknown()) }),
  del: z.function({ input: [z.string()], output: z.promise(z.unknown()) }),
  zadd: z.function({ input: [z.string(), z.unknown()], output: z.promise(z.unknown()) }),
  zrem: z.function({ input: [z.string(), z.string()], output: z.promise(z.unknown()) }),
  zscore: z.function({ input: [z.string(), z.string()], output: z.promise(z.number().nullable()) }),
  zrange: z.function({ input: [z.string(), z.number(), z.number()], output: z.promise(z.array(z.string())) }),
  sadd: z.function({ input: [z.string(), z.string()], output: z.promise(z.unknown()) }),
  srem: z.function({ input: [z.string(), z.string()], output: z.promise(z.unknown()) }),
  smembers: z.function({ input: [z.string()], output: z.promise(z.array(z.string())) }),
  sscan: z.function({ input: [z.string(), z.union([z.string(), z.number()]), z.unknown().optional()], output: z.promise(z.unknown()) }).optional(),
  eval: z.function({ input: [z.string(), z.array(z.string()), z.array(z.string())], output: z.promise(z.unknown()) }),
}).passthrough();

export const RedisCoordinationStoreOptionsSchema = z.object({
  redis: RedisClientSchema,
  namespace: z.string().optional(),
  participantTtlSeconds: z.number().int().positive().optional(),
  claimTtlSeconds: z.number().int().positive().optional(),
  claimRetentionSeconds: z.number().int().positive().optional(),
}).strict();

/**
 * Redis coordination adapter for coarse, pre-partitioned pools.
 *
 * Redis Lua cannot execute arbitrary TypeScript compatibility or scoring
 * functions. Callers using this adapter must encode hard constraints in the
 * pool key and use FIFO selection. Kismet-specific filters can be represented
 * by partitioning pools or by adding a purpose-built adapter rule in a later
 * version.
 */
export class RedisCoordinationStore {
  private readonly redis: z.infer<typeof RedisClientSchema>;
  private readonly namespace: string;
  private readonly participantTtlSeconds: number;
  private readonly claimTtlSeconds: number;
  private readonly claimRetentionSeconds: number;

  constructor(options: z.infer<typeof RedisCoordinationStoreOptionsSchema>) {
    this.redis = options.redis;
    this.namespace = options.namespace ?? "matchforge:v1";
    this.participantTtlSeconds = options.participantTtlSeconds ?? 90;
    this.claimTtlSeconds = options.claimTtlSeconds ?? 30;
    this.claimRetentionSeconds = options.claimRetentionSeconds ?? 300;
  }

  async atomicClaim(input: z.infer<typeof AtomicClaimInputSchema>): Promise<z.infer<typeof ClaimResultSchema>> {
    if (
      input.compatibility ||
      input.score ||
      input.relax ||
      input.strategy.type !== "fifo"
    ) {
      return {
        status: "error",
        error: new Error(
          "RedisCoordinationStore supports FIFO claims with serializable filters only",
        ),
      };
    }
    if (!input.filter) {
      return {
        status: "error",
        error: new Error(
          "RedisCoordinationStore requires a serializable filter",
        ),
      };
    }
    const filter = SerializableFilterSchema.parse(input.filter);
    const requestFingerprint =
      input.requestFingerprint ??
      stableSerialize({
        requesterId: input.requesterId,
        poolKey: input.poolKey,
        scanLimit: input.scanLimit,
        excludeIds: input.excludeIds,
        strategy: input.strategy,
        filter,
      });

    const raw = z.array(z.unknown()).parse(await this.redis.eval(
      CLAIM_SCRIPT,
      [
        this.participantKey(input.requesterId),
        this.waitingKey(input.poolKey),
        this.idempotencyKey(input.idempotencyKey ?? input.claimId),
        this.claimIndexKey(),
      ],
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
        JSON.stringify(filter),
        input.idempotencyKey ?? "",
        requestFingerprint,
      ],
    ));

    if (raw?.[0] === "error") {
      return {
        status: "error",
        error: new Error(typeof raw[1] === "string" ? raw[1] : "Redis claim error"),
      };
    }
    if (!raw || (raw[0] !== "matched" && raw[0] !== "idempotent")) {
      if (raw?.[0] === "stale")
        return {
          status: "stale",
          reason:
            typeof raw[1] === "string" ? raw[1] : "requester is not queued",
        };
      return { status: "none" };
    }
    return {
      status: "matched",
      claim: parseClaimResult(raw[1]),
    };
  }

  async putParticipant(participant: z.infer<typeof ParticipantSchema>): Promise<void> {
    const validated = ParticipantSchema.parse(participant);
    await this.redis.set(this.participantKey(validated.id), validated, {
      ex: this.participantTtlSeconds,
    });
  }

  getParticipant(id: string): Promise<z.infer<typeof ParticipantSchema> | null> {
    return this.redis
      .get(this.participantKey(id))
      .then((value) =>
        value === null ? null : ParticipantSchema.parse(value),
      );
  }

  async updateParticipant(
    id: string,
    update: (participant: z.infer<typeof ParticipantSchema>) => z.infer<typeof ParticipantSchema>,
  ): Promise<z.infer<typeof ParticipantSchema> | null> {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const current = await this.getParticipant(id);
      if (!current) return null;
      const next = ParticipantSchema.parse(update(cloneParticipant(current)!));
      const result = z.array(z.string()).parse(await this.redis.eval(
        UPDATE_PARTICIPANT_SCRIPT,
        [this.participantKey(id)],
        [
          String(current.version),
          JSON.stringify(next),
          String(this.participantTtlSeconds),
        ],
      ));
      if (result?.[0] === "updated") return next;
    }
    throw new Error(`participant ${id} update conflicted`);
  }

  addToWaitingIndex(entry: z.infer<typeof WaitingEntrySchema>): Promise<void> {
    const validated = WaitingEntrySchema.parse(entry);
    return this.redis
      .zadd(this.waitingKey(validated.poolKey), {
        score: validated.joinedAt,
        member: validated.participantId,
      })
      .then(() => undefined);
  }

  removeFromWaitingIndex(
    poolKey: string,
    participantId: string,
  ): Promise<void> {
    return this.redis
      .zrem(this.waitingKey(poolKey), participantId)
      .then(() => undefined);
  }

  getClaim(claimId: string): Promise<z.infer<typeof MatchClaimSchema> | null> {
    return this.redis
      .get(this.claimKey(claimId))
      .then((value) => (value === null ? null : MatchClaimSchema.parse(value)));
  }

  async putClaim(claim: z.infer<typeof MatchClaimSchema>): Promise<void> {
    const validated = MatchClaimSchema.parse(claim);
    const seconds = Math.max(
      1,
      Math.ceil((validated.expiresAt - Date.now()) / 1000),
    );
    await this.redis.set(this.claimKey(validated.claimId), validated, {
      ex: seconds + this.claimRetentionSeconds,
    });
    await this.redis.sadd(this.claimIndexKey(), validated.claimId);
  }

  async commitClaim(claimId: string): Promise<boolean> {
    const claim = await this.getClaim(claimId);
    if (!claim) return false;
    const result = z.array(z.string()).parse(await this.redis.eval(
      COMMIT_SCRIPT,
      [
        this.claimKey(claimId),
        ...claim.participants.map((participant) =>
          this.participantKey(participant.id),
        ),
        this.idempotencyKey(claim.recoveryToken),
      ],
      [String(this.participantTtlSeconds)],
    ));
    return result?.[0] === "committed";
  }

  async rollbackClaim(claimId: string): Promise<boolean> {
    const claim = await this.getClaim(claimId);
    if (!claim) return false;
    const result = z.array(z.string()).parse(await this.redis.eval(
      ROLLBACK_SCRIPT,
      [
        this.claimKey(claimId),
        ...claim.participants.map((participant) =>
          this.participantKey(participant.id),
        ),
        this.waitingKey(claim.poolKey),
        this.idempotencyKey(claim.recoveryToken),
      ],
      [String(this.participantTtlSeconds)],
    ));
    return result?.[0] === "rolled_back";
  }

  async clearIdempotency(key: string): Promise<void> {
    await this.redis.del(this.idempotencyKey(key));
  }

  async getQueueDepth(poolKey: string): Promise<number> {
    const members = z.array(z.string()).parse(await this.redis.zrange(
      this.waitingKey(poolKey),
      0,
      -1,
    ));
    return members.length;
  }

  async recoverExpiredClaims(
    now: number,
    hasDurableMatch?: (claim: z.infer<typeof MatchClaimSchema>) => Promise<boolean>,
  ): Promise<z.infer<typeof MatchClaimSchema>[]> {
    const claimIds = await this.scanClaimIds();
    const recovered: z.infer<typeof MatchClaimSchema>[] = [];
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
      if (hasDurableMatch && (await hasDurableMatch(claim))) {
        if (await this.commitClaim(claimId)) {
          recovered.push({ ...claim, state: "committed" });
          await this.redis.srem(this.claimIndexKey(), claimId);
        }
        continue;
      }
      const result = z.array(z.unknown()).parse(await this.redis.eval(
        RECOVER_SCRIPT,
        [
          this.claimKey(claimId),
          this.waitingKey(claim.poolKey),
          this.participantKey(claim.participants[0]!.id),
          this.participantKey(claim.participants[1]!.id),
        ],
        [String(this.participantTtlSeconds), String(now)],
      ));
      if (result?.[0] === "expired") {
        recovered.push(parseClaimResult(result[1]));
        await this.redis.srem(this.claimIndexKey(), claimId);
      }
    }
    return recovered;
  }

  private participantPrefix() {
    return `${this.namespace}:{${this.namespace}}:participant:`;
  }

  private claimPrefix() {
    return `${this.namespace}:{${this.namespace}}:claim:`;
  }

  private claimIndexKey() {
    return `${this.namespace}:{${this.namespace}}:claims`;
  }

  private participantKey(id: string) {
    return `${this.participantPrefix()}${id}`;
  }

  private claimKey(id: string) {
    return `${this.claimPrefix()}${id}`;
  }

  private idempotencyKey(id: string) {
    return `${this.namespace}:{${this.namespace}}:idempotency:${id}`;
  }

  private waitingKey(pool: string) {
    return `${this.namespace}:{${this.namespace}}:waiting:${pool}`;
  }

  private async scanClaimIds(): Promise<string[]> {
    if (!this.redis.sscan) {
      return z.array(z.string()).parse(await this.redis.smembers(this.claimIndexKey()));
    }
    const ids: string[] = [];
    let cursor = "0";
    do {
      const [nextCursor, members] = z.tuple([z.string(), z.array(z.string())]).parse(
        await this.redis.sscan(
        this.claimIndexKey(),
        cursor,
        { count: 100 },
        ),
      );
      ids.push(...members);
      cursor = String(nextCursor);
    } while (cursor !== "0");
    return ids;
  }
}

function parseClaimResult(value: unknown): z.infer<typeof MatchClaimSchema> {
  if (typeof value === "string")
    return MatchClaimSchema.parse(JSON.parse(value));
  return MatchClaimSchema.parse(value);
}

const UPDATE_PARTICIPANT_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local participant = cjson.decode(raw)
if participant.version ~= tonumber(ARGV[1]) then return {'conflict'} end
redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3])
return {'updated'}
`;

const CLAIM_SCRIPT = `
if ARGV[14] ~= '' then
  local existing = redis.call('get', KEYS[3])
  if existing then
    local previous = cjson.decode(existing)
    if previous.requestFingerprint ~= ARGV[15] then
      return {'error', 'idempotency key reused with different request'}
    end
    return {'idempotent', existing}
  end
end
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
      local function read_path(root, path)
        local value = root
        for _, segment in ipairs(path or {}) do
          if type(value) ~= 'table' then return nil end
          value = value[segment]
        end
        return value
      end
      local function operand_value(operand)
        if operand.literal ~= nil then return operand.literal end
        if operand.source == 'requester' then return read_path(me.preferences, operand.path) end
        return read_path(candidate.preferences, operand.path)
      end
      local function filter_matches(filter)
        if filter.op == 'all' then
          for _, nested in ipairs(filter.filters) do
            if not filter_matches(nested) then return false end
          end
          return true
        elseif filter.op == 'any' then
          for _, nested in ipairs(filter.filters) do
            if filter_matches(nested) then return true end
          end
          return false
        elseif filter.op == 'not' then
          return not filter_matches(filter.filter)
        elseif filter.op == 'overlaps' then
          local left = operand_value(filter.left)
          local right = operand_value(filter.right)
          if type(left) ~= 'table' or type(right) ~= 'table' then return false end
          for _, left_value in ipairs(left) do
            for _, right_value in ipairs(right) do
              if cjson.encode(left_value) == cjson.encode(right_value) then return true end
            end
          end
          return false
        elseif filter.op == 'equals' then
          return cjson.encode(operand_value(filter.left)) == cjson.encode(operand_value(filter.right))
        elseif filter.op == 'in' then
          local value = operand_value(filter.left)
          local values = operand_value(filter.right)
          if type(values) ~= 'table' then return false end
          for _, item in ipairs(values) do
            if cjson.encode(value) == cjson.encode(item) then return true end
          end
          return false
        elseif filter.op == 'contains' then
          local container = operand_value(filter.left)
          local value = operand_value(filter.right)
          if type(container) == 'table' then
            for _, item in ipairs(container) do
              if cjson.encode(value) == cjson.encode(item) then return true end
            end
            return false
          end
          return type(container) == 'string' and type(value) == 'string' and string.find(container, value, 1, true) ~= nil
        end
        return false
      end
      local filter = cjson.decode(ARGV[13])
      if candidate.state == 'waiting' and filter_matches(filter) then
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
          recoveryToken = ARGV[14] ~= '' and ARGV[14] or ARGV[7],
          requestFingerprint = ARGV[15]
        }
        redis.call('zrem', KEYS[2], ARGV[1], id)
        me.state = 'claimed'
        me.version = me.version + 1
        candidate.state = 'claimed'
        candidate.version = candidate.version + 1
        redis.call('set', KEYS[1], cjson.encode(me), 'EX', ARGV[11])
        redis.call('set', ARGV[5] .. id, cjson.encode(candidate), 'EX', ARGV[11])
        redis.call('set', ARGV[6] .. ARGV[7], cjson.encode(claim), 'EX', ARGV[9] + ARGV[12])
        redis.call('sadd', KEYS[4], ARGV[7])
        if ARGV[14] ~= '' then
          redis.call('set', KEYS[3], cjson.encode(claim), 'EX', ARGV[12])
        end
        return {'matched', cjson.encode(claim)}
      end
    end
  end
end
return {'none'}
`;

const COMMIT_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local claim = cjson.decode(raw)
if claim.state == 'committed' then return {'committed'} end
if claim.state ~= 'reserved' then return {'ignored'} end
for index, key in ipairs({KEYS[2], KEYS[3]}) do
  local participantRaw = redis.call('get', key)
  if not participantRaw then return {'conflict'} end
  local participant = cjson.decode(participantRaw)
  local snapshot = claim.participants[index]
  if participant.state ~= 'claimed' or participant.version ~= snapshot.version + 1 then return {'conflict'} end
end
for index, key in ipairs({KEYS[2], KEYS[3]}) do
  local participant = cjson.decode(redis.call('get', key))
  participant.state = 'matched'
  participant.version = participant.version + 1
  redis.call('set', key, cjson.encode(participant), 'EX', ARGV[1])
end
claim.state = 'committed'
redis.call('set', KEYS[1], cjson.encode(claim), 'EX', ARGV[1])
redis.call('set', KEYS[4], cjson.encode(claim), 'EX', ARGV[1])
return {'committed'}
`;

const ROLLBACK_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local claim = cjson.decode(raw)
if claim.state == 'rolled_back' then return {'rolled_back'} end
if claim.state ~= 'reserved' then return {'ignored'} end
for index, key in ipairs({KEYS[2], KEYS[3]}) do
  local participantRaw = redis.call('get', key)
  if participantRaw then
    local participant = cjson.decode(participantRaw)
    local snapshot = claim.participants[index]
    if participant.state == 'claimed' and participant.version == snapshot.version + 1 then
      participant.state = 'waiting'
      participant.version = participant.version + 1
      redis.call('set', key, cjson.encode(participant), 'EX', ARGV[1])
      redis.call('zadd', KEYS[4], participant.joinedAt, participant.id)
    end
  end
end
claim.state = 'rolled_back'
redis.call('set', KEYS[1], cjson.encode(claim), 'EX', ARGV[1])
redis.call('del', KEYS[5])
return {'rolled_back'}
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
