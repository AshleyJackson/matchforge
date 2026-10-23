import { z } from "zod";
import { SerializableFilterSchema } from "./schemas";
import {
  AtomicClaimInputSchema,
  ClaimResultSchema,
  MatchClaimSchema,
  ParticipantSchema,
  SyncParticipantInputSchema,
  SyncParticipantResultSchema,
  WaitingEntrySchema,
} from "./schemas";
import {
  claimRequestFingerprint,
  cloneParticipant,
  decodeClaimCursor,
  syncedParticipantBody,
} from "./utils";

type RedisMethod = (...args: any[]) => Promise<unknown>;

/**
 * Structural contract for the Redis commands used by this adapter.
 *
 * The broad argument lists accommodate client-specific overloads. Runtime
 * validation checks that every command the adapter needs is present.
 */
export interface RedisLike {
  get: RedisMethod;
  set: RedisMethod;
  del: RedisMethod;
  zadd: RedisMethod;
  zrem: RedisMethod;
  zscore: RedisMethod;
  zrange: RedisMethod;
  sadd: RedisMethod;
  srem: RedisMethod;
  smembers: RedisMethod;
  sscan?: RedisMethod;
  eval: RedisMethod;
}

const RedisMethodSchema = z.custom<RedisMethod>(
  (value) => typeof value === "function",
);

/**
 * Minimal Redis contract implemented by both Upstash Redis and native clients.
 *
 * The adapter deliberately depends on a structural contract instead of a
 * Redis package, keeping `matchforge` usable in Bun, Node, and edge runtimes.
 */
export const RedisClientSchema = z.object({
  get: RedisMethodSchema,
  set: RedisMethodSchema,
  del: RedisMethodSchema,
  zadd: RedisMethodSchema,
  zrem: RedisMethodSchema,
  zscore: RedisMethodSchema,
  zrange: RedisMethodSchema,
  sadd: RedisMethodSchema,
  srem: RedisMethodSchema,
  smembers: RedisMethodSchema,
  sscan: RedisMethodSchema.optional(),
  eval: RedisMethodSchema,
}).loose();

export const RedisCoordinationStoreOptionsSchema = z.object({
  redis: RedisClientSchema,
  namespace: z.string().optional(),
  participantTtlSeconds: z.number().int().positive().optional(),
  claimTtlSeconds: z.number().int().positive().optional(),
  claimRetentionSeconds: z.number().int().positive().optional(),
}).strict();

export type RedisCoordinationStoreOptions = Omit<
  z.input<typeof RedisCoordinationStoreOptionsSchema>,
  "redis"
> & { redis: RedisLike };

const ScanCountSchema = z.coerce.number().int().nonnegative();
const ScriptStatusSchema = z.array(z.unknown()).min(1);

/** Upper bound on candidates evaluated by one claim script call. */
export const REDIS_MAX_SCAN_LIMIT = 100;

/**
 * Redis coordination adapter for coarse, pre-partitioned pools.
 *
 * Redis Lua cannot execute arbitrary TypeScript compatibility or scoring
 * functions. Callers using this adapter must encode hard constraints as a
 * serializable filter and use FIFO or random selection.
 */
export class RedisCoordinationStore {
  private readonly redis: RedisLike;
  private readonly namespace: string;
  private readonly participantTtlSeconds: number;
  private readonly claimTtlSeconds: number;
  private readonly claimRetentionSeconds: number;

  constructor(options: RedisCoordinationStoreOptions) {
    const validated = RedisCoordinationStoreOptionsSchema.parse(options);
    // Zod copies object members into a new plain object, which would detach
    // prototype methods from their client instance.
    this.redis = options.redis;
    this.namespace = validated.namespace ?? "matchforge:v1";
    this.participantTtlSeconds = validated.participantTtlSeconds ?? 90;
    this.claimTtlSeconds = validated.claimTtlSeconds ?? 30;
    this.claimRetentionSeconds = validated.claimRetentionSeconds ?? 300;
  }

  async atomicClaim(input: z.infer<typeof AtomicClaimInputSchema>): Promise<z.infer<typeof ClaimResultSchema>> {
    if (
      input.compatibility ||
      input.score ||
      input.relax ||
      (input.strategy.type !== "fifo" && input.strategy.type !== "random")
    ) {
      return {
        status: "error",
        error: new Error(
          "RedisCoordinationStore supports FIFO and random claims with serializable filters only",
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
    const cursor = input.cursor ? decodeClaimCursor(input.cursor) : null;
    const requestFingerprint =
      input.requestFingerprint ?? claimRequestFingerprint({ ...input, filter });

    const raw = ScriptStatusSchema.parse(await this.redis.eval(
      CLAIM_SCRIPT,
      [
        this.participantKey(input.requesterId),
        this.waitingKey(input.poolKey),
        this.idempotencyKey(input.idempotencyKey ?? input.claimId),
        this.claimExpiryKey(),
      ],
      [
        input.requesterId,
        String(input.now),
        String(Math.min(input.scanLimit, REDIS_MAX_SCAN_LIMIT)),
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
        input.strategy.type,
        input.strategy.type === "random" ? String(input.random()) : "0",
        input.maxHeartbeatAgeMs === undefined ? "" : String(input.maxHeartbeatAgeMs),
        cursor ? String(cursor.joinedAt) : "",
        cursor ? cursor.participantId : "",
      ],
    ));

    switch (raw[0]) {
      case "error":
        return {
          status: "error",
          error: new Error(typeof raw[1] === "string" ? raw[1] : "Redis claim error"),
        };
      case "stale":
        return {
          status: "stale",
          reason: typeof raw[1] === "string" ? raw[1] : "requester is not queued",
        };
      case "idempotent":
        return {
          status: "matched",
          claim: parseClaimResult(raw[1]),
          scanned: 0,
          stale: 0,
          replayed: true,
        };
      case "matched":
        return {
          status: "matched",
          claim: parseClaimResult(raw[1]),
          scanned: ScanCountSchema.parse(raw[2] ?? 0),
          stale: ScanCountSchema.parse(raw[3] ?? 0),
        };
      default: {
        const nextCursor = raw[3] === undefined || raw[3] === null ? "" : String(raw[3]);
        return {
          status: "none",
          scanned: ScanCountSchema.parse(raw[1] ?? 0),
          stale: ScanCountSchema.parse(raw[2] ?? 0),
          ...(nextCursor ? { cursor: nextCursor } : {}),
        };
      }
    }
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

  /**
   * Creates or replaces a participant record and its waiting-index entries in
   * one atomic step. The store assigns the next version. A participant held by
   * a live claim is never overwritten.
   */
  async syncParticipant(
    input: z.infer<typeof SyncParticipantInputSchema>,
  ): Promise<z.infer<typeof SyncParticipantResultSchema>> {
    const validated = SyncParticipantInputSchema.parse(input);
    const raw = ScriptStatusSchema.parse(await this.redis.eval(
      SYNC_PARTICIPANT_SCRIPT,
      [this.participantKey(validated.id)],
      [
        JSON.stringify(syncedParticipantBody(validated)),
        validated.expectedVersion === undefined ? "" : String(validated.expectedVersion),
        validated.revision === undefined ? "" : String(validated.revision),
        String(this.participantTtlSeconds),
        this.waitingPrefix(),
      ],
    ));
    const status = z
      .enum(["created", "updated", "claimed", "conflict", "stale"])
      .parse(raw[0]);
    const participant =
      raw[1] === undefined || raw[1] === null || raw[1] === "" || raw[1] === false
        ? null
        : parseParticipantResult(raw[1]);
    return SyncParticipantResultSchema.parse({ status, participant });
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
    if (validated.state === "reserved") {
      await this.redis.zadd(this.claimExpiryKey(), {
        score: validated.expiresAt,
        member: validated.claimId,
      });
    } else {
      await this.redis.zrem(this.claimExpiryKey(), validated.claimId);
    }
  }

  async commitClaim(claimId: string): Promise<boolean> {
    return this.runCommit(claimId, false);
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
        this.claimExpiryKey(),
      ],
      [String(this.participantTtlSeconds), claimId],
    ));
    return result?.[0] === "rolled_back";
  }

  async clearIdempotency(key: string): Promise<void> {
    await this.redis.del(this.idempotencyKey(key));
  }

  async getQueueDepth(poolKey: string): Promise<number> {
    return ScanCountSchema.parse(
      await this.redis.eval(QUEUE_DEPTH_SCRIPT, [this.waitingKey(poolKey)], []),
    );
  }

  /**
   * Recovers at most `limit` (default 100) claims whose TTL has passed, oldest
   * expiry first. Run it repeatedly from a sweeper; each pass only reads the
   * expired slice of the claim-expiry index.
   */
  async recoverExpiredClaims(
    now: number,
    hasDurableMatch?: (claim: z.infer<typeof MatchClaimSchema>) => boolean | Promise<boolean>,
    options: { limit?: number } = {},
  ): Promise<z.infer<typeof MatchClaimSchema>[]> {
    const limit = z.number().int().positive().parse(options.limit ?? 100);
    const due = z.array(z.unknown()).parse(await this.redis.eval(
      LIST_EXPIRED_CLAIMS_SCRIPT,
      [this.claimExpiryKey(), this.legacyClaimIndexKey()],
      [String(now), String(limit), this.claimPrefix()],
    ));
    const recovered: z.infer<typeof MatchClaimSchema>[] = [];
    for (const value of due) {
      const claim = parseClaimResult(value);
      if (hasDurableMatch && (await hasDurableMatch(claim))) {
        if (await this.runCommit(claim.claimId, true))
          recovered.push({ ...claim, state: "committed" });
        continue;
      }
      const result = z.array(z.unknown()).parse(await this.redis.eval(
        RECOVER_SCRIPT,
        [
          this.claimKey(claim.claimId),
          this.waitingKey(claim.poolKey),
          this.participantKey(claim.participants[0]!.id),
          this.participantKey(claim.participants[1]!.id),
          this.claimExpiryKey(),
          this.idempotencyKey(claim.recoveryToken),
        ],
        [String(this.participantTtlSeconds), String(now)],
      ));
      if (result?.[0] === "expired") recovered.push(parseClaimResult(result[1]));
    }
    return recovered;
  }

  /**
   * With `reconcile`, a claim backed by a durable match is completed even if a
   * participant has since changed; only participants still held by the claim
   * move to `matched`. Without it, any such change rejects the commit.
   */
  private async runCommit(claimId: string, reconcile: boolean): Promise<boolean> {
    const result = z.array(z.string()).parse(await this.redis.eval(
      COMMIT_SCRIPT,
      [this.claimKey(claimId), this.claimExpiryKey()],
      [
        this.participantPrefix(),
        this.idempotencyPrefix(),
        String(this.participantTtlSeconds),
        String(this.claimRetentionSeconds),
        reconcile ? "1" : "0",
        claimId,
      ],
    ));
    return result?.[0] === "committed";
  }

  private keyPrefix() {
    return `${this.namespace}:{${this.namespace}}:`;
  }

  private participantPrefix() {
    return `${this.keyPrefix()}participant:`;
  }

  private claimPrefix() {
    return `${this.keyPrefix()}claim:`;
  }

  /** Sorted set of reserved claim IDs scored by `expiresAt`. */
  private claimExpiryKey() {
    return `${this.keyPrefix()}claim-expiry`;
  }

  /** Unsorted claim set used by earlier releases; drained into the expiry index. */
  private legacyClaimIndexKey() {
    return `${this.keyPrefix()}claims`;
  }

  private participantKey(id: string) {
    return `${this.participantPrefix()}${id}`;
  }

  private claimKey(id: string) {
    return `${this.claimPrefix()}${id}`;
  }

  private idempotencyKey(id: string) {
    return `${this.idempotencyPrefix()}${id}`;
  }

  private idempotencyPrefix() {
    return `${this.keyPrefix()}idempotency:`;
  }

  private waitingPrefix() {
    return `${this.keyPrefix()}waiting:`;
  }

  private waitingKey(pool: string) {
    return `${this.waitingPrefix()}${pool}`;
  }
}

// REST clients such as Upstash deserialize JSON strings in script replies, so
// records can arrive either as strings or as parsed objects.
function parseClaimResult(value: unknown): z.infer<typeof MatchClaimSchema> {
  if (typeof value === "string")
    return MatchClaimSchema.parse(JSON.parse(value));
  return MatchClaimSchema.parse(value);
}

function parseParticipantResult(value: unknown): z.infer<typeof ParticipantSchema> {
  if (typeof value === "string")
    return ParticipantSchema.parse(JSON.parse(value));
  return ParticipantSchema.parse(value);
}

const QUEUE_DEPTH_SCRIPT = `return redis.call('zcard', KEYS[1])`;

const UPDATE_PARTICIPANT_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local participant = cjson.decode(raw)
if participant.version ~= tonumber(ARGV[1]) then return {'conflict'} end
redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3])
return {'updated'}
`;

// The record body is spliced rather than re-encoded so cjson cannot turn
// empty arrays in preferences into empty objects.
const SYNC_PARTICIPANT_SCRIPT = `
local raw = redis.call('get', KEYS[1])
local current = nil
if raw then current = cjson.decode(raw) end
if current and current.state == 'claimed' then return {'claimed', raw} end
if ARGV[2] ~= '' then
  local currentVersion = 0
  if current then currentVersion = current.version end
  if currentVersion ~= tonumber(ARGV[2]) then return {'conflict', raw or ''} end
end
if ARGV[3] ~= '' and current and type(current.syncRevision) == 'number'
  and current.syncRevision >= tonumber(ARGV[3]) then
  return {'stale', raw}
end
local desired = cjson.decode(ARGV[1])
local version = 1
if current then version = current.version + 1 end
local head = '{"version":' .. string.format('%d', version) .. ','
if ARGV[3] == '' and current and type(current.syncRevision) == 'number' then
  head = head .. '"syncRevision":' .. string.format('%d', current.syncRevision) .. ','
end
local encoded = head .. string.sub(ARGV[1], 2)
redis.call('set', KEYS[1], encoded, 'EX', ARGV[4])
local keep = {}
if desired.state == 'waiting' then
  for _, pool in ipairs(desired.pools) do
    keep[pool] = true
    redis.call('zadd', ARGV[5] .. pool, desired.joinedAt, desired.id)
  end
end
local function release(pools)
  if type(pools) ~= 'table' then return end
  for _, pool in ipairs(pools) do
    if not keep[pool] then redis.call('zrem', ARGV[5] .. pool, desired.id) end
  end
end
release(desired.pools)
if current then release(current.pools) end
if current then return {'updated', encoded} end
return {'created', encoded}
`;

const CLAIM_SCRIPT = `
local now = tonumber(ARGV[2])
if ARGV[14] ~= '' then
  local existing = redis.call('get', KEYS[3])
  if existing then
    local previous = cjson.decode(existing)
    local claimRaw = false
    if type(previous.claimId) == 'string' then
      claimRaw = redis.call('get', ARGV[6] .. previous.claimId)
    end
    if claimRaw then
      local previousClaim = cjson.decode(claimRaw)
      if previousClaim.state == 'reserved' or previousClaim.state == 'committed' then
        if previous.requestFingerprint ~= ARGV[15] then
          return {'error', 'idempotency key reused with different request'}
        end
        return {'idempotent', claimRaw}
      end
    end
    redis.call('del', KEYS[3])
  end
end
local requester = redis.call('get', KEYS[1])
if not requester then return {'stale', 'requester is missing'} end
local me = cjson.decode(requester)
if me.state ~= 'waiting' then return {'stale', 'requester is not waiting'} end
if not redis.call('zscore', KEYS[2], ARGV[1]) then return {'stale', 'requester is not queued'} end

local excluded = {}
local excludedCount = 0
for _, id in ipairs(cjson.decode(ARGV[4])) do
  if not excluded[id] then
    excluded[id] = true
    excludedCount = excludedCount + 1
  end
end
local limit = tonumber(ARGV[3])
local maxHeartbeatAge = tonumber(ARGV[18])
local cursorScore = tonumber(ARGV[19])
local cursorMember = ARGV[20]

-- Byte-wise comparison, matching sorted-set member order; Lua string
-- comparison follows the server's collation locale instead.
local function member_at_or_before(a, b)
  local length = math.min(#a, #b)
  for index = 1, length do
    local x = string.byte(a, index)
    local y = string.byte(b, index)
    if x ~= y then return x < y end
  end
  return #a <= #b
end

-- Excluded IDs and the requester do not use up the candidate budget, so the
-- page is widened by that many entries.
local pageSize = limit + excludedCount + 1
local page
if cursorScore then
  page = redis.call('zrangebyscore', KEYS[2], ARGV[19], '+inf', 'WITHSCORES', 'LIMIT', 0, pageSize)
else
  page = redis.call('zrange', KEYS[2], 0, pageSize - 1, 'WITHSCORES')
end
local pageCount = #page / 2
local window = {}
local consumed = 0
local lastScore = nil
local lastMember = nil
for index = 1, pageCount do
  if #window >= limit then break end
  local member = page[index * 2 - 1]
  local score = page[index * 2]
  consumed = index
  lastScore = score
  lastMember = member
  local seen = cursorScore and tonumber(score) == cursorScore and member_at_or_before(member, cursorMember)
  if not seen and member ~= ARGV[1] and not excluded[member] then
    window[#window + 1] = member
  end
end
local more = consumed < pageCount or pageCount == pageSize

local function read_path(root, path)
  local value = root
  for _, segment in ipairs(path or {}) do
    if type(value) ~= 'table' then return nil end
    value = value[segment]
  end
  return value
end
local function operand_value(operand, candidate)
  if operand.literal ~= nil then return operand.literal end
  if operand.source == 'requester' then return read_path(me.preferences, operand.path) end
  return read_path(candidate.preferences, operand.path)
end
local function filter_matches(filter, candidate)
  if filter.op == 'all' then
    for _, nested in ipairs(filter.filters) do
      if not filter_matches(nested, candidate) then return false end
    end
    return true
  elseif filter.op == 'any' then
    for _, nested in ipairs(filter.filters) do
      if filter_matches(nested, candidate) then return true end
    end
    return false
  elseif filter.op == 'not' then
    return not filter_matches(filter.filter, candidate)
  elseif filter.op == 'overlaps' then
    local left = operand_value(filter.left, candidate)
    local right = operand_value(filter.right, candidate)
    if type(left) ~= 'table' or type(right) ~= 'table' then return false end
    for _, left_value in ipairs(left) do
      for _, right_value in ipairs(right) do
        if cjson.encode(left_value) == cjson.encode(right_value) then return true end
      end
    end
    return false
  elseif filter.op == 'equals' then
    return cjson.encode(operand_value(filter.left, candidate)) == cjson.encode(operand_value(filter.right, candidate))
  elseif filter.op == 'in' then
    local value = operand_value(filter.left, candidate)
    local values = operand_value(filter.right, candidate)
    if type(values) ~= 'table' then return false end
    for _, item in ipairs(values) do
      if cjson.encode(value) == cjson.encode(item) then return true end
    end
    return false
  elseif filter.op == 'contains' then
    local container = operand_value(filter.left, candidate)
    local value = operand_value(filter.right, candidate)
    if type(container) == 'table' then
      for _, item in ipairs(container) do
        if cjson.encode(value) == cjson.encode(item) then return true end
      end
      return false
    end
    return type(container) == 'string' and type(value) == 'string' and string.find(container, value, 1, true) ~= nil
  elseif filter.op == 'is-empty' then
    local value = operand_value(filter.left, candidate)
    if value == nil then return true end
    if type(value) == 'string' then return string.len(value) == 0 end
    if type(value) == 'table' then return next(value) == nil end
    return false
  end
  return false
end
local function excludes(owner, otherId)
  if type(owner.exclusions) ~= 'table' then return false end
  local untilAt = owner.exclusions[otherId]
  return type(untilAt) == 'number' and untilAt > now
end
local function present(candidate)
  if not maxHeartbeatAge then return true end
  return type(candidate.lastHeartbeatAt) == 'number' and now - candidate.lastHeartbeatAt <= maxHeartbeatAge
end

local filter = cjson.decode(ARGV[13])
local count = #window
local offset = 0
if ARGV[16] == 'random' and count > 0 then
  offset = math.min(math.floor(tonumber(ARGV[17]) * count), count - 1)
end
local scanned = 0
local stale = 0
for index = 1, count do
  local id = window[((offset + index - 1) % count) + 1]
  scanned = scanned + 1
  local raw = redis.call('get', ARGV[5] .. id)
  if not raw then
    redis.call('zrem', KEYS[2], id)
    stale = stale + 1
  else
    local candidate = cjson.decode(raw)
    if candidate.state == 'waiting'
      and present(candidate)
      and not excludes(me, id)
      and not excludes(candidate, ARGV[1])
      and filter_matches(filter, candidate) then
      local expiresAt = now + tonumber(ARGV[9]) * 1000
      local claim = {
        claimId = ARGV[7],
        matchId = ARGV[8],
        poolKey = ARGV[10],
        participants = {
          {id = ARGV[1], version = me.version, snapshot = me.preferences},
          {id = id, version = candidate.version, snapshot = candidate.preferences}
        },
        state = 'reserved',
        createdAt = now,
        expiresAt = expiresAt,
        strategy = ARGV[16],
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
      redis.call('zadd', KEYS[4], string.format('%d', expiresAt), ARGV[7])
      if ARGV[14] ~= '' then
        redis.call('set', KEYS[3], cjson.encode({claimId = ARGV[7], requestFingerprint = ARGV[15]}), 'EX', ARGV[9] + ARGV[12])
      end
      return {'matched', cjson.encode(claim), scanned, stale}
    end
  end
end
local cursor = ''
if more and lastScore then cursor = lastScore .. ':' .. lastMember end
return {'none', scanned, stale, cursor}
`;

const COMMIT_SCRIPT = `
local raw = redis.call('get', KEYS[1])
if not raw then
  redis.call('zrem', KEYS[2], ARGV[6])
  return {'missing'}
end
local claim = cjson.decode(raw)
if claim.state == 'committed' then return {'committed'} end
if claim.state ~= 'reserved' then return {'ignored'} end
local participantKeys = {}
local held = {}
for index, snapshot in ipairs(claim.participants) do
  participantKeys[index] = ARGV[1] .. snapshot.id
  local participantRaw = redis.call('get', participantKeys[index])
  local participant = nil
  if participantRaw then participant = cjson.decode(participantRaw) end
  if participant and participant.state == 'claimed' and participant.version == snapshot.version + 1 then
    held[index] = participant
  elseif ARGV[5] ~= '1' then
    return {'conflict'}
  end
end
for index, key in ipairs(participantKeys) do
  local participant = held[index]
  if participant then
    participant.state = 'matched'
    participant.version = participant.version + 1
    redis.call('set', key, cjson.encode(participant), 'EX', ARGV[3])
  end
end
claim.state = 'committed'
redis.call('set', KEYS[1], cjson.encode(claim), 'EX', ARGV[4])
redis.call('zrem', KEYS[2], claim.claimId)
local idempotencyKey = ARGV[2] .. claim.recoveryToken
if claim.recoveryToken ~= claim.claimId and redis.call('exists', idempotencyKey) == 1 then
  redis.call('expire', idempotencyKey, ARGV[4])
end
return {'committed'}
`;

const RELEASE_IDEMPOTENCY_LUA = `
local function release_idempotency(key, claimId)
  local pointer = redis.call('get', key)
  if pointer then
    local decoded = cjson.decode(pointer)
    if decoded.claimId == claimId then redis.call('del', key) end
  end
end
`;

const ROLLBACK_SCRIPT = `${RELEASE_IDEMPOTENCY_LUA}
local raw = redis.call('get', KEYS[1])
if not raw then
  redis.call('zrem', KEYS[6], ARGV[2])
  return {'missing'}
end
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
redis.call('zrem', KEYS[6], claim.claimId)
release_idempotency(KEYS[5], claim.claimId)
return {'rolled_back'}
`;

// Claims written before the expiry index existed live in an unsorted set;
// each pass moves a bounded batch of them into the index.
const LIST_EXPIRED_CLAIMS_SCRIPT = `
local legacy = redis.call('srandmember', KEYS[2], ARGV[2])
for _, id in ipairs(legacy) do
  local raw = redis.call('get', ARGV[3] .. id)
  if raw then
    local claim = cjson.decode(raw)
    if claim.state == 'reserved' then
      redis.call('zadd', KEYS[1], string.format('%d', claim.expiresAt), id)
    end
  end
  redis.call('srem', KEYS[2], id)
end
local ids = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local due = {}
for _, id in ipairs(ids) do
  local raw = redis.call('get', ARGV[3] .. id)
  local reserved = false
  if raw then reserved = cjson.decode(raw).state == 'reserved' end
  if reserved then
    due[#due + 1] = raw
  else
    redis.call('zrem', KEYS[1], id)
  end
end
return due
`;

const RECOVER_SCRIPT = `${RELEASE_IDEMPOTENCY_LUA}
local raw = redis.call('get', KEYS[1])
if not raw then return {'missing'} end
local claim = cjson.decode(raw)
if claim.state ~= 'reserved' then
  redis.call('zrem', KEYS[5], claim.claimId)
  return {'ignored'}
end
if claim.expiresAt > tonumber(ARGV[2]) then return {'ignored'} end
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
redis.call('zrem', KEYS[5], claim.claimId)
release_idempotency(KEYS[6], claim.claimId)
return {'expired', cjson.encode(claim)}
`;
