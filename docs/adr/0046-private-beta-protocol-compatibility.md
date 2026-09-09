# ADR-0046: Private-beta protocol compatibility closure

- Status: Accepted
- Date: 2026-09-09
- Owners: data plane, protocol compatibility, verification
- Extends: [ADR-0044](0044-native-backed-protocol-state.md)

## Context

The first native-backed compatibility slice left three migration-critical gaps.
Redis `MSET` was translated into independent writes and therefore violated its
all-or-nothing contract. Kafka clients configured with `group.instance.id`
could not join even though Epoch already had durable group generations and
member identities. AMQP headers routing and dead-letter declarations were
rejected despite native Queue dead-letter forwarding already being replicated.

Implementing these only as gateway conveniences would create false guarantees:
partial Redis writes, disposable dead letters, or Kafka behavior stronger than
the native session state can fence. The private-beta contract needs a narrow
but truthful translation for each feature.

## Decision

1. Translate `MSET` and `MSETNX` into one revision-fenced native Cache
   transaction, capped at 128 distinct keys. `MSETNX` observes every key and
   commits only if all are missing. A revision conflict retries the complete
   transaction at most four times; it never exposes a partial success.
   Repeated command keys are normalized before submission with the final value
   winning, matching Redis command behavior.
2. Encode a Kafka static identity as a bounded, reversible member ID containing
   the Stream and `group.instance.id`. Rejoining with the same identity reuses
   the native member and generation; join, sync, heartbeat, offset commit, and
   leave reject a mismatched instance with `FENCED_INSTANCE_ID`. The current
   native group model does not carry a separate live-owner epoch, so simultaneous
   duplicate owners and cooperative handoff remain unsupported and unclaimed.
3. Add process-shared direct, fanout, topic, and headers exchanges. Headers
   bindings accept only bounded UTF-8 string criteria plus an explicit
   `x-match=all|any`. Routing is evaluated after the AMQP content header arrives;
   routing keys are ignored for headers exchanges. Non-string or unknown
   binding arguments fail closed.
4. Accept Queue dead-letter arguments only when
   `x-dead-letter-exchange` names the default exchange and
   `x-dead-letter-routing-key` exactly matches the Queue's provisioned native
   `advanced.dead_letter_target`. Regional discovery exposes only that target,
   not arbitrary Queue configuration. Reject/nack without requeue continues to
   use the native replicated reject and dead-letter outbox; the gateway never
   acknowledges a lossy republish.
5. Before an AMQP-origin dead letter is forwarded, remove its original TTL and
   expiration, retarget its routing metadata to the destination Queue through
   the default exchange, and add bounded `x-first-death-exchange`,
   `x-first-death-queue`, and `x-first-death-reason` string headers. The AMQP
   `x-death` table array remains unsupported because the gateway currently
   preserves only string headers. Snapshot restoration accepts both the new
   deterministic forward envelope and the legacy original envelope so an
   upgrade cannot strand a pending pre-transformation forward.
6. Expand exact-client and regional recovery evidence to cover `MSETNX`, Kafka
   static identity rejoin, headers routing, DLX declaration, durable forwarding,
   and recovery through gateway and leader failure. Malformed arguments and
   identity mismatches remain unit-level regression cases as well.

## Consequences

Redis multi-set now has one native linearization point. Kafka Java clients can
use a bounded static identity and preserve it across gateway replacement, but
applications that require two simultaneously competing processes with the same
instance ID must not use this revision. AMQP headers exchanges remain gateway
topology and must be redeclared per gateway, while dead letters themselves and
their forwarding are durable native Queue state.

Named AMQP dead-letter exchanges, full field-table value matching, durable
exchange/binding metadata, the `x-death` array, Kafka cooperative/new group
protocols, idempotent or transactional producers, Redis scripts/transactions,
and comparative performance certification remain separate gates.

## Rejected alternatives

- Sequential Redis writes were rejected because rollback cannot repair a
  visible partial `MSET` after a process or leader failure.
- A gateway-side AMQP publish followed by acknowledge was rejected because the
  crash boundary can lose or duplicate the dead letter.
- Advertising cooperative Kafka assignment while using eager native ownership
  was rejected because a protocol name is not equivalent to revoke/claim
  coordination.
- Accepting arbitrary DLX arguments and silently forwarding to one Queue was
  rejected because it would misrepresent RabbitMQ exchange routing semantics.
