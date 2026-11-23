# idempotency

The first answer to a client's `Idempotency-Key`, kept in a table in the module's own schema: a
retry gets the same answer, and a different request with the same key is refused.

```groovy
implementation libs.idempotency
```

The module's changelog sets the table's place, then includes org4j's:

```yaml
databaseChangeLog:
  - property: { name: idempotency.schema, value: card }
  - property: { name: idempotency.table, value: card_idempotency }
  - include: { file: db/changelog/platform-idempotency.yaml }
```

```java
var idempotency = new Idempotency("card", "card_idempotency", dataSource, transactionManager, json);
var result = idempotency.execute(clientId, key, requestHash, PaymentResult.class, () -> pay(request));
idempotency.run(clientId, key, requestHash, () -> cancel(request)); // work without an answer, e.g. a 204
```

## How a key behaves

The key is claimed, and committed, before the work runs: a concurrent request with the same key
never runs the work, so it is safe for work that calls outside systems (a debit), not only for
database writes. Keys are the client's own: two clients may use the same one.

| The request | Answer |
|---|---|
| a new key | the work runs; `replayed` is false |
| the same key and request hash, answered | the first answer, `replayed` true; the work doesn't run |
| the same key, a different request hash | `IDEMPOTENCY_KEY_REUSED` (422) |
| the same key while the work runs, or after it ended without an answer | `IDEMPOTENCY_IN_PROGRESS` (409): the client asks for the status |

| The work | Then |
|---|---|
| answers | the answer is kept |
| throws a `GeneralException` | the key is free again: the client may correct the request and retry |
| throws anything else, or the process stops | the outcome is unknown: the key stays in progress, and the work never runs again |
| runs in the caller's transaction | the answer is kept once that transaction commits; if it rolls back, the outcome is unknown |

## Options

The second constructor takes three more:

| Option | Default | What |
|---|---|---|
| `keep` | none: until `purgeBefore` | a key older than this is free again, answered or not |
| `inProgressTimeout` | none: never | a key without an answer older than this is free again, and its work runs again on a retry. Only for work safe to run twice (an outside call that takes the same key), and longer than the work can take |
| `clock` | UTC | what a key's age is measured by |

`purgeBefore(Instant)` forgets every key claimed before then, answered or not; the module runs it on
its own schedule.

## Tests

- `./gradlew test`: names that aren't plain SQL names are refused.
- `./gradlew integrationTest`: `IdempotencyIT` is the specification, on a real Postgres, one nested
  group each: answers (replays, a reused key, two clients), outcomes (a refusal, an unknown outcome,
  the in-progress timeout), concurrency (ten requests, one run), transactions (commit and rollback)
  and age (`keep`, the clock, the purge).
