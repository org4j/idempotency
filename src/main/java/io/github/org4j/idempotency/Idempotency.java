package io.github.org4j.idempotency;

import io.github.org4j.error.GeneralException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * The first answer to each client's idempotency key, kept in a table of the module's own schema: a
 * retry gets the same answer, and a different request with the same key is refused.
 *
 * <p>The key is claimed, and committed, before the work runs, so the work runs once even for
 * concurrent requests. Its outcome then decides:
 *
 * <ul>
 *   <li>an answer is kept, and replayed to every retry;
 *   <li>a {@link GeneralException} frees the key: the client may correct the request and retry;
 *   <li>anything else leaves the outcome unknown, and the key in progress: the work is never run
 *       again, unless an {@code inProgressTimeout} says it may be.
 * </ul>
 */
public class Idempotency {

  private final JdbcClient jdbc;
  private final TransactionTemplate own;
  private final JsonMapper json;
  private final @Nullable Duration keep;
  private final @Nullable Duration inProgressTimeout;
  private final Clock clock;
  private final String find;
  private final String claim;
  private final String answer;
  private final String release;
  private final String expire;
  private final String expireInProgress;
  private final String purge;

  /**
   * Keys kept until {@link #purgeBefore}; an unknown outcome is never run again.
   *
   * @param schema the module's schema, a lower-case SQL name
   * @param table the table {@code db/changelog/platform-idempotency.yaml} made there
   */
  public Idempotency(
      String schema,
      String table,
      DataSource dataSource,
      PlatformTransactionManager transactionManager,
      JsonMapper json) {
    this(schema, table, dataSource, transactionManager, json, null, null, Clock.systemUTC());
  }

  /**
   * Keys that expire, measured by a clock of the caller's.
   *
   * @param schema the module's schema, a lower-case SQL name
   * @param table the table {@code db/changelog/platform-idempotency.yaml} made there
   * @param keep how long a key holds; older, it is free again. {@code null}: until {@link
   *     #purgeBefore}
   * @param inProgressTimeout how long a key without an answer holds; older, its work runs again on
   *     a retry. Only for work that is safe to run twice (an outside call that takes the same key),
   *     and longer than the work can take. {@code null}: an unknown outcome is never run again
   * @param clock what a key's age is measured by
   */
  public Idempotency(
      String schema,
      String table,
      DataSource dataSource,
      PlatformTransactionManager transactionManager,
      JsonMapper json,
      @Nullable Duration keep,
      @Nullable Duration inProgressTimeout,
      Clock clock) {
    var target = name(schema) + "." + name(table);
    this.jdbc = JdbcClient.create(dataSource);
    // The claim, the answer and the release commit at once, whatever transaction the caller is in:
    // a concurrent request must see them.
    this.own = new TransactionTemplate(transactionManager);
    this.own.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    this.json = json;
    this.keep = keep;
    this.inProgressTimeout = inProgressTimeout;
    this.clock = Objects.requireNonNull(clock);
    this.find = IdempotencyConstants.FIND.formatted(target);
    this.claim = IdempotencyConstants.CLAIM.formatted(target);
    this.answer = IdempotencyConstants.ANSWER.formatted(target);
    this.release = IdempotencyConstants.RELEASE.formatted(target);
    this.expire = IdempotencyConstants.EXPIRE.formatted(target);
    this.expireInProgress = IdempotencyConstants.EXPIRE_IN_PROGRESS.formatted(target);
    this.purge = IdempotencyConstants.PURGE.formatted(target);
  }

  /**
   * What {@link #execute} answers.
   *
   * @param response the work's answer, or the first one for a replay
   * @param replayed whether the work ran for an earlier request rather than this one
   */
  public record Result<T>(@Nullable T response, boolean replayed) {}

  /**
   * Runs the work once per client and key, or replays its answer.
   *
   * <p>Inside a transaction, the answer is kept once that transaction commits: if it rolls back,
   * the outcome is unknown, as the work's effects outside the database may have happened.
   *
   * @param requestHash the request's hash; a different one with the same key is refused
   * @param type the answer's type, read back from JSON for a replay
   * @throws GeneralException {@code IDEMPOTENCY_IN_PROGRESS} while the key has no answer, {@code
   *     IDEMPOTENCY_KEY_REUSED} for a different request, or the work's own
   */
  public <T> Result<T> execute(
      String clientId, String key, String requestHash, Class<T> type, Supplier<T> work) {
    if (claim(clientId, key, requestHash) == 0) {
      return earlier(clientId, key, requestHash, type);
    }
    T response;
    try {
      response = work.get();
    } catch (GeneralException refused) {
      ownTransaction(() -> jdbc.sql(release).param("client", clientId).param("key", key).update());
      throw refused;
    }
    var kept = json.writeValueAsString(response);
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              answered(clientId, key, kept);
            }
          });
    } else {
      answered(clientId, key, kept);
    }
    return new Result<>(response, false);
  }

  /** {@link #execute} for work without an answer, e.g. a 204. */
  public Result<Void> run(String clientId, String key, String requestHash, Runnable work) {
    return execute(
        clientId,
        key,
        requestHash,
        Void.class,
        () -> {
          work.run();
          return null;
        });
  }

  /**
   * Forgets every key claimed before then, answered or not.
   *
   * @return how many were forgotten
   */
  public int purgeBefore(Instant before) {
    return jdbc.sql(purge).param("before", Timestamp.from(before)).update();
  }

  private int claim(String clientId, String key, String requestHash) {
    return ownTransaction(
        () -> {
          var now = clock.instant();
          if (inProgressTimeout != null) {
            forget(expireInProgress, clientId, key, now.minus(inProgressTimeout));
          }
          if (keep != null) {
            forget(expire, clientId, key, now.minus(keep));
          }
          return jdbc.sql(claim)
              .param("client", clientId)
              .param("key", key)
              .param("hash", requestHash)
              .param("now", Timestamp.from(now))
              .update();
        });
  }

  private void forget(String statement, String clientId, String key, Instant before) {
    jdbc.sql(statement)
        .param("client", clientId)
        .param("key", key)
        .param("before", Timestamp.from(before))
        .update();
  }

  private void answered(String clientId, String key, String response) {
    ownTransaction(
        () ->
            jdbc.sql(answer)
                .param("client", clientId)
                .param("key", key)
                .param("response", response)
                .update());
  }

  private <T> Result<T> earlier(String clientId, String key, String requestHash, Class<T> type) {
    var kept =
        jdbc.sql(find)
            .param("client", clientId)
            .param("key", key)
            .query((rs, n) -> new Kept(rs.getString("request_hash"), rs.getString("response")))
            .optional()
            .orElseThrow(
                () -> new GeneralException(IdempotencyExceptionStatus.IDEMPOTENCY_IN_PROGRESS));
    if (!kept.requestHash().equals(requestHash)) {
      throw new GeneralException(IdempotencyExceptionStatus.IDEMPOTENCY_KEY_REUSED);
    }
    if (kept.response() == null) {
      throw new GeneralException(IdempotencyExceptionStatus.IDEMPOTENCY_IN_PROGRESS);
    }
    return new Result<>(json.readValue(kept.response(), type), true);
  }

  private int ownTransaction(Supplier<Integer> statement) {
    return Objects.requireNonNull(own.execute(status -> statement.get()));
  }

  private static String name(String name) {
    if (!IdempotencyConstants.NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(IdempotencyConstants.NAME_INVALID.formatted(name));
    }
    return name;
  }

  private record Kept(String requestHash, @Nullable String response) {}
}
