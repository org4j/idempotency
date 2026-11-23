package io.github.org4j.idempotency;

import java.util.regex.Pattern;

final class IdempotencyConstants {

  static final Pattern NAME = Pattern.compile("[a-z_][a-z0-9_]{0,62}");
  static final String NAME_INVALID = "Not a lower-case SQL name: '%s'";

  static final String FIND =
      "SELECT request_hash, response FROM %s WHERE client_id = :client AND idempotency_key = :key";

  static final String CLAIM =
      """
      INSERT INTO %s (client_id, idempotency_key, request_hash, created_at)
      VALUES (:client, :key, :hash, :now)
      ON CONFLICT DO NOTHING
      """;

  static final String ANSWER =
      "UPDATE %s SET response = :response WHERE client_id = :client AND idempotency_key = :key";

  static final String RELEASE =
      "DELETE FROM %s WHERE client_id = :client AND idempotency_key = :key";

  static final String EXPIRE =
      "DELETE FROM %s WHERE client_id = :client AND idempotency_key = :key AND created_at < :before";

  static final String EXPIRE_IN_PROGRESS =
      "DELETE FROM %s WHERE client_id = :client AND idempotency_key = :key AND response IS NULL AND created_at < :before";

  static final String PURGE = "DELETE FROM %s WHERE created_at < :before";

  private IdempotencyConstants() {}
}
