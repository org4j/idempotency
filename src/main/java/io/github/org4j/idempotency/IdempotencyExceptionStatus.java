package io.github.org4j.idempotency;

import io.github.org4j.error.StatusThrowable;

public enum IdempotencyExceptionStatus implements StatusThrowable {
  IDEMPOTENCY_KEY_REUSED,
  IDEMPOTENCY_IN_PROGRESS;

  @Override
  public String getType() {
    return name();
  }
}
