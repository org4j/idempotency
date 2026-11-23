package io.github.org4j.idempotency;

import io.github.org4j.error.StatusThrowable;

/** The codes of the {@code GeneralException}s {@link Idempotency} throws. */
public enum IdempotencyExceptionStatus implements StatusThrowable {
  /** 422: the key was used for a different request. */
  IDEMPOTENCY_KEY_REUSED,
  /** 409: the work for this key is still running, or ended without an answer. */
  IDEMPOTENCY_IN_PROGRESS;

  @Override
  public String getType() {
    return name();
  }
}
