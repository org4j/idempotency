package uz.khakimjanovich.idempotency4j;

import uz.khakimjanovich.error4j.StatusThrowable;

public enum IdempotencyExceptionStatus implements StatusThrowable {
  IDEMPOTENCY_KEY_REUSED,
  IDEMPOTENCY_IN_PROGRESS;

  @Override
  public String getType() {
    return name();
  }
}
