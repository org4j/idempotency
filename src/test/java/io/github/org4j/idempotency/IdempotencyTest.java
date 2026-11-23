package io.github.org4j.idempotency;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

class IdempotencyTest {

  @Test
  void aSchemaThatIsNotAPlainSqlNameIsRefused() {
    assertThatThrownBy(() -> idempotency("note; DROP TABLE x", "note_idempotency"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("note; DROP TABLE x");
  }

  @Test
  void aTableThatIsNotALowerCaseSqlNameIsRefused() {
    assertThatThrownBy(() -> idempotency("note", "Note-Idempotency"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Note-Idempotency");
  }

  private static Idempotency idempotency(String schema, String table) {
    return new Idempotency(
        schema,
        table,
        mock(DataSource.class),
        mock(PlatformTransactionManager.class),
        JsonMapper.shared());
  }
}
