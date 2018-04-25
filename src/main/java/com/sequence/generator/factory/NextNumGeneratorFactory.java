package com.sequence.generator.factory;

import com.sequence.generator.NextNumGenerator;
import com.sequence.generator.OracleNextNumGenerator;
import javax.sql.DataSource;

public final class NextNumGeneratorFactory {

  private NextNumGeneratorFactory() {
  }

  public static NextNumGenerator getNextNumGenerator(DataSource dataSource) {
    return new OracleNextNumGenerator(dataSource);
  }
}
