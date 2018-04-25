package com.sequence;

import com.sequence.generator.NextNumGenerator;
import com.sequence.generator.factory.NextNumGeneratorFactory;
import javax.sql.DataSource;

public abstract class AbstractSequence implements Sequence {

  protected String sequenceName;
  protected int incrementBy;
  protected DataSource dataSource;
  protected NextNumGenerator nextNumGenerator;

  /**
   * Added for Junit Testing
   */
  protected int noOfTimesPopulateInvoked;

  public AbstractSequence(String sequenceName, DataSource dataSource) {
    this.sequenceName = sequenceName;
    this.dataSource = dataSource;
  }

  protected void init() {
    nextNumGenerator = NextNumGeneratorFactory.getNextNumGenerator(dataSource);
    this.incrementBy = nextNumGenerator.getIncrementByValue(this.sequenceName);
  }
}
