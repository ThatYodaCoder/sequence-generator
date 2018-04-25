package com.sequence.factory;

import com.sequence.Sequence;
import com.sequence.SequenceImplOpt;
import javax.sql.DataSource;

public final class SequenceFactory {

  private SequenceFactory() {
  }

  public static Sequence getSequence(String sequenceName, DataSource dataSource) {
    return new SequenceImplOpt(sequenceName, dataSource);
  }
}
