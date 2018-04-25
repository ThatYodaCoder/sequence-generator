package com.sequence.generator.factory;

import com.sequence.generator.OracleNextNumGenerator;
import org.junit.Assert;
import org.junit.Test;

public class NextNumGeneratorFactoryTest {

  @Test
  public void testGetNextNumGenerator() {
    Assert.assertTrue(NextNumGeneratorFactory.getNextNumGenerator(null) instanceof OracleNextNumGenerator);
  }

}