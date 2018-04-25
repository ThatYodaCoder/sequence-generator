package com.sequence.generator;

public interface NextNumGenerator {

  long getNextNum(String seqName);

  int getIncrementByValue(String seqName);

}
