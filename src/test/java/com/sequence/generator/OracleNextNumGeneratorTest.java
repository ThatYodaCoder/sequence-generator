package com.sequence.generator;

import com.sequence.exception.SequenceGenerationException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleNextNumGeneratorTest {

  private static final String SEQ_NAME = "TEST_SEQ";
  private static final String url = "jdbc:oracle:thin:@db-server:1521:testSvc";
  private static final String userName = "test";
  private static final String psswrd = "test";
  private static final String driverClassName = "oracle.jdbc.driver.OracleDriver";
  private static DataSource dataSource;


  @BeforeClass
  public static void tearUp() {
    dataSource = getDataSource();
    String createSeqSql = "CREATE SEQUENCE TEST_SEQ MINVALUE 1 MAXVALUE 999999999999999999999999999 START WITH 1 INCREMENT BY 15 CACHE 20";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(createSeqSql)) {
      pStmt.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    String nextSeqSql = "select TEST_SEQ.nextval from dual";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(nextSeqSql)) {
      pStmt.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {

    String dropSeqSql = "DROP SEQUENCE TEST_SEQ";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(dropSeqSql)) {
      pStmt.executeQuery();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static DataSource getDataSource() {

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setPoolName("agms-conn-pool");
    config.setDriverClassName(driverClassName);
    config.setUsername(userName);
    config.setPassword(psswrd);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    return new HikariDataSource(config);
  }

  @Test
  public void testGetNextNum() {

    OracleNextNumGenerator oracleNextNumGenerator = new OracleNextNumGenerator(dataSource);

    try {
      long nextNum = oracleNextNumGenerator.getNextNum("TEST_SEQ");
      Assert.assertEquals(16, nextNum);

    } catch (SequenceGenerationException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetIncrementByValue() {

    OracleNextNumGenerator oracleNextNumGenerator = new OracleNextNumGenerator(dataSource);
    try {
      int incrementByValue = oracleNextNumGenerator.getIncrementByValue("TEST_SEQ");
      Assert.assertEquals(15, incrementByValue);
    } catch (SequenceGenerationException e) {
      e.printStackTrace();
    }

  }
}