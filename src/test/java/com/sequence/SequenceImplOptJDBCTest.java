package com.sequence;

import com.sequence.exception.SequenceGenerationException;
import com.sequence.task.SeqTestTask;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

//@Ignore
public class SequenceImplOptJDBCTest {

  private static final String SEQ_NAME = "TEST_SEQ";
  private static final String url = "jdbc:oracle:thin:@db-server:1521:testSvc";
  private static final String userName = "test";
  private static final String passwrd = "test";
  private static final String driverClassName = "oracle.jdbc.driver.OracleDriver";
  private static DataSource dataSource;
  private final int incrementByValue = 50;

  private static Connection getConnection() {

    Connection con = null;
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      con = DriverManager.getConnection("jdbc:oracle:thin:@db-server:1521:testSvc", "test", "test");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return con;
  }

  @BeforeClass
  public static void tearUp() {

    dataSource = getDataSource();

    String createSeqSql = "CREATE SEQUENCE TEST_SEQ MINVALUE 1 MAXVALUE 999999999999999999999999999 START WITH 1 INCREMENT BY 1000 CACHE 20";
    try (Connection connection = dataSource.getConnection();
        PreparedStatement pStmt = connection.prepareStatement(createSeqSql)) {
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
    config.setPoolName("test-conn-pool");
    config.setDriverClassName(driverClassName);
    config.setUsername(userName);
    config.setPassword(passwrd);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    return new HikariDataSource(config);
  }

  @Test
  public void testNext() throws SequenceGenerationException, ExecutionException, InterruptedException {
    multithreadedTester();
  }

  private void multithreadedTester() throws SequenceGenerationException, ExecutionException, InterruptedException {

    int expectedNoOfTimesPopulateInvoked = 4000;
    long expectedMinValue = 1;
    long expectedMaxValue = 4000000;
    long expectedSize = 4000000;

    SequenceImplOpt seqImpl = getSeqImpl();

    for (int noOfThreads = 1; noOfThreads <= 40; noOfThreads = noOfThreads + 2) {

      SortedSet<Long> treeSet = Collections.synchronizedSortedSet(new TreeSet<Long>());

      System.out.println("No of Threads=" + noOfThreads);

      ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads);

      List<Future<Boolean>> futureList = new ArrayList<>();

      long sleepTime = 600;
      for (int ctr = 0; ctr < 40; ctr++) {
        futureList.add(executorService.submit(new SeqTestTask(seqImpl, treeSet, ctr, sleepTime)));
        sleepTime = sleepTime - 15;
      }

      for (Future<Boolean> future : futureList) {
        Assert.assertTrue(future.get().booleanValue());
      }

      System.out.println("No of Threads=" + noOfThreads);
      System.out.println("MinValue=" + treeSet.first().longValue() + " :: MaxValue=" + treeSet.last().longValue());

      Assert.assertEquals(expectedNoOfTimesPopulateInvoked, seqImpl.getNoOfTimesPopulateInvoked());

      Assert.assertEquals(expectedMinValue, treeSet.first().longValue());
      Assert.assertEquals(expectedMaxValue, treeSet.last().longValue());
      Assert.assertEquals(expectedSize, treeSet.size());

      expectedNoOfTimesPopulateInvoked = expectedNoOfTimesPopulateInvoked + 4000;
      expectedMinValue = expectedMaxValue + 1;
      expectedMaxValue = expectedMaxValue + 4000000;

      executorService.shutdown();
    }

  }

  private SequenceImplOpt getSeqImpl() throws SequenceGenerationException {
    return new SequenceImplOpt(SEQ_NAME, dataSource);
  }
}