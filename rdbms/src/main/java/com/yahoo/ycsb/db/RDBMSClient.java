package com.yahoo.ycsb.db;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

public final class RDBMSClient extends DB {
  
  public static final String CONNECTION_URL_PROP = "db.url";
  public static final String AUTOCOMMIT_PROP = "db.autocommit";  
  public static final String INSERT_BATCH_SIZE_PROP = "db.batchSize";
  public static final String CONNECTION_USER_PROP = "db.user";
  
  public static final String CONNECTION_PASSWD_PROP = "db.passwd";
  public static final String QUERY_SQL_PROP = "db.querySql";
  
  private static final String DEF_BATCH_SIZE = "1000";

  private static final String YCSB_KEY = "y_id";
  
  private static final String INSERT_SQL = "insert into %s (%s) values (%s)";
  
  private static final String UPDATE_SQL = "update %s set %s where %s";
  
  private static final String READ_SQL = "select * from %s where %s";
  
  private Connection conn;
  private PreparedStatement insertStmt, readStmt, deleteStmt, selectStmt;
  private final Map<String, PreparedStatement> updateStmt = new HashMap<>();
  private int batchSize;
  
  private int insertCt = 1;
  private boolean debug = false;
  
  private static final MetricRegistry reg = new MetricRegistry();
  private static ScheduledReporter metricReporter;
  
  private static Histogram resSizeStat;

  private final Function<ByteIterator, Object> xFormer = new Function<ByteIterator, Object>() {
    @Override
    public Object apply(ByteIterator input) {
      return input.toString();
    }
  };
  

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return read(table, key, fields, Maps.transformValues(result, xFormer));
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return update(table, key, Maps.transformValues(values, xFormer));
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return insert(table, key, Maps.transformValues(values, xFormer));
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String url = props.getProperty(CONNECTION_URL_PROP);
    boolean autoCommit = Boolean.valueOf(props.getProperty(AUTOCOMMIT_PROP, "true"));
    batchSize = Integer.valueOf(props.getProperty(INSERT_BATCH_SIZE_PROP, DEF_BATCH_SIZE));
    String user = props.getProperty(CONNECTION_USER_PROP, "");
    String pwd = props.getProperty(CONNECTION_PASSWD_PROP, "");
    debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

    try {
        conn = DriverManager.getConnection(url, user, pwd);
        conn.setAutoCommit(autoCommit);
        String querySql = getProperties().getProperty(QUERY_SQL_PROP);
        
        if (querySql != null) {
          selectStmt = conn.prepareStatement(querySql);

          synchronized (getClass()) {
            if (resSizeStat == null) {
              resSizeStat = reg.histogram("ResultSetSize");
              metricReporter = ConsoleReporter.forRegistry(reg).build();
              metricReporter.start(10, TimeUnit.SECONDS);
            }
          }
        }
        
    } catch (SQLException e) {
      System.err.println("Error in database operation: " + e);
      throw new DBException(e);
    } 
    
  }

  @Override
  public void cleanup() throws DBException {
    try {
      conn.close();
    } catch (SQLException e){
      throw new DBException(e);
    }

    synchronized (getClass()) {
      if (metricReporter != null) {
        metricReporter.report();
        metricReporter.close();
        metricReporter = null;
      }
    }
  }

  @Override
  public Status query(String table, Set<String> fields, Collection<Map<String, Object>> result, QueryConstraint... criteria) {
    Arrays.sort(criteria, new Comparator<QueryConstraint>() {

      @Override
      public int compare(QueryConstraint o1, QueryConstraint o2) {
        return o1.fieldName.compareTo(o2.fieldName);
      }
      
    });
    int pos = 1;
    for (QueryConstraint c : criteria) {
      Object o = c.fieldValue;
      
      try {
        selectStmt.setObject(pos++, maybeConvDate(o));
      } catch (SQLException e) { 
        throw new RuntimeException(e);
      }
      
    }
    
    if (debug) {
      System.out.println("Executing: " + selectStmt);
    }

    try (ResultSet rs = selectStmt.executeQuery()){
      int rowCt = 0;
      while (rs.next()) {
          Map<String, Object> res = new HashMap<>();
          getNextRow(rs, res, fields);
          result.add(res);
          rowCt++;
      }

      if (debug) {
        System.out.println(rowCt + " rows fetched.");
      }
      resSizeStat.update(rowCt);
      
      return Status.OK;
        
      } catch (SQLException e) {
        e.printStackTrace(System.err);
        return Status.ERROR;
      }
  }

  @Override
  public Status read(String table, Object key, Set<String> fields, Map<String, Object> result) {
    try {
      Map<String, Object> pk;
      if (key instanceof Map<?, ?>) {
        pk = (Map<String, Object>)key;
      } else {
        pk = Collections.singletonMap(YCSB_KEY, key);
      }
        
      if (readStmt == null) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry: pk.entrySet()) {
          sb.append(entry.getKey());
          sb.append("= ? and ");
        }
        sb.delete(sb.lastIndexOf(" and "), sb.length());
        String readSql = String.format(READ_SQL, table, sb.toString());
        readStmt = conn.prepareStatement(readSql);
      }
      
      int idx = 1;
      for (Map.Entry<String, Object> entry: pk.entrySet()) {
        readStmt.setObject(idx++, maybeConvDate(entry.getValue()));
      }
      
      if (debug) {
        System.out.println(readStmt.toString());
      }

      ResultSet rs = readStmt.executeQuery();

      if (!rs.next()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      getNextRow(rs, result, fields);

      return Status.OK;

    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, Object key, Map<String, Object> values) {
    try {
      Map<String, Object> pk;
      if (key instanceof Map<?, ?>) {
        pk = (Map<String, Object>)key;
      } else {
        pk = Collections.singletonMap(YCSB_KEY, key);
      }
        
      StringBuilder valSb = new StringBuilder();
      
      for (Map.Entry<String, Object> entry: values.entrySet()) {
        valSb.append(entry.getKey());
        valSb.append(" = ?,");
      }
      valSb.deleteCharAt(valSb.length() - 1);
      
      
      StringBuilder keySb = new StringBuilder();
      for (Map.Entry<String, Object> entry: pk.entrySet()) {
        keySb.append(entry.getKey());
        keySb.append("= ? and ");
      }
      keySb.delete(keySb.lastIndexOf(" and "), keySb.length());
      String updSql = String.format(UPDATE_SQL, table, valSb.toString(), keySb.toString());
      
      PreparedStatement updStmt = updateStmt.get(updSql);
      if (updStmt == null) {
        updStmt = conn.prepareStatement(updSql);
        updateStmt.put(updSql, updStmt);
      }
      if (debug) System.out.println(updStmt);
      
      int idx = 1;
      for (Object value: values.values()) {
        updStmt.setObject(idx++, maybeConvDate(value));
      }
    
      for (Object value: pk.values()) {
        updStmt.setObject(idx++, maybeConvDate(value));
      }
      
      int rowCt = updStmt.executeUpdate();
      if (rowCt != 1) return Status.UNEXPECTED_STATE;
      return Status.OK;
    } catch (SQLException e) {
      e.printStackTrace();
      System.out.println("Error updating row with key: " + key);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, Object key, Map<String, Object> values) {
    try {
      if (insertStmt == null) {
        StringBuilder sb1 = new StringBuilder(), sb2 = new StringBuilder();
        if (key != null) {
          sb1.append(YCSB_KEY).append(",");
          sb2.append("?,");
        }
        
        for (String colName: values.keySet()) {
          sb1.append(colName).append(",");
          sb2.append("?,");
        }
        sb1.deleteCharAt(sb1.length() - 1);
        sb2.deleteCharAt(sb2.length() - 1);
        
        String insSql = String.format(INSERT_SQL, table, sb1.toString(), sb2.toString());
        insertStmt = conn.prepareStatement(insSql);
      }
      int index = 1;
      int result[] = null;
      if (key != null) {
        insertStmt.setObject(index++, key);
      }
      for (Object o : values.values()) {
        insertStmt.setObject(index++, maybeConvDate(o));
      }

      insertStmt.addBatch();
      
      if (insertCt++ % batchSize == 0) {
        result = insertStmt.executeBatch();
        for (int r: result) {
          if (r == Statement.EXECUTE_FAILED) {
            return Status.UNEXPECTED_STATE;
          }
        }
      }
      return Status.OK;
    } catch (SQLException e) {
      System.err.println("Error in processing insert to table: " + table + e);
      if (e instanceof BatchUpdateException) {
        if (debug) {
          BatchUpdateException be = (BatchUpdateException)e;
          be.getNextException().printStackTrace();
        }
        return Status.OK;
      }
      return Status.ERROR;
    }
      
  }

  private Object maybeConvDate(Object value) {
    if (value instanceof Date) {
      value = new java.sql.Date(((Date) value).getTime());
    }
    return value;
  }
  
  private void getNextRow(ResultSet rs, Map<String, Object> result, Set<String> includeFields) throws SQLException {

    ResultSetMetaData rsMeta = rs.getMetaData();
    for (int i = 0; i < rsMeta.getColumnCount(); i++) {
      String colName = rsMeta.getColumnName(i + 1);
      if (includeFields != null && !includeFields.contains(colName)) continue; 

      result.put(colName, rs.getObject(colName));
    }
  }
  
}
