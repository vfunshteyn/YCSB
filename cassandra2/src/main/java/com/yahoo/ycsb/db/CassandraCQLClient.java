/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClient extends DB {

  private static Cluster cluster = null;
  private static Session session = null;

  private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
  
  private static String queryString;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "cassandra.username";
  public static final String PASSWORD_PROPERTY = "cassandra.password";
  public static final String QUERY_PROPERTY = "cassandra.query";

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY =
      "cassandra.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

  public static final String MAX_CONNECTIONS_PROPERTY =
      "cassandra.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY =
      "cassandra.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY =
      "cassandra.readtimeoutmillis";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;

  private final Function<ByteIterator, Object> xFormer = new Function<ByteIterator, Object>() {
    @Override
    public Object apply(ByteIterator input) {
      return input.toString();
    }
  };
  
  private static PreparedStatement queryStmt;
  
  private static final Logger log = LoggerFactory.getLogger(CassandraCQLClient.class);
  
  private static final Map<String, int[]> queryCols = new HashMap<>();
  
  private static ScheduledReporter metricReporter;
  
  private static Histogram resSizeStat;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // cluster/session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the cluster has already been initialized
      if (cluster != null) {
        return;
      }
      
      try {

        debug =
            Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

        String host = getProperties().getProperty(HOSTS_PROPERTY).trim();
        if (host == null) {
          throw new DBException(String.format(
              "Required property \"%s\" missing for CassandraCQLClient",
              HOSTS_PROPERTY));
        }
        String[] hosts = host.split("\\s*,\\s*");
        String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY,
            KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = ConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        
        queryString = getProperties().getProperty(QUERY_PROPERTY);

        if ((username != null) && !username.isEmpty()) {
          cluster = Cluster.builder().withCredentials(username, password)
              .withPort(Integer.valueOf(port)).addContactPoints(hosts).build();
        } else {
          cluster = Cluster.builder().withPort(Integer.valueOf(port))
              .addContactPoints(hosts).build();
        }

        String maxConnections = getProperties().getProperty(
            MAX_CONNECTIONS_PROPERTY);
        if (maxConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setMaxConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(maxConnections));
        }

        String coreConnections = getProperties().getProperty(
            CORE_CONNECTIONS_PROPERTY);
        if (coreConnections != null) {
          cluster.getConfiguration().getPoolingOptions()
              .setCoreConnectionsPerHost(HostDistance.LOCAL,
              Integer.valueOf(coreConnections));
        }

        String connectTimoutMillis = getProperties().getProperty(
            CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setConnectTimeoutMillis(Integer.valueOf(connectTimoutMillis));
        }

        String readTimoutMillis = getProperties().getProperty(
            READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimoutMillis != null) {
          cluster.getConfiguration().getSocketOptions()
              .setReadTimeoutMillis(Integer.valueOf(readTimoutMillis));
          System.out.println("Using read timeout value of " +  cluster.getConfiguration().getSocketOptions().getReadTimeoutMillis());
        }

        Metadata metadata = cluster.getMetadata();
        System.err.printf("Connected to cluster: %s\n",
            metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
          System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
              discoveredHost.getDatacenter(), discoveredHost.getAddress(),
              discoveredHost.getRack());
        }

        cluster.getConfiguration().getQueryOptions().setFetchSize(500);
        session = cluster.connect(keyspace);
        
        if (queryString != null) {
          queryStmt = session.prepare(queryString);
          
          for (int i = 0; i < queryStmt.getVariables().size(); i++) {
              // Be optimistic, 99% of the time, previous will be null.
              String colName = queryStmt.getVariables().getName(i).toLowerCase();
              int[] previous = queryCols.put(colName, new int[]{ i });
              if (previous != null) {
                  int[] indexes = new int[previous.length + 1];
                  System.arraycopy(previous, 0, indexes, 0, previous.length);
                  indexes[indexes.length - 1] = i;
                  queryCols.put(colName, indexes);
              }
          }
          
          MetricRegistry reg = new MetricRegistry();
          resSizeStat = reg.histogram("ResultSetSize");
          metricReporter = ConsoleReporter.forRegistry(reg).build();
          metricReporter.start(10, TimeUnit.SECONDS);
        }

      } catch (Exception e) {
        throw new DBException(e);
      }
      
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        session.close();
        cluster.close();
        cluster = null;
        session = null;
        
        if (metricReporter != null) {
          metricReporter.report();
          metricReporter.close();
        }
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(
            String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(table).where(QueryBuilder.eq(YCSB_KEY, key))
          .limit(1);
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      ResultSet rs = session.execute(stmt);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      Row row = rs.one();
      ColumnDefinitions cd = row.getColumnDefinitions();

      for (ColumnDefinitions.Definition def : cd) {
        ByteBuffer val = row.getBytesUnsafe(def.getName());
        if (val != null) {
          result.put(def.getName(), new ByteArrayByteIterator(val.array()));
        } else {
          result.put(def.getName(), null);
        }
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * Cassandra CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      Statement stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }

      stmt = selectBuilder.from(table);

      // The statement builder is not setup right for tokens.
      // So, we need to build it manually.
      String initialStmt = stmt.toString();
      StringBuilder scanStmt = new StringBuilder();
      scanStmt.append(initialStmt.substring(0, initialStmt.length() - 1));
      scanStmt.append(" WHERE ");
      scanStmt.append(QueryBuilder.token(YCSB_KEY));
      scanStmt.append(" >= ");
      scanStmt.append("token('");
      scanStmt.append(startkey);
      scanStmt.append("')");
      scanStmt.append(" LIMIT ");
      scanStmt.append(recordcount);

      stmt = new SimpleStatement(scanStmt.toString());
      stmt.setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      ResultSet rs = session.execute(stmt);

      HashMap<String, ByteIterator> tuple;
      while (!rs.isExhausted()) {
        Row row = rs.one();
        tuple = new HashMap<String, ByteIterator>();

        ColumnDefinitions cd = row.getColumnDefinitions();

        for (ColumnDefinitions.Definition def : cd) {
          ByteBuffer val = row.getBytesUnsafe(def.getName());
          if (val != null) {
            tuple.put(def.getName(), new ByteArrayByteIterator(val.array()));
          } else {
            tuple.put(def.getName(), null);
          }
        }

        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error scanning with startkey: " + startkey);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    return update(table, key, Maps.transformValues(values, xFormer));
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    
    return insert(table, key, Maps.transformValues(values, xFormer));
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      Statement stmt;

      stmt = QueryBuilder.delete().from(table)
          .where(QueryBuilder.eq(YCSB_KEY, key));
      stmt.setConsistencyLevel(writeConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      session.execute(stmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error deleting key: " + key);
    }

    return Status.ERROR;
  }

  @Override
  public Status query(String table, Set<String> fields, Collection<Map<String, Object>> result, 
      QueryConstraint... criteria) {
    if (queryString == null) {
      log.info("Query string not configured");
      return Status.UNEXPECTED_STATE;
    }
    
    try {
      BoundStatement bs = new BoundStatement(queryStmt);
      Map<String, Integer> colCt = new HashMap<>();
      
      for (QueryConstraint c : criteria) {
        Object o = c.fieldValue;
        
        String colName = c.fieldName.split("\\d")[0];
        int[] indices = queryCols.get(colName);
        if (indices == null) continue;

        Integer idx = colCt.get(colName);
        if (idx == null) {
          idx = 0;
        }
        int pos = indices[idx];
        
        switch (o.getClass().getSimpleName()) {
          case "Integer" : 
            bs.setInt(pos, (Integer)o);
            break;
          case "Byte" : 
            bs.setByte(pos, (Byte)o);
            break;
          case "Short" : 
            bs.setShort(pos, (Short)o);
            break;
          case "Long" : 
            bs.setLong(pos, (Long)o);
            break;
          case "String" : 
            bs.setString(pos, (String)o);
            break;
          case "Double" :
            bs.setFloat(pos, ((Double)o).floatValue());
            break;
          case "Date" :
            bs.setDate(pos, LocalDate.fromMillisSinceEpoch(((Date)o).getTime()));
            break;
          default:
            throw new IllegalArgumentException(o.toString());
        }
        colCt.put(colName, ++idx);
      }
      
      ResultSet rs = session.execute(bs);
      int rowCt = 0;
      for (Row r: rs) {
          Map<String, Object> res = new HashMap<>();
          getNextRow(r, res, fields);
          result.add(res);
          rowCt++;
      }

      resSizeStat.update(rowCt);
      
      if (debug) {
        System.out.println(rowCt + " rows fetched.");
      }
      
    } catch (Exception e) {
      log.error("Exception executing query", e);
      return Status.ERROR;
    } 
    
    return Status.OK;
  }

  @Override
  public Status read(String table, Object key, Set<String> fields, Map<String, Object> result) {
    try {
      Select stmt;
      Select.Builder selectBuilder;

      if (fields == null) {
        selectBuilder = QueryBuilder.select().all();
      } else {
        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }
      }
      stmt = selectBuilder.from(table);
      
      if (key instanceof Map<?, ?>) {
        Map<String, Object> compPk = (Map<String, Object>)key;
        for (Map.Entry<String, Object> pkField: compPk.entrySet()) {
          Object value = pkField.getValue();

          if (value != null) {
            if (value instanceof Date) {
              value = LocalDate.fromMillisSinceEpoch(((Date)value).getTime());
            }
            stmt.where(QueryBuilder.eq(pkField.getKey(), value)); 
          }
        }
      } else {
        stmt.where(QueryBuilder.eq(YCSB_KEY, key));
      }

      stmt.limit(1).setConsistencyLevel(readConsistencyLevel);

      if (debug) {
        System.out.println(stmt.toString());
      }

      ResultSet rs = session.execute(stmt);

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      // Should be only 1 row
      getNextRow(rs.one(), result, fields);

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error reading key: " + key);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, Object key, Map<String, Object> values) {
    try {
      Update updStmt = QueryBuilder.update(table);
      Set<String> pkNames;

      // Hack!
      if (key instanceof Map<?, ?>) {
        Map<String, Object> compPk = (Map<String, Object>)key;
        for (Map.Entry<String, Object> pkField: compPk.entrySet()) {
          Object value = pkField.getValue();
          if (value != null) {
            if (value instanceof Date) {
              value = LocalDate.fromMillisSinceEpoch(((Date)value).getTime());
            }
            
            updStmt.where(QueryBuilder.eq(pkField.getKey(), value)); 
          }
        }
        pkNames = new HashSet<>(compPk.keySet());
      } else {
        updStmt.where(QueryBuilder.eq(YCSB_KEY, key));
        pkNames = Collections.singleton(YCSB_KEY);
      }
      
      // Add fields
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        if (!pkNames.contains(entry.getKey()))
          updStmt.with(QueryBuilder.set(entry.getKey(), entry.getValue()));
      }

      updStmt.setConsistencyLevel(writeConsistencyLevel).enableTracing();

      if (debug) {
        System.out.println(updStmt.toString());
      }

      session.execute(updStmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }

  @Override
  public Status insert(String table, Object key, Map<String, Object> values) {
    try {
      Insert insertStmt = QueryBuilder.insertInto(table);

      // Add key if applicable
      if (key != null) insertStmt.value(YCSB_KEY, key);

      // Add fields
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof Date) {
          value = LocalDate.fromMillisSinceEpoch(((Date)value).getTime());
        }
        insertStmt.value(entry.getKey(), value);
      }

      insertStmt.setConsistencyLevel(writeConsistencyLevel).enableTracing();

      if (debug) {
        System.out.println(insertStmt.toString());
      }

      session.execute(insertStmt);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Status.ERROR;
  }
  
  private void getNextRow(Row row, Map<String, Object> result, Set<String> includeFields) {
    ColumnDefinitions cd = row.getColumnDefinitions();

    for (ColumnDefinitions.Definition def : cd) {
      if (includeFields != null && !includeFields.contains(def.getName())) continue; 

      Object val = row.getObject(def.getName());
      result.put(def.getName(), val);
    }
  }

}
