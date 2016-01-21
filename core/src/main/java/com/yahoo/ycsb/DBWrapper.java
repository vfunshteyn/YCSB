/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB
{
  private DB _db;
  private Measurements _measurements;

  private boolean reportLatencyForEachError = false;
  private HashSet<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY =
      "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT =
      "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY =
      "latencytrackederrors";

  public DBWrapper(DB db)
  {
    _db=db;
    _measurements=Measurements.getMeasurements();
  }

  /**
   * Set the properties for this DB.
   */
  @Override
  public void setProperties(Properties p)
  {
    _db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  @Override
  public Properties getProperties()
  {
    return _db.getProperties();
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException
  {
    _db.init();

    this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
        getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
            REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

    if (!reportLatencyForEachError) {
      String latencyTrackedErrors = getProperties().getProperty(
          LATENCY_TRACKED_ERRORS_PROPERTY, null);
      if (latencyTrackedErrors != null) {
        this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
            latencyTrackedErrors.split(",")));
      }
    }

    System.err.println("DBWrapper: report latency for each error is " +
        this.reportLatencyForEachError + " and specific error codes to track" +
        " for latency are: " + this.latencyTrackedErrors.toString());
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    _db.cleanup();
    long en=System.nanoTime();
    measure(Operation.CLEANUP, Status.OK, ist, st, en);
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String,ByteIterator> result)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.read(table,key,fields,result);
    long en=System.nanoTime();
    Operation op = Operation.READ;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.scan(table,startkey,recordcount,fields,result);
    long en=System.nanoTime();
    Operation op = Operation.SCAN;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  private void measure(Operation op, Status result, long intendedStartTimeNanos,
      long startTimeNanos, long endTimeNanos) {
    String measurementName = op.toString();
    if (result != Status.OK) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    _measurements.measure(measurementName,
        (int)((endTimeNanos-startTimeNanos)/1000));
    _measurements.measureIntended(measurementName,
        (int)((endTimeNanos-intendedStartTimeNanos)/1000));
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key,
      HashMap<String,ByteIterator> values)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.update(table,key,values);
    long en=System.nanoTime();
    Operation op = Operation.UPDATE;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key,
      HashMap<String,ByteIterator> values)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.insert(table,key,values);
    long en=System.nanoTime();
    Operation op = Operation.INSERT;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  @Override
  public Status delete(String table, String key)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.delete(table,key);
    long en=System.nanoTime();
    Operation op = Operation.DELETE;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  @Override
  public Status query(String table, Set<String> fields, Collection<Map<String, Object>> result, QueryConstraint... criteria) {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.query(table, fields, result, criteria);
    long en=System.nanoTime();
    Operation op = Operation.QUERY;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  @Override
  public Status read(String table, Object key, Set<String> fields, Map<String, Object> result) {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.read(table,key,fields,result);
    long en=System.nanoTime();
    Operation op = Operation.READ;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  @Override
  public Status update(String table, Object key, Map<String, Object> values) {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.update(table, key, values);
    long en=System.nanoTime();
    Operation op = Operation.UPDATE;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }

  @Override
  public Status insert(String table, Object key, Map<String, Object> values) {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    Status res=_db.insert(table,key,values);
    long en=System.nanoTime();
    Operation op = Operation.INSERT;
    measure(op, res, ist, st, en);
    _measurements.reportStatus(op.toString(), res);
    return res;
  }
  
  
}
