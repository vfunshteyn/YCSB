package com.yahoo.ycsb.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Func1;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * 
 * @author Vitaliy Funshteyn
 */
public class CBDriver extends DB {

  public static final String HOST_LIST_PROPERTY = "couchbase.servers";
  public static final String HOST_LIST_DEFAULT  = "localhost";

  public static final String BUCKET_PROPERTY = "couchbase.bucket";
  public static final String BUCKET_DEFAULT  = "default";
  
  public static final String PASSWORD_PROPERTY = "couchbase.password";
  public static final String PASSWORD_DEFAULT = "";
  
  public static final String PERSIST_PROPERTY = "couchbase.persistTo";
  public static final String PERSIST_DEFAULT = "0";
  
  public static final String REPLICATE_PROPERTY = "couchbase.replicateTo";
  public static final String REPLICATE_DEFAULT = "0";
  
  public static final String QUERY_STRING_PROPERTY = "couchbase.query";
  
  public static final String OP_RETRY_PROPERTY = "couchbase.retry";
  public static final String OP_RETRY_DEFAULT = "3";
  
  public static final String IGNORE_BULK_OP_ERROR_PROPERTY = "couchbase.ignoreErrors";
  public static final String IGNORE_BULK_OP_ERRORS_DEFAULT = "true";
  
  private Cluster cbCluster;
  private Bucket cbBucket; 

  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  
  private String queryString;
  
  private int retryCt;
  
  private boolean ignoreErrors;
  
  private final BufferedDocMutator batcher = new BufferedDocMutator();
  
  private final N1qlParams queryParams = N1qlParams.build().adhoc(false);
  
  private static final Logger log = LoggerFactory.getLogger(CBDriver.class);
  
  @Override
  public Status read(String table, Object key, Set<String> fields, Map<String, Object> result) {

    String formattedKey = formatKey(table, key);
    try {
      JsonDocument doc = cbBucket.get(formattedKey);

      if (doc == null) {
        return Status.ERROR;
      }
      
      JsonObject json = doc.content();
      result.putAll(populateFields(fields, json));

      return Status.OK;
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Could not read value for key " + formattedKey, e);
      }
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, Object key, Map<String, Object> values) {
    JsonObject content = JsonObject.create();
    for (Map.Entry<String, Object> entry: values.entrySet()) {
      content.put(entry.getKey(), entry.getValue());
    }
    
    String formattedKey = formatKey(table, key);
    JsonDocument doc = JsonDocument.create(formattedKey, content);
    try {
      cbBucket.replace(doc, persistTo, replicateTo);
      return Status.OK;
    } catch (Exception e) {
      log.error("Could not update value for key " + formattedKey, e);
      return Status.ERROR;
    }
    
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    JsonObject content = JsonObject.create();
    for (Map.Entry<String, ByteIterator> entry: values.entrySet()) {
      content.put(entry.getKey(), entry.getValue().toString());
    }
    
    String formattedKey = formatKey(table, key);
    JsonDocument doc = JsonDocument.create(formattedKey, content);
    try {
      cbBucket.insert(doc, persistTo, replicateTo);
      return Status.OK;
    } catch (Exception e) {
      log.error("Could not insert value for key " + formattedKey, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, Object key, Map<String, Object> values) {
    JsonObject content = JsonObject.create();
    for (Map.Entry<String, Object> entry: values.entrySet()) {
      content.put(entry.getKey(), entry.getValue());
    }
    
    final String formattedKey = formatKey(table, key);
    JsonDocument doc = JsonDocument.create(formattedKey, content);
    try {
      batcher.addMutation(maybeHideErrors(cbBucket.async().insert(doc, persistTo, replicateTo).timeout(cbBucket.environment().kvTimeout(),
          TimeUnit.MILLISECONDS).retry(retryCt), formattedKey));
      return Status.OK;
    } catch (Exception e) {
      log.error("Could not insert value for key " + formattedKey, e);
      return Status.ERROR;
    }
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
      
    String[] servers = tokenize(props.getProperty(HOST_LIST_PROPERTY, HOST_LIST_DEFAULT));
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment
        .builder()
        .mutationTokensEnabled(true)
        .build();
    cbCluster = CouchbaseCluster.create(env, servers);

    String bucket = props.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    String password = props.getProperty(PASSWORD_PROPERTY, PASSWORD_DEFAULT);
    cbBucket = cbCluster.openBucket(bucket, password);
    
    persistTo = parsePersistTo(props.getProperty(PERSIST_PROPERTY, PERSIST_DEFAULT));
    replicateTo = parseReplicateTo(props.getProperty(REPLICATE_PROPERTY, REPLICATE_DEFAULT));
    
    queryString = props.getProperty(QUERY_STRING_PROPERTY);
    
    retryCt = Integer.parseInt(props.getProperty(OP_RETRY_PROPERTY, OP_RETRY_DEFAULT));
    
    ignoreErrors = Boolean.valueOf(props.getProperty(IGNORE_BULK_OP_ERROR_PROPERTY, IGNORE_BULK_OP_ERRORS_DEFAULT));
  }

  @Override
  public Status query(String table, Set<String> fields, Collection<Map<String, Object>> result, QueryConstraint... criteria) {
    if (queryString == null) {
      log.info("Query string not configured");
      return Status.UNEXPECTED_STATE;
    }
    
    JsonObject params = JsonObject.create();
    for (QueryConstraint c: criteria) {
      params.put(c.fieldName, c.fieldValue);
    }
    
    try {
      ParameterizedN1qlQuery query = N1qlQuery.parameterized(queryString, params, queryParams);
      N1qlQueryResult rs = cbBucket.query(query);
      System.out.println("Row count=" + rs.allRows().size());

      for (N1qlQueryRow row: rs.allRows()) {
        JsonObject json = row.value();
        result.add(populateFields(fields, json));
      }
    } catch (Exception e) {
      log.error("Coud not execute query", e);
      return Status.ERROR;
    }
    return Status.OK;
  }
  
  private static Map<String, Object> populateFields(Set<String> fields, JsonObject json) throws Exception {
    Map<String, Object> res = new HashMap<>();
    for (String field : json.getNames()) {
      if (fields != null && !fields.contains(field)) continue; 
      Object value = json.get(field);
      if (value == null) continue;
      res.put(field, value);
    }
    return res;
  }

  @Override
  public void cleanup() throws DBException {
    batcher.flush();
    cbBucket.close();
    cbCluster.disconnect();
  }

  private static String[] tokenize(String arg) {
    return arg.trim().split("\\s*[,;]\\s*");
  }
  
  /**
   * Parse the replicate property into the correct enum.
   *
   * @param property the stringified property value.
   * @throws DBException if parsing the property did fail.
   * @return the correct enum.
   */
  private static ReplicateTo parseReplicateTo(String value) throws DBException {

    switch (value) {
      case "0": return ReplicateTo.NONE;
      case "1": return ReplicateTo.ONE;
      case "2": return ReplicateTo.TWO;
      case "3": return ReplicateTo.THREE;
      default:
        throw new DBException(REPLICATE_PROPERTY + " must be between 0 and 3");
    }
  }

  /**
   * Parse the persist property into the correct enum.
   *
   * @param property the stringified property value.
   * @throws DBException if parsing the property did fail.
   * @return the correct enum.
   */
  private static PersistTo parsePersistTo(String value) throws DBException {

    switch (value) {
      case "0": return PersistTo.NONE;
      case "1": return PersistTo.ONE;
      case "2": return PersistTo.TWO;
      case "3": return PersistTo.THREE;
      case "4": return PersistTo.FOUR;
      case "self": return PersistTo.MASTER;
      default:
        throw new DBException(PERSIST_PROPERTY + " must be between 0 and 4");
    }
  }
 
  /**
   * Prefix the key with the given prefix, to establish a unique namespace.
   *
   * @param prefix the prefix to use.
   * @param key the actual key.
   * @return the formatted and prefixed key.
   */
  private static String formatKey(final String prefix, final Object key) {
    return key.toString();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null; // XXX - delegate
  }

  @Override
  public Status delete(String table, Object key) {
    String formattedKey = formatKey(table, key);
    JsonDocument doc = JsonDocument.create(formattedKey);
    try {
      batcher.addMutation(maybeHideErrors(cbBucket.async().remove(doc, persistTo, replicateTo).timeout(cbBucket.environment().kvTimeout(),
          TimeUnit.MILLISECONDS).retry(retryCt), formattedKey));
      return Status.OK;
    } catch (Exception e) {
      log.error("Could not delete document for key " + formattedKey, e);
      return Status.ERROR;
    }
  }

  private Observable<JsonDocument> maybeHideErrors(Observable<JsonDocument> o, final String key) {
    return ignoreErrors ?  o.onErrorResumeNext(new Func1<Throwable, Observable<? extends JsonDocument>>() {

        @Override
        public Observable<? extends JsonDocument> call(Throwable t) {
          log.warn("Failed to mutate doc with id {} due to {}", key, t.getMessage());
          return Observable.<JsonDocument>empty();
        }
      }) : o;
    }
  
}
