package com.yahoo.ycsb.workloads;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DB.QueryConstraint;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class SearchableWorkload extends CoreWorkload {

  private static final String FIELD_PROP_PREFIX = "field";
  private static final String FIELD_PROP_TYPE_SUFFIX = "type";
  private static final String FIELD_PROP_RANGE_SUFFIX = "range";
  private static final String FIELD_PROP_QUERY_PARAM_COUNT_SUFFIX = "queryparams";
  private static final String FIELD_PROP_DIST_SUFFIX = "dist";
  
  private static final String FIELD_DIST_DEFAULT = "uniform";
  public static final String ADDITIONAL_PK_FIELDS_PROP = "pk_fields";
  
  private static final Map<String, FieldType> ftIndex = new HashMap<>();
  
  private static enum FieldType {
    INT(Integer.class), LONG(Long.class), DOUBLE(Double.class), STRING(String.class), DATE(Date.class);
    
    final Class<?> javaType; 
    
    FieldType(Class<?> c) {
      javaType = c;
      ftIndex.put(c.getSimpleName().toLowerCase(), this);
    }
    
  }
  
  static {
    FieldType.values();
  }
  
  private static final class Field<T> {
    
    private final Generator<Comparable<T>> valueGen;
    private final FieldType type;
    private final String name;
    private final int paramCt;
    
    Field(String name, FieldType type, Generator<Comparable<T>> gen, int queryParamCt) {
      this.valueGen = gen;
      this.type = type;
      this.name = name;
      this.paramCt = queryParamCt;
    }
    
    Comparable<T> nextRandomValue() {
      return valueGen.nextValue();
    }
    
  }

  private final List<Field<?>> fields = new ArrayList<>();
  private final Map<String, Field<?>> pkFields = new HashMap<>();
    
  private static FieldType getFieldType(String type) {
    return ftIndex.get(type.toLowerCase()); 
  }

  private static class Builder<T> {
    private Generator<Comparable<T>> valueGen;
    private FieldType type;
    private String name;
    private int queryParams = 0;
    private String min;
    private String max;
    private String distType = FIELD_DIST_DEFAULT;
    
    Field<T> build() {
      return new Field<T>(name, type, valueGen, queryParams);
    }
    
  }

  private static String getRandomAlphaSeq(int len, Random rand) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append((char)(rand.nextInt(26) + (rand.nextBoolean() ? 'A' : 'a')));
    }
    return sb.toString();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    
    Map<String, Builder<?>> fieldMap = new HashMap<>();
    fieldnames.clear();
    
    for (String prop: p.stringPropertyNames()) {
      if (!prop.startsWith(FIELD_PROP_PREFIX)) continue;
      
      try {
        String desc = prop.substring(prop.indexOf(".") + 1);
        String fieldName = desc.substring(0, desc.indexOf("."));
        
        Builder<?> b = fieldMap.get(fieldName);
        if (b == null) {
          b = new Builder<>();
          b.name = fieldName;
          fieldMap.put(fieldName, b);
        }
        
        String suffix = desc.substring(desc.lastIndexOf(".") + 1);
        
        String value = p.getProperty(prop);
        if (value != null) value = value.trim();
                
        switch (suffix) {
          case FIELD_PROP_RANGE_SUFFIX: 
            String[] parts = value.split("\\s*,\\s*", 2);
            b.min = parts[0];
            b.max = parts[1];
            break;
          case FIELD_PROP_TYPE_SUFFIX:
            b.type = getFieldType(value);
            break;
          case FIELD_PROP_QUERY_PARAM_COUNT_SUFFIX:
            b.queryParams = value.isEmpty() ? 0 : Integer.valueOf(value);
            break;
          case FIELD_PROP_DIST_SUFFIX:
            b.distType = value.isEmpty() ? FIELD_DIST_DEFAULT : value;
            break;
        }
       
      } catch (IndexOutOfBoundsException e) {
        System.err.println(e);
        continue;
      }
    }
    
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    
    for (Builder<?> fb: fieldMap.values()) {
      switch (fb.type) {
        case STRING:
          final NumberGenerator lenGen = getNumericGenerator(fb.distType, Integer.valueOf(fb.min), Integer.valueOf(fb.max), p);
          Builder<String> sb = (Builder<String>)fb;
          sb.valueGen = new Generator<Comparable<String>>() {
            private String last = null;
  
            @Override
            public String nextValue() {
              last = getRandomAlphaSeq(lenGen.nextValue().intValue(), Utils.random());
              return last;
            }
  
            @Override
            public String lastValue() {
              return last;
            }
          };
          break;
        case DATE:
          try {
            Date start = df.parse(fb.min);
            Date end = df.parse(fb.max);
            final NumberGenerator dayGen = getNumericGenerator(fb.distType, 
                TimeUnit.MILLISECONDS.toDays(start.getTime()), TimeUnit.MILLISECONDS.toDays(end.getTime()), p);
            
            Builder<Integer> db = (Builder<Integer>)fb;
            db.valueGen = new Generator<Comparable<Integer>>() {
              @Override
              public Integer nextValue() {
                return dayGen.nextValue().intValue();
              }

              @Override
              public Integer lastValue() {
                return dayGen.lastValue().intValue();
              }
              
            };
          } catch (ParseException e) {
            throw new WorkloadException(e);
          }
          break;
        case DOUBLE: {
          final Builder<Double> db = (Builder<Double>)fb;
          Double min = fb.min == null ? 0 : Double.valueOf(fb.min);
          Double max = fb.max == null ? 1000.0 : Double.valueOf(fb.max); // XXX
          
          final NumberGenerator gen = getNumericGenerator(fb.distType, min, max, p);
          db.valueGen = new Generator<Comparable<Double>>() {

            @Override
            public Comparable<Double> nextValue() {
              return gen.nextValue().doubleValue();
            }

            @Override
            public Comparable<Double> lastValue() {
              return gen.lastValue().doubleValue();
            }
          };
          break;
        }
        case INT: {
          Builder<Integer> db = (Builder<Integer>)fb;
          int min = fb.min == null ? 1 : Integer.valueOf(fb.min);
          int max = fb.max == null ? Integer.MAX_VALUE : Integer.valueOf(fb.max);
          final NumberGenerator gen = getNumericGenerator(fb.distType, min, max, p);
          db.valueGen = new Generator<Comparable<Integer>>() {

            @Override
            public Comparable<Integer> nextValue() {
              return gen.nextValue().intValue();
            }

            @Override
            public Comparable<Integer> lastValue() {
              return gen.lastValue().intValue();
            }
          };
          
          break;
        }
        case LONG: {
          Builder<Long> db = (Builder<Long>)fb;
          long min = fb.min == null ? 1 : Long.valueOf(fb.min);
          long max = fb.max == null ? Long.MAX_VALUE : Long.valueOf(fb.max);
          final NumberGenerator gen = getNumericGenerator(fb.distType, min, max, p);
          db.valueGen = new Generator<Comparable<Long>>() {

            @Override
            public Comparable<Long> nextValue() {
              return gen.nextValue().longValue();
            }

            @Override
            public Comparable<Long> lastValue() {
              return gen.lastValue().longValue();
            }
          };
          break;
        }
        default:
          throw new WorkloadException("Unsupported field type: " + fb.type);
      };
      
      Field<?> f = fb.build();
      fields.add(f);
      fieldnames.add(f.name);
    }
    
    String pkCols = p.getProperty(ADDITIONAL_PK_FIELDS_PROP, "").trim();
    if (!pkCols.isEmpty()) {
      String[] parts = pkCols.split("\\s*,\\s*");
      for (String s: parts) {
        Builder<?> b = fieldMap.get(s);
        if (b == null) {
          System.err.println("Ignoring undefined PK column: " + s);
        }
        pkFields.put(s, b.build());
      }
    }
    
    fieldcount = fields.size();
    fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    Map<String, Object> values = buildValues();

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, keynum, values);
      if (status == Status.OK) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return (status == Status.OK);  }

  @Override
  public void doTransactionRead(DB db) {
    // choose a random key
    int keynum = nextKeynum();

    Set<String> fields;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = Collections.singleton(fieldname);
    } else {
      fields = new HashSet<>(fieldnames);
    }

    Map<String, Object> cells = new HashMap<>();
    db.read(table, keynum, fields, cells);

  }

  @Override
  public void doTransactionUpdate(DB db) {
    // choose a random key
    int keynum = nextKeynum();

    Map<String, Object> values = new HashMap<>();

    if (writeallfields) {
      // new data for all the fields
      values.putAll(buildValues());
    } else {
      // update a random field
      Field<?> f;
      do {
        f = fields.get(fieldchooser.nextValue().intValue());
      } while (pkFields.containsKey(f.name));
      
      values.put(f.name, buildValue(f));
      for (Field<?> pk: pkFields.values()) {
        values.put(pk.name, buildValue(pk));
      }
    }

    db.update(table, keynum, values);
  }

  @Override
  public void doTransactionInsert(DB db) {
    // choose the next key
    int keynum = transactioninsertkeysequence.nextValue();

    try {
      Map<String, Object> values = buildValues();
      db.insert(table, keynum, values);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  @Override
  public void doTransactionQuery(DB db) {
    HashSet<String> fields = new HashSet<>();

    if (!readallfields) {
      // read a random field
      fields.add(fieldnames.get(fieldchooser.nextValue().intValue()));

    } else {
      fields.addAll(fieldnames);
    }

    Collection<Map<String, Object>> cells = new LinkedList<>();
    Collection<QueryConstraint> criteria = new ArrayList<>();
    
    for (Field<?> f: this.fields) {
      List<QueryConstraint> fieldParams = new ArrayList<>();
      List<Object> fieldValues = new ArrayList<>();
      for (int i = 0; i < f.paramCt; i++) {
        Object o = f.nextRandomValue();
        fieldValues.add(o);
      }
      if (fieldValues.isEmpty()) continue;
      if (fieldValues.get(0) instanceof Comparable<?>) {
        Collections.sort(fieldValues, new Comparator<Object>() {
  
          @SuppressWarnings("unchecked")
          @Override
          public int compare(Object o1, Object o2) {
            Comparable<Object> c1 = (Comparable<Object>)o1;
            return c1.compareTo(o2);
          }
          
        });
      }
      for (int i = 0; i < f.paramCt; i++) {
        String cName = f.paramCt > 1 ? f.name + (i + 1) : f.name;
        fieldParams.add(new QueryConstraint(cName, fieldValues.get(i)));
      }
      criteria.addAll(fieldParams);
    }

    db.query(table, fields, cells, criteria.toArray(new QueryConstraint[criteria.size()])); 
  }
  
  public boolean doDelete(DB db) {
    int keynum = keysequence.nextValue().intValue();
    return Status.OK == db.delete(table, String.valueOf(keynum));
  }

  private Map<String, Object> buildValues() {
    Map<String, Object> res = new HashMap<>();
    for (Field<?> f: fields) {
      res.put(f.name, buildValue(f));
    }
    return res;
  }

  private Object buildValue(Field<?> f) {
    return f.nextRandomValue();
  }

}
