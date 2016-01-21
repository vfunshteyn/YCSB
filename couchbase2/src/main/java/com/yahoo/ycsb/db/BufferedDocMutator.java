package com.yahoo.ycsb.db;

import java.util.Arrays;

import com.couchbase.client.java.document.JsonDocument;

import rx.Observable;

final class BufferedDocMutator {
  
  public static final int DEFAULT_BUFFER_SIZE = 2048;
  
  private final Observable<JsonDocument>[] buffer;
  private int pos = 0; 
  
  BufferedDocMutator() {
    this(DEFAULT_BUFFER_SIZE);
  }

  @SuppressWarnings("unchecked")
  BufferedDocMutator(int bufSize) {
    buffer = new Observable[bufSize];
  }
  
  void addMutation(Observable<JsonDocument> op) {
    if (pos >= buffer.length) {
      flush();
    }
    buffer[pos++] = op;
  }
  
  void flush() {
    Observable.
      merge(Arrays.copyOfRange(buffer, 0, pos)).
      toBlocking().
      lastOrDefault(JsonDocument.create("dummy"));
    pos = 0;
  }
}
