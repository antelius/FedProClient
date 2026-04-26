package se.pitch.oss.fedpro.client_common;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class HandleHandleCache<K, T> {
   private final ConcurrentHashMap<K,ConcurrentHashMap<String, CompletableFuture<T>>> _cache = new ConcurrentHashMap<>();

   /**
    * Gets the existing Future, or computes a new one atomically.
    * If the newly computed Future fails, it will automatically evict itself.
    */
   public CompletableFuture<T> cacheHandle(K key, String name, BiFunction<K, String, CompletableFuture<T>> handleProvider) {
      ConcurrentHashMap<String, CompletableFuture<T>> cache = _cache.computeIfAbsent(key, __ -> new ConcurrentHashMap<>());

      CompletableFuture<T> existing = cache.get(name);
      if (existing != null) {
         return existing;
      }

      CompletableFuture<T> result = new CompletableFuture<>();
      CompletableFuture<T> old = cache.putIfAbsent(name, result);
      if (old != null) {
         return old;
      }

      try {
         handleProvider.apply(key, name).whenComplete((value, exception) -> {
            if (exception == null) {
               result.complete(value);
            } else {
               cache.remove(name, result);
               result.completeExceptionally(exception);
            }
         });
      } catch (Throwable t) {
         cache.remove(name, result);
         result.completeExceptionally(t);
      }

      return result;
   }

   /**
    * Performs a reverse lookup to find the name (key) associated with the specified handle.
    */
   public String findHandle(K key, T handle) {
      if (handle == null) {
         return null;
      }

      Map<String, CompletableFuture<T>> cache = _cache.get(key);
      if (cache == null) {
         return null;
      }

      for (Map.Entry<String, CompletableFuture<T>> entry : cache.entrySet()) {
         CompletableFuture<T> future = entry.getValue();

         if (future.isDone() && !future.isCompletedExceptionally()) {
            if (handle.equals(future.getNow(null))) {
               return entry.getKey();
            }
         }
      }
      return null;
   }
}