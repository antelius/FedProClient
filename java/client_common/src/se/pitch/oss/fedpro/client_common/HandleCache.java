package se.pitch.oss.fedpro.client_common;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class HandleCache<T> {
   private final ConcurrentHashMap<String, CompletableFuture<T>> _cache = new ConcurrentHashMap<>();

   /**
    * Gets the existing Future, or computes a new one atomically.
    * If the newly computed Future fails, it will automatically evict itself.
    */
   public CompletableFuture<T> cacheHandle(String name, Function<String, CompletableFuture<T>> handleProvider) {
      CompletableFuture<T> existing = _cache.get(name);
      if (existing != null) {
         return existing;
      }

      CompletableFuture<T> result = new CompletableFuture<>();
      CompletableFuture<T> old = _cache.putIfAbsent(name, result);
      if (old != null) {
         return old;
      }

      try {
         handleProvider.apply(name).whenComplete((value, exception) -> {
            if (exception == null) {
               result.complete(value);
            } else {
               _cache.remove(name, result);
               result.completeExceptionally(exception);
            }
         });
      } catch (Throwable t) {
         _cache.remove(name, result);
         result.completeExceptionally(t);
      }

      return result;
   }

   /**
    * Performs a reverse lookup to find the name (key) associated with the specified handle.
    */
   public String findHandle(T handle) {
      if (handle == null) {
         return null;
      }

      for (Map.Entry<String, CompletableFuture<T>> entry : _cache.entrySet()) {
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