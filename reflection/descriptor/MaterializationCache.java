/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.reflection.descriptor;

import static com.dremio.exec.planner.physical.PlannerSettings.REFLECTION_MATERIALIZATION_STALENESS_ENABLED;
import static com.dremio.service.reflection.ExternalReflectionStatus.STATUS.OUT_OF_SYNC;
import static com.dremio.service.reflection.ReflectionMetrics.TAG_SOURCE_DOWN;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_INIT_TIMEOUT_SECONDS;
import static java.util.Collections.emptyMap;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.descriptor.ExpandedMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.Pointer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionMetrics;
import com.dremio.service.reflection.ReflectionOptions;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Failure;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

/**
 * Cache for expanded {@link MaterializationDescriptor} to avoid having to expand all the
 * descriptor's plans for every planned query.
 */
public class MaterializationCache implements MaterializationCacheViewer {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaterializationCache.class);

  private final AtomicReference<Map<String, ExpandedMaterializationDescriptor>> cached =
      new AtomicReference<>(emptyMap());

  private final CountDownLatch latch;

  private final Meter.MeterProvider<Timer> syncHistogram;

  // Error with expanding materialization that may be retried
  private final Meter.MeterProvider<Counter> errorCounter;

  // Permanent failures where materialization is marked as FAILED
  private final Meter.MeterProvider<Counter> retryFailedCounter;

  /**
   * This retryMap tracks the timestamp of the first attempt to update a materialization into the
   * materialization cache. Each update may rebuild the materialization plan, de-serialize the plan
   * bytes and expand the descriptor. If this fails, we retry up to {@link
   * ReflectionOptions#MATERIALIZATION_CACHE_RETRY_MINUTES} before marking the materialization as
   * failed.
   *
   * <p>expireAfterWrite is needed to clean up the retryMap in case the materialization is
   * deprecated by reflection manager and can't be cleaned up by a successful or failed update.
   */
  private final LoadingCache<MaterializationId, Long> retryMap =
      Caffeine.newBuilder()
          .expireAfterWrite(ReflectionOptions.MAX_RETRY_HOURS + 1, TimeUnit.HOURS)
          .build(
              new CacheLoader<MaterializationId, Long>() {
                @Override
                public Long load(MaterializationId key) {
                  return System.currentTimeMillis();
                }
              });

  private final DescriptorHelper provider;
  private final ReflectionStatusService reflectionStatusService;
  private final CatalogService catalogService;
  private final OptionManager optionManager;
  private final MaterializationStore materializationStore;
  private final ReflectionEntriesStore entriesStore;

  public MaterializationCache(
      DescriptorHelper provider,
      ReflectionStatusService reflectionStatusService,
      CatalogService catalogService,
      OptionManager optionManager,
      MaterializationStore materializationStore,
      ReflectionEntriesStore entriesStore) {
    this.provider = Preconditions.checkNotNull(provider, "materialization provider required");
    this.reflectionStatusService =
        Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.optionManager = Preconditions.checkNotNull(optionManager, "option manager required");
    this.materializationStore = materializationStore;
    this.entriesStore = entriesStore;
    latch = new CountDownLatch(1);
    syncHistogram =
        Timer.builder(ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_SYNC))
            .description("Histogram of reflection materialization cache sync times")
            .publishPercentileHistogram()
            .withRegistry(Metrics.globalRegistry);
    Gauge.builder(
            ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_ENTRIES),
            () -> cached.get().size())
        .description("Number of materialization cache entries")
        .register(Metrics.globalRegistry);
    errorCounter =
        io.micrometer.core.instrument.Counter.builder(
                ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_ERRORS))
            .description("Counter for materialization cache errors")
            .withRegistry(io.micrometer.core.instrument.Metrics.globalRegistry);
    retryFailedCounter =
        io.micrometer.core.instrument.Counter.builder(
                ReflectionMetrics.createName(ReflectionMetrics.MAT_CACHE_RETRY_FAILED))
            .description("Counter for materialization cache retry failures")
            .withRegistry(io.micrometer.core.instrument.Metrics.globalRegistry);
  }

  @WithSpan
  public void refreshCache() {
    if (isCacheDisabled()) {
      logger.debug("Materialization cache is disabled. Resetting materialization cache");
      resetCache();
    } else {
      logger.debug("Materialization cache refresh...");
      if (latch.getCount() > 0) {
        syncHistogram
            .withTag(ReflectionMetrics.TAG_MAT_CACHE_INITIAL, "true")
            .record(() -> compareAndSetCache());
      } else {
        syncHistogram
            .withTag(ReflectionMetrics.TAG_MAT_CACHE_INITIAL, "false")
            .record(() -> compareAndSetCache());
      }
    }
  }

  private void compareAndSetCache() {
    final Instant coldStart = Instant.now();
    try {
      boolean exchanged;
      do {
        Map<String, ExpandedMaterializationDescriptor> old = cached.get();
        Map<String, ExpandedMaterializationDescriptor> updated = updateMaterializationCache(old);
        exchanged = cached.compareAndSet(old, updated);
        if (!exchanged) {
          logger.warn(
              "Unable to compare and set cache.  Old count: {}.  Updated count: {}",
              old.size(),
              updated.size());
        }
      } while (!exchanged);
    } finally {
      if (latch.getCount() > 0) {
        logger.info(
            "Materialization Cache Initialization: Cold cache update took {} ms: expanded={} version={}",
            Duration.between(coldStart, Instant.now()).toMillis(),
            cached.get().size(),
            DremioVersionInfo.getVersion());
      }
      latch.countDown();
    }
  }

  private void resetCache() {
    boolean exchanged;
    do {
      Map<String, ExpandedMaterializationDescriptor> old = cached.get();
      exchanged = cached.compareAndSet(old, emptyMap());
    } while (!exchanged);
  }

  /**
   * Updates the cache map taking into account the existing cache.<br>
   * Will only "expand" descriptors that are new in the cache.<br>
   * Because, in debug mode, this can be called from multiple threads, it must be thread-safe
   *
   * @param old existing cache
   * @return updated cache
   */
  @WithSpan
  private Map<String, ExpandedMaterializationDescriptor> updateMaterializationCache(
      Map<String, ExpandedMaterializationDescriptor> old) {

    // new list of descriptors
    final Iterable<Materialization> provided = provider.getValidMaterializations();
    // this will hold the updated cache
    final Map<String, ExpandedMaterializationDescriptor> updated = Maps.newHashMap();

    int materializationExpandCount = 0;
    int materializationReuseCount = 0;
    int materializationErrorCount = 0;
    // cache is enabled so we want to reuse as much of the existing cache as possible. Make sure to:
    // remove all cached descriptors that no longer exist
    // reuse all descriptors that are already in the cache
    // add any descriptor that are not already cached
    final Catalog catalog = CatalogUtil.getSystemCatalogForMaterializationCache(catalogService);
    final boolean materializationStalenessEnabled =
        optionManager.getOption(REFLECTION_MATERIALIZATION_STALENESS_ENABLED);
    for (Materialization materialization : provided) {
      final ExpandedMaterializationDescriptor cachedDescriptor =
          old.get(materialization.getId().getId());
      if (cachedDescriptor == null || schemaChanged(cachedDescriptor, materialization, catalog)) {
        if (updateMaterializationEntry(updated, materialization, catalog)) {
          materializationExpandCount++;
        } else {
          materializationErrorCount++;
        }
      } else {
        // Descriptor already in the cache, we can just reuse it.
        // If materialization staleness changes after materialization is expanded and cached,
        // simply update the staleness flag of cached descriptor.
        if (materializationStalenessEnabled
            && materialization.getIsStale()
            && !cachedDescriptor.isStale()) {
          logger.info(
              "update staleness flag for existing descriptor {}",
              ReflectionUtils.getId(materialization));
          updated.put(
              materialization.getId().getId(),
              new ExpandedMaterializationDescriptor(
                  cachedDescriptor, materialization.getIsStale(), materialization.getTag()));
        } else {
          updated.put(materialization.getId().getId(), cachedDescriptor);
        }
        materializationReuseCount++;
      }
    }

    int externalExpandCount = 0;
    int externalReuseCount = 0;
    int externalErrorCount = 0;
    for (ExternalReflection externalReflection : provider.getExternalReflections()) {
      final ExpandedMaterializationDescriptor cachedDescriptor =
          old.get(externalReflection.getId());
      if (cachedDescriptor == null
          || isExternalReflectionOutOfSync(externalReflection.getId())
          || isExternalReflectionMetadataUpdated(cachedDescriptor, catalog)) {
        if (updateExternalReflectionEntry(updated, externalReflection, catalog)) {
          externalExpandCount++;
        } else {
          externalErrorCount++;
        }
      } else {
        // descriptor already in the cache, we can just reuse it
        updated.put(externalReflection.getId(), cachedDescriptor);
        externalReuseCount++;
      }
    }
    Set<String> oldKeys = new HashSet<>(old.keySet());
    Set<String> updatedKeys = new HashSet<>(updated.keySet());
    oldKeys.removeAll(updatedKeys);
    logger.info(
        "Materialization cache updated. Materializations: "
            + "reused={} expanded={} errors={}. External: "
            + "reused={} expanded={} errors={}. removed={}",
        materializationReuseCount,
        materializationExpandCount,
        materializationErrorCount,
        externalReuseCount,
        externalExpandCount,
        externalErrorCount,
        oldKeys);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationReuseCount", materializationReuseCount);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationExpandCount", materializationExpandCount);
    Span.current()
        .setAttribute(
            "dremio.materialization_cache.materializationErrorCount", materializationErrorCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalReuseCount", externalReuseCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalExpandCount", externalExpandCount);
    Span.current()
        .setAttribute("dremio.materialization_cache.externalErrorCount", externalErrorCount);
    CatalogUtil.clearAllDatasetCache(catalog);
    return updated;
  }

  private boolean isExternalReflectionMetadataUpdated(
      ExpandedMaterializationDescriptor descriptor, EntityExplorer catalog) {
    DremioMaterialization materialization = descriptor.getMaterialization();
    Pointer<Boolean> updated = new Pointer<>(false);
    materialization
        .getTableRel()
        .accept(
            new RelShuttleImpl() {
              @Override
              public RelNode visit(TableScan tableScan) {
                if (tableScan instanceof ScanCrel) {
                  String version = ((ScanCrel) tableScan).getTableMetadata().getVersion();
                  DatasetConfig datasetConfig =
                      CatalogUtil.getDatasetConfig(
                          catalog, new NamespaceKey(tableScan.getTable().getQualifiedName()));
                  if (datasetConfig == null) {
                    updated.value = true;
                  } else {
                    if (!datasetConfig.getTag().equals(version)) {
                      logger.info(
                          "Dataset {} has new data. Invalidating cache for external reflection",
                          tableScan.getTable().getQualifiedName());
                      updated.value = true;
                    }
                  }
                } else {
                  updated.value = true;
                }
                return tableScan;
              }
            });
    return updated.value;
  }

  private boolean isExternalReflectionOutOfSync(String id) {
    return reflectionStatusService
            .getExternalReflectionStatus(new ReflectionId(id))
            .getConfigStatus()
        == OUT_OF_SYNC;
  }

  @WithSpan
  private boolean updateExternalReflectionEntry(
      Map<String, ExpandedMaterializationDescriptor> cache,
      ExternalReflection entry,
      Catalog catalog) {
    Span.current().setAttribute("dremio.materialization_cache.reflection_id", entry.getId());
    Span.current().setAttribute("dremio.materialization_cache.name", entry.getName());
    Span.current()
        .setAttribute("dremio.materialization_cache.query_dataset_id", entry.getQueryDatasetId());
    Span.current()
        .setAttribute("dremio.materialization_cache.target_dataset_id", entry.getTargetDatasetId());
    try {
      final ExpandedMaterializationDescriptor descriptor = provider.expand(entry, catalog);
      if (descriptor != null) {
        cache.put(entry.getId(), descriptor);
        logger.info(
            "added to cache external reflection: name={}, id={}", entry.getName(), entry.getId());
        return true;
      }
    } catch (Throwable e) {
      if (!isInitialized()) {
        logger.warn(
            "Materialization Cache Initialization: Error occurred with external materialization {}",
            entry,
            e);
      } else {
        logger.warn(
            "Materialization Cache Retry: Error occurred with external materialization {}",
            entry,
            e);
      }
      incrementCounter(errorCounter, e);
    }
    return false;
  }

  @WithSpan
  private boolean updateMaterializationEntry(
      Map<String, ExpandedMaterializationDescriptor> cache,
      Materialization materialization,
      Catalog catalog) {
    try {
      if (!Boolean.TRUE.equals(materialization.getIsIcebergDataset())) {
        logger.warn("Non-iceberg {} is not supported", ReflectionUtils.getId(materialization));
        throw new UnsupportedOperationException(
            String.format(
                "Non-iceberg %s is not supported", ReflectionUtils.getId(materialization)));
      }

      Span.current()
          .setAttribute(
              "dremio.materialization_cache.reflection_id",
              materialization.getReflectionId().getId());
      Span.current()
          .setAttribute(
              "dremio.materialization_cache.materialization_id", materialization.getId().getId());
      final ExpandedMaterializationDescriptor descriptor =
          provider.expand(materialization, catalog);
      if (descriptor != null) {
        cache.put(materialization.getId().getId(), descriptor);
        logger.info("added to cache {}", ReflectionUtils.getId(materialization));
        retryMap.invalidate(materialization.getId());
        return true;
      }
    } catch (Throwable e) {
      if (!isInitialized()) {
        logger.warn(
            "Materialization Cache Initialization: Error expanding {}. Will retry.",
            ReflectionUtils.getId(materialization),
            e);
      } else {
        logger.warn(
            "Materialization Cache Retry: Error expanding {}. Will retry.",
            ReflectionUtils.getId(materialization),
            e);
      }
      incrementCounter(errorCounter, e);
      Long retryMinutes =
          optionManager.getOption(ReflectionOptions.MATERIALIZATION_CACHE_RETRY_MINUTES);
      // Source down exceptions have unlimited retries
      if (!Boolean.TRUE.equals(materialization.getIsIcebergDataset())
          || (!ReflectionUtils.isSourceDown(e)
              && Duration.of(
                          System.currentTimeMillis() - retryMap.get(materialization.getId()),
                          ChronoUnit.MILLIS)
                      .toMinutes()
                  >= retryMinutes)) {
        // Exceeded max retry minutes so mark the materialization as failed and stop retrying.
        // Next materialization will be rebuilt based on the reflection's refresh policy.
        String failureMsg =
            String.format(
                "Materialization Cache Failure: Error expanding %s. All retries exhausted. Updated to FAILED. %s",
                ReflectionUtils.getId(materialization), e.getMessage());
        logger.error(failureMsg, e);
        Materialization update = materializationStore.get(materialization.getId());
        update.setState(MaterializationState.FAILED);
        update.setFailure(
            new Failure()
                .setMessage(failureMsg)
                .setStackTrace(Throwables.getStackTraceAsString(e)));
        try {
          materializationStore.save(update);
          incrementCounter(retryFailedCounter, e);
          ReflectionEntry entry = entriesStore.get(update.getReflectionId());
          entry.setLastFailure(
              new Failure()
                  .setMessage(failureMsg)
                  .setStackTrace(Throwables.getStackTraceAsString(e)));
          if (!Boolean.TRUE.equals(materialization.getIsIcebergDataset())) {
            entry.setNumFailures(1); // hand it over to reflection retry policy
          }
          entriesStore.save(entry);
        } catch (ConcurrentModificationException e2) {
          // ignore in case another coordinator also tries to mark the materialization as failed
        }
        retryMap.invalidate(materialization.getId());
      }
    }
    return false;
  }

  private void incrementCounter(Meter.MeterProvider<Counter> counter, Throwable t) {
    counter
        .withTags(
            PlannerMetrics.TAG_REASON,
            t.getClass().getSimpleName(),
            TAG_SOURCE_DOWN,
            ReflectionUtils.isSourceDown(t) ? "true" : "false")
        .increment();
  }

  private boolean schemaChanged(
      MaterializationDescriptor old, Materialization materialization, EntityExplorer catalog) {
    // TODO is this enough ? shouldn't we use the dataset hash instead ??
    final NamespaceKey matKey =
        new NamespaceKey(ReflectionUtils.getMaterializationPath(materialization));

    DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, matKey);
    if (datasetConfig == null) {
      return true;
    }

    ByteString schemaString = datasetConfig.getRecordSchema();
    BatchSchema newSchema = BatchSchema.deserialize(schemaString);
    BatchSchema oldSchema =
        ((ExpandedMaterializationDescriptor) old).getMaterialization().getSchema();
    return !oldSchema.equals(newSchema);
  }

  /** Used by reflection manager to immediately invalidate a materialization from the cache. */
  public void invalidate(Materialization materialization) {
    final MaterializationId mId = materialization.getId();
    boolean exchanged;
    do {
      Map<String, ExpandedMaterializationDescriptor> old = cached.get();
      if (!old.containsKey(mId.getId())) {
        break; // entry not present in the cache, nothing more to do
      }
      // copy over everything
      Map<String, ExpandedMaterializationDescriptor> updated = Maps.newHashMap(old);
      // remove the specific materialization.
      updated.remove(mId.getId());
      logger.info(
          "RM invalidate {}, count={}", ReflectionUtils.getId(materialization), updated.size());
      // update the cache.
      exchanged = cached.compareAndSet(old, updated);
      if (!exchanged) {
        logger.warn(
            "RM invalidate {}: Unable to compare and set cache.  Old count: {}.  Updated count: {}",
            ReflectionUtils.getId(materialization),
            old.size(),
            updated.size());
      }
    } while (!exchanged);
  }

  /**
   * Used by reflection manager to immediately update the materialization cache when RM is on the
   * same coordinator. Otherwise, on other coordinators, there could be a materialization cache sync
   * delay.
   */
  public void update(Materialization current, Materialization previous)
      throws InterruptedException {
    // Let initial cold cache sync complete first so that we don't risk causing the compareAndSet to
    // fail there.
    latch.await(10, TimeUnit.MINUTES);

    // Do expansion (including deserialization) out of the do-while loop, so that in case it takes
    // long time
    // the update loop does not race with MaterializationCache.refresh() and falls into infinite
    // loop.
    final ExpandedMaterializationDescriptor descriptor =
        provider.expand(
            current, CatalogUtil.getSystemCatalogForMaterializationCache(catalogService));
    if (descriptor != null) {
      boolean exchanged;
      do {
        Map<String, ExpandedMaterializationDescriptor> old = cached.get();
        Map<String, ExpandedMaterializationDescriptor> updated =
            Maps.newHashMap(old); // copy over everything
        if (previous != null) {
          updated.remove(previous.getId().getId());
        }
        updated.put(current.getId().getId(), descriptor);
        logger.info(
            "RM update current={}, previous={}, count={}",
            ReflectionUtils.getId(current),
            previous != null ? ReflectionUtils.getId(previous) : null,
            updated.size());
        exchanged = cached.compareAndSet(old, updated); // update the cache.
        if (!exchanged) {
          logger.warn(
              "RM update {}: Unable to compare and set cache.  Old count: {}.  Updated count: {}",
              ReflectionUtils.getId(current),
              old.size(),
              updated.size());
        }
      } while (!exchanged);
    }
  }

  /**
   * Returns cached materialization descriptors for logical planning. Blocks on initialization of
   * the materialization cache.
   */
  Iterable<MaterializationDescriptor> getAll() {
    boolean success;
    try {
      success =
          latch.await(
              this.optionManager.getOption(MATERIALIZATION_CACHE_INIT_TIMEOUT_SECONDS),
              TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      success = false;
    }
    if (!success) {
      throw new MaterializationCacheTimeoutException(
          "Timed out waiting for materialization cache to initialize.");
    }
    Collection<ExpandedMaterializationDescriptor> currentValues = cached.get().values();
    if (logger.isDebugEnabled()) {
      logger.debug("get all materializations, count={}", currentValues.size());
    }
    return Iterables.unmodifiableIterable(currentValues);
  }

  @Override
  public boolean isCached(MaterializationId id) {
    return isCacheDisabled() || cached.get().containsKey(id.getId());
  }

  @Override
  public boolean isInitialized() {
    if (isCacheDisabled()) {
      return true;
    }
    return latch.getCount() == 0;
  }

  private boolean isCacheDisabled() {
    return !optionManager.getOption(MATERIALIZATION_CACHE_ENABLED);
  }

  /** Returns descriptor for default raw reflection matching during convertToRel */
  MaterializationDescriptor getOne(Materialization materialization, NamespaceKey path) {
    final MaterializationId mId = materialization.getId();
    final MaterializationDescriptor descriptor = cached.get().get(mId.getId());
    if (logger.isDebugEnabled()) {
      logger.debug(
          "get one {}, view={}, found={}",
          ReflectionUtils.getId(materialization),
          path.getSchemaPath(),
          descriptor != null);
    }
    return descriptor;
  }

  public static class MaterializationCacheTimeoutException extends RuntimeException {

    public MaterializationCacheTimeoutException(String message) {
      super(message);
    }
  }

  @VisibleForTesting
  LoadingCache<MaterializationId, Long> getRetryMap() {
    return retryMap;
  }
}
