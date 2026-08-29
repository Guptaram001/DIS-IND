package disIND.valueBased.structures;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import disIND.valueBased.membership.AdaptiveColumnCounts;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.utility.UserConfig;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import disIND.valueBased.monitor.WorkerMembershipMetrics;

/**
 * Worker-local, disk-backed membership state shared by all ValueOwner
 */
public final class ValueOwnerMembershipStore implements AutoCloseable {
    private static final float BLOOM_FILTER_BITS_PER_KEY = UserConfig.BLOOM_FILTER_BITS_PER_KEY;

    static {
        RocksDB.loadLibrary();
    }

    private final BloomFilter bloomFilter;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;
    private final BucketMembershipCache[] bucketCaches;
    private final WorkerMembershipMetrics metrics;

    // Write overlay: staged but not-yet-flushed-to-RocksDB deltas. Populated
    // synchronously by VO actors via stage(); drained by the dedicated writer
    // actor via flushOverlay(). Every read path that can fall through to raw
    // RocksDB bytes (loadBatch, loadCandidates, findWitnesses) must consult
    // this first, since a bucketCache/candidateStateCache eviction can happen
    // before an entry is durably flushed.
    private final ConcurrentHashMap<Long, Int2IntMap> pendingRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CandidateKey, CandidateState> pendingCandidates = new ConcurrentHashMap<>();

    private static final byte CANDIDATE_PREFIX = 0x43;
    private static final byte COUNT_STATE_TYPE = 1;
    private static final byte WITNESS_STATE_TYPE = 2;
    private static final byte PRUNE_STATE_TYPE = 3;
    private static final int CANDIDATE_STATE_HEADER_BYTES = Byte.BYTES;
    private static final byte EXACT_STATE_TYPE = 4;

    public static final int MAX_WITNESSES = UserConfig.MAX_VALUE_OWNER_WITNESSES;
    private static final int MAX_MEMBERSHIP_CACHE_BYTES = 512 * 1024 * 1024;
    private static final int CANDIDATE_CACHE_BASE_WEIGHT = 10;

    public record CandidateKey(int bucketId, int lhsCol, int rhsCol) {
    }

    private final Cache<CandidateKey, CandidateState> candidateStateCache;

    public sealed interface CandidateState permits CountState, WitnessState, PruneState, ExactState {
        boolean rejected();

        CandidateTrackingMode trackingMode();
    }

    public record CountState(int violationCount) implements CandidateState {
        public CountState {
            if (violationCount < 0)
                throw new IllegalArgumentException("Violation count cannot be negative");
        }

        @Override
        public boolean rejected() {
            return violationCount > 0;
        }

        @Override
        public CandidateTrackingMode trackingMode() {
            return CandidateTrackingMode.COUNT;
        }
    }

    public record PruneState(int witnessValueId) implements CandidateState {
        public static final int CLUSTER_PROOF = -3;
        public static final int CARDINALITY_PROOF = -2;
        public static final int NO_WITNESS = -1;

        private static final PruneState VALID_STATE = new PruneState(NO_WITNESS);
        private static final PruneState CARDINALITY_STATE = new PruneState(CARDINALITY_PROOF);
        private static final PruneState CLUSTER_STATE = new PruneState(CLUSTER_PROOF);

        public PruneState {
            if (witnessValueId < CLUSTER_PROOF)
                throw new IllegalArgumentException("Invalid prune proof value: " + witnessValueId);
        }

        public static PruneState valid() {
            return VALID_STATE;
        }

        public static PruneState rejected(
                int witnessValueId) {
            if (witnessValueId < 0)
                throw new IllegalArgumentException("Witness value ID must be non-negative");
            return new PruneState(witnessValueId);
        }

        public static PruneState rejectedByCardinality() {
            return CARDINALITY_STATE;
        }

        public static PruneState rejectedByCluster() {
            return CLUSTER_STATE;
        }

        @Override
        public boolean rejected() {
            return witnessValueId != NO_WITNESS;
        }

        @Override
        public CandidateTrackingMode trackingMode() {
            return CandidateTrackingMode.PRUNE;
        }
    }

    public record WitnessState(int[] witnesses) implements CandidateState {

        public WitnessState {
            Objects.requireNonNull(witnesses, "witnesses");
            witnesses = witnesses.clone();
        }

        @Override
        public boolean rejected() {
            return witnesses.length != 0;
        }

        @Override
        public CandidateTrackingMode trackingMode() {
            return CandidateTrackingMode.WITNESS;
        }

        @Override
        public boolean equals(Object object) {
            return object == this || object instanceof WitnessState other && Arrays.equals(witnesses, other.witnesses);
        }
    }

    public record ExactState(boolean rejected) implements CandidateState {

        private static final ExactState VALID = new ExactState(false);
        private static final ExactState REJECTED = new ExactState(true);

        public static ExactState valid() {
            return VALID;
        }

        public static ExactState rejectedState() {
            return REJECTED;
        }

        @Override
        public CandidateTrackingMode trackingMode() {
            return CandidateTrackingMode.EXACT;
        }
    }

    private final class BucketMembershipCache {

        private final Int2ObjectLinkedOpenHashMap<Int2IntMap> entries = new Int2ObjectLinkedOpenHashMap<>();
        private final long maxWeight;
        private long currentWeight;

        private BucketMembershipCache(long maxWeight) {
            this.maxWeight = maxWeight;
        }

        Int2IntMap get(int valueId) {
            Int2IntMap cached = entries.getAndMoveToLast(valueId);
            // Return a mutable copy because MembershipUpdater modifies it.
            return cached == null ? null : new AdaptiveColumnCounts(cached);
        }

        private static long weight(Int2IntMap membership) {
            return 64L + membership.size() * 16L;
        }

        void clear() {
            entries.clear();
            currentWeight = 0;
        }

        void putOwned(int valueId, Int2IntMap membership) {
            Int2IntMap previous = entries.remove(valueId);
            if (previous != null)
                currentWeight -= weight(previous);

            // No map copy. The caller must never mutate membership again.
            entries.putAndMoveToLast(valueId, membership);
            currentWeight += weight(membership);
            while (currentWeight > maxWeight && !entries.isEmpty()) {
                Int2IntMap evicted = entries.removeFirst();
                currentWeight -= weight(evicted);
                metrics.cacheEviction();
            }
        }
    }

    public ValueOwnerMembershipStore(Path directory, long hotEntries, CandidateTrackingMode trackingMode,
            int bucketCount, WorkerMembershipMetrics metrics) {

        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (bucketCount <= 0)
            throw new IllegalArgumentException("bucketCount must be positive");
        try {
            Files.createDirectories(directory);
            this.bucketCaches = new BucketMembershipCache[bucketCount];
            this.bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(bloomFilter)
                    .setWholeKeyFiltering(true);
            this.options = new Options();
            this.options.setCreateIfMissing(true);
            this.options.setTableFormatConfig(tableConfig);
            this.writeOptions = new WriteOptions();
            if (trackingMode == CandidateTrackingMode.WITNESS)
                candidateStateCache = CacheBuilder.newBuilder()
                        .maximumWeight(Math.multiplyExact(hotEntries, CANDIDATE_CACHE_BASE_WEIGHT))
                        .weigher((CandidateKey ignored, CandidateState state) -> candidateStateWeight(state))
                        .build();
            else
                candidateStateCache = CacheBuilder.newBuilder().maximumSize(hotEntries).build();
            this.db = RocksDB.open(options, directory.toString());
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Unable to open ValueOwner membership store at " + directory, exception);
        }
    }

    private BucketMembershipCache bucketCache(int bucketId) {
        if (bucketId < 0 || bucketId >= bucketCaches.length)
            throw new IllegalArgumentException("Invalid bucketId: " + bucketId);
        BucketMembershipCache cache = bucketCaches[bucketId];
        if (cache == null) {
            long bytesPerBucket = MAX_MEMBERSHIP_CACHE_BYTES / bucketCaches.length;
            cache = new BucketMembershipCache(bytesPerBucket);
            bucketCaches[bucketId] = cache;
        }
        return cache;
    }

    private static byte[] candidateKey(CandidateKey key) {
        return ByteBuffer.allocate(1 + Integer.BYTES * 3)
                .put(CANDIDATE_PREFIX)
                .putInt(key.bucketId())
                .putInt(key.lhsCol())
                .putInt(key.rhsCol())
                .array();
    }

    private static byte[] encodeCandidateState(CandidateState state) {
        if (state instanceof CountState countState) {
            return ByteBuffer.allocate(CANDIDATE_STATE_HEADER_BYTES + Integer.BYTES)
                    .put(COUNT_STATE_TYPE)
                    .putInt(countState.violationCount())
                    .array();
        }

        if (state instanceof PruneState pruneState)
            return encodePruneState(pruneState);

        if (state instanceof ExactState)
            return new byte[] { EXACT_STATE_TYPE };

        if (state instanceof WitnessState witnessState) {
            int count = witnessState.witnesses().length;
            ByteBuffer buffer = ByteBuffer
                    .allocate(CANDIDATE_STATE_HEADER_BYTES + Integer.BYTES + count * Integer.BYTES)
                    .put(WITNESS_STATE_TYPE)
                    .putInt(count);
            for (int valueId : witnessState.witnesses())
                buffer.putInt(valueId);
            return buffer.array();
        }
        throw new IllegalArgumentException("Unsupported candidate state: " + state.getClass().getName());
    }

    private static CandidateState decodeCandidateState(byte[] encoded) {
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        if (buffer.remaining() < CANDIDATE_STATE_HEADER_BYTES)
            throw invalidCandidateState("truncated header");

        byte type = buffer.get();
        if (type == COUNT_STATE_TYPE) {
            if (buffer.remaining() != Integer.BYTES)
                throw invalidCandidateState("invalid count-state length");
            int violationCount = buffer.getInt();
            if (violationCount < 0)
                throw invalidCandidateState("negative violation count");
            return new CountState(violationCount);
        }

        if (type == EXACT_STATE_TYPE) {
            if (buffer.hasRemaining())
                throw invalidCandidateState("invalid exact-state length");

            return ExactState.rejectedState();
        }

        if (type == PRUNE_STATE_TYPE) {
            if (buffer.remaining() != Integer.BYTES)
                throw invalidCandidateState("invalid prune-state length");

            int witnessValueId = buffer.getInt();

            if (witnessValueId == PruneState.NO_WITNESS)
                return PruneState.valid();

            if (witnessValueId == PruneState.CARDINALITY_PROOF)
                return PruneState.rejectedByCardinality();

            if (witnessValueId == PruneState.CLUSTER_PROOF)
                return PruneState.rejectedByCluster();

            if (witnessValueId < 0)
                throw invalidCandidateState("invalid prune witness " + witnessValueId);

            return PruneState.rejected(witnessValueId);
        }

        if (type != WITNESS_STATE_TYPE)
            throw invalidCandidateState("unknown state type " + type);
        if (buffer.remaining() < Integer.BYTES)
            throw invalidCandidateState("truncated witness count");

        int count = buffer.getInt();
        if (count < 0 || count > MAX_WITNESSES)
            throw invalidCandidateState("invalid witness count " + count);
        if (buffer.remaining() != count * Integer.BYTES)
            throw invalidCandidateState("invalid length for " + count + " witnesses");

        int[] witnesses = new int[count];
        for (int index = 0; index < count; index++)
            witnesses[index] = buffer.getInt();
        return new WitnessState(witnesses);
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static byte[] key(int bucketId, int valueId) {
        byte[] key = new byte[Integer.BYTES * 2];
        putInt(key, 0, bucketId);
        putInt(key, Integer.BYTES, valueId);
        return key;
    }

    private static long overlayKey(int bucketId, int valueId) {
        return ((long) bucketId << Integer.SIZE) | (valueId & 0xFFFFFFFFL);
    }

    private static int overlayKeyBucket(long overlayKey) {
        return (int) (overlayKey >>> Integer.SIZE);
    }

    private static int overlayKeyValueId(long overlayKey) {
        return (int) overlayKey;
    }

    public Map<CandidateKey, CandidateState> loadCandidates(Set<CandidateKey> keys,
            CandidateTrackingMode trackingMode) {
        Objects.requireNonNull(trackingMode, "trackingMode"); // Throws NPE

        Map<CandidateKey, CandidateState> result = new HashMap<>(keys.size());
        List<CandidateKey> misses = new ArrayList<>();
        List<byte[]> missKeys = new ArrayList<>();

        for (CandidateKey key : keys) {
            CandidateState cached = candidateStateCache.getIfPresent(key);
            if (cached != null) {
                requireTrackingMode(key, cached, trackingMode);
                result.put(key, cached);
                continue;
            }
            // Not yet in the read cache - it may still be sitting unflushed in the
            // write overlay (evicted from cache before the writer persisted it).
            CandidateState overlayState = pendingCandidates.get(key);
            if (overlayState != null) {
                requireTrackingMode(key, overlayState, trackingMode);
                result.put(key, overlayState);
                if (overlayState.rejected())
                    candidateStateCache.put(key, overlayState);
                continue;
            }
            misses.add(key);
            missKeys.add(candidateKey(key));
        }

        if (missKeys.isEmpty())
            return result;

        try {
            List<byte[]> encodedStates = db.multiGetAsList(missKeys);

            for (int index = 0; index < misses.size(); index++) {
                CandidateKey key = misses.get(index);
                byte[] encoded = encodedStates.get(index);
                // encoded = null represent the key is valid and not therefore in store.
                CandidateState state = encoded == null ? validCandidateState(trackingMode)
                        : decodeCandidateState(encoded);
                requireTrackingMode(key, state, trackingMode);
                result.put(key, state);
                if (state.rejected())
                    candidateStateCache.put(key, state);
            }
            return result;
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to load candidate states", exception);
        }
    }

    public WorkerMembershipMetrics.Snapshot metricsSnapshot() {
        long currentEntries = 0L;
        long currentEstimatedBytes = 0L;
        long activeBuckets = 0L;

        for (BucketMembershipCache cache : bucketCaches) {
            if (cache == null)
                continue;
            activeBuckets = Math.incrementExact(activeBuckets);
            currentEntries = Math.addExact(currentEntries, cache.entries.size());
            currentEstimatedBytes = Math.addExact(currentEstimatedBytes, cache.currentWeight);
        }

        return metrics.snapshot(currentEntries, currentEstimatedBytes, MAX_MEMBERSHIP_CACHE_BYTES, activeBuckets);
    }

    private static CandidateState validCandidateState(CandidateTrackingMode trackingMode) {
        return switch (trackingMode) {
            case COUNT -> new CountState(0);
            case WITNESS -> new WitnessState(new int[0]);
            case PRUNE -> PruneState.valid();
            case EXACT -> ExactState.valid();
        };
    }

    private static byte[] encodePruneState(PruneState state) {
        return ByteBuffer.allocate(
                CANDIDATE_STATE_HEADER_BYTES + Integer.BYTES)
                .put(PRUNE_STATE_TYPE)
                .putInt(state.witnessValueId())
                .array();
    }

    private static void requireTrackingMode(CandidateKey key, CandidateState state, CandidateTrackingMode expected) {
        if (state.trackingMode() != expected) {
            throw new IllegalStateException("Candidate " + key + " was stored using " + state.trackingMode() +
                    " tracking but this run uses " + expected);
        }
    }

    private static int candidateStateWeight(CandidateState state) {
        if (state instanceof WitnessState witnessState) {
            return Math.max(CANDIDATE_CACHE_BASE_WEIGHT, witnessState.witnesses().length);
        }
        return CANDIDATE_CACHE_BASE_WEIGHT;
    }

    private static IllegalStateException invalidCandidateState(String detail) {
        return new IllegalStateException("Invalid persisted candidate state " + detail);
    }

    public Int2ObjectMap<Int2IntMap> loadBatch(int bucketId, IntSet valueIds) {
        Int2ObjectMap<Int2IntMap> result = new Int2ObjectOpenHashMap<>(valueIds.size());
        IntArrayList misses = new IntArrayList();
        List<byte[]> missKeys = new ArrayList<>();

        for (int valueId : valueIds) {
            Int2IntMap cached = bucketCache(bucketId).get(valueId);
            if (cached != null) {
                metrics.cacheHit();
                result.put(valueId, cached);
                continue;
            }
            // Not yet in the read cache - it may still be sitting unflushed in the write
            // overlay
            Int2IntMap overlayRecord = pendingRecords.get(overlayKey(bucketId, valueId));
            if (overlayRecord != null) {
                metrics.cacheHit();
                result.put(valueId, new AdaptiveColumnCounts(overlayRecord));
                continue;
            }
            metrics.cacheMiss();
            misses.add(valueId);
            missKeys.add(key(bucketId, valueId));
        }

        if (!missKeys.isEmpty()) {
            long readStarted = System.nanoTime();
            List<byte[]> encoded;
            try {
                encoded = db.multiGetAsList(missKeys);
            } catch (RocksDBException exception) {
                throw new IllegalStateException("Unable to batch-load VO bucket " + bucketId, exception);
            } finally {
                metrics.rocksRead(missKeys.size(), System.nanoTime() - readStarted);
            }
            for (int index = 0; index < misses.size(); index++) {
                int valueId = misses.getInt(index);
                byte[] stored = encoded.get(index);
                Int2IntMap record = stored == null ? new AdaptiveColumnCounts() : decode(stored);
                result.put(valueId, record);
            }

        }
        return result;
    }

    public void stage(int bucketId, Int2ObjectMap<Int2IntMap> changedRecords,
            Map<CandidateKey, CandidateState> changedCandidates) {

        ObjectIterator<Int2ObjectMap.Entry<Int2IntMap>> iterator = Int2ObjectMaps.fastIterator(changedRecords);
        while (iterator.hasNext()) {
            Int2ObjectMap.Entry<Int2IntMap> entry = iterator.next();
            int valueId = entry.getIntKey();
            Int2IntMap record = entry.getValue();
            bucketCache(bucketId).putOwned(valueId, record);
            pendingRecords.put(overlayKey(bucketId, valueId), record);
        }

        for (Map.Entry<CandidateKey, CandidateState> entry : changedCandidates.entrySet()) {
            CandidateKey key = entry.getKey();
            CandidateState state = entry.getValue();
            if (state.rejected())
                candidateStateCache.put(key, state);
            else
                candidateStateCache.invalidate(key);
            pendingCandidates.put(key, state);
        }
    }

    public int pendingOverlaySize() {
        return pendingRecords.size() + pendingCandidates.size();
    }

    public void flushOverlay() {
        if (pendingRecords.isEmpty() && pendingCandidates.isEmpty())
            return;

        Map<Long, Int2IntMap> recordSnapshot = new HashMap<>(pendingRecords);
        Map<CandidateKey, CandidateState> candidateSnapshot = new HashMap<>(pendingCandidates);
        if (recordSnapshot.isEmpty() && candidateSnapshot.isEmpty())
            return;

        long logicalBytes = 0L;
        long membershipWrites = 0L;
        long candidateWrites = 0L;
        long candidateDeletes = 0L;

        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<Long, Int2IntMap> entry : recordSnapshot.entrySet()) {
                long overlayKey = entry.getKey();
                byte[] encodedKey = key(overlayKeyBucket(overlayKey), overlayKeyValueId(overlayKey));
                byte[] encodedValue = encode(entry.getValue());
                batch.put(encodedKey, encodedValue);
                logicalBytes = Math.addExact(logicalBytes, encodedKey.length + encodedValue.length);
                membershipWrites = Math.incrementExact(membershipWrites);
            }
            for (Map.Entry<CandidateKey, CandidateState> entry : candidateSnapshot.entrySet()) {
                CandidateKey key = entry.getKey();
                CandidateState state = entry.getValue();
                byte[] encodedKey = candidateKey(key);
                if (state.rejected()) {
                    byte[] encodedValue = encodeCandidateState(state);
                    batch.put(encodedKey, encodedValue);
                    logicalBytes = Math.addExact(logicalBytes, encodedKey.length + encodedValue.length);
                    candidateWrites = Math.incrementExact(candidateWrites);
                } else {
                    batch.delete(encodedKey);
                    logicalBytes = Math.addExact(logicalBytes, encodedKey.length);
                    candidateDeletes = Math.incrementExact(candidateDeletes);
                }
            }

            long writeStarted = System.nanoTime();
            db.write(writeOptions, batch);
            long writeNanos = System.nanoTime() - writeStarted;
            metrics.rocksWrite(writeNanos, logicalBytes, membershipWrites, candidateWrites, candidateDeletes);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to flush VO write overlay", exception);
        }

        for (Map.Entry<Long, Int2IntMap> entry : recordSnapshot.entrySet())
            pendingRecords.remove(entry.getKey(), entry.getValue());
        for (Map.Entry<CandidateKey, CandidateState> entry : candidateSnapshot.entrySet())
            pendingCandidates.remove(entry.getKey(), entry.getValue());
    }

    public int[] findWitnesses(int bucketId, int lhsCol, int rhsCol, int limit,
            Int2ObjectMap<Int2IntMap> updatedRecords) {
        if (limit <= 0 || limit > MAX_WITNESSES) {
            throw new IllegalArgumentException("Witness limit must be between 1 and " + MAX_WITNESSES);
        }

        IntArrayList witnesses = new IntArrayList(limit);
        IntSet seenValues = new IntOpenHashSet();
        byte[] prefix = bucketPrefix(bucketId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && hasPrefix(iterator.key(), prefix)) {
                byte[] storedKey = iterator.key();
                if (!hasPrefix(storedKey, prefix))
                    break;
                if (storedKey.length != Integer.BYTES * 2) {
                    iterator.next();
                    continue;
                }

                // int valueId = ByteBuffer.wrap(storedKey).getInt(Integer.BYTES);
                int valueId = readInt(storedKey, Integer.BYTES);
                Int2IntMap membership = updatedRecords.get(valueId);
                if (membership == null)
                    membership = pendingRecords.get(overlayKey(bucketId, valueId));
                boolean violation;
                if (membership != null) {
                    seenValues.add(valueId);
                    violation = isViolation(membership, lhsCol, rhsCol);
                } else
                    violation = isEncodedViolation(iterator.value(), lhsCol, rhsCol);

                if (violation) {
                    witnesses.add(valueId);
                    if (witnesses.size() == limit)
                        break;
                }
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException(
                    "Unable to find witnesses for bucket=" + bucketId + " candidate=" + lhsCol + "⊆" + rhsCol,
                    exception);
        }
        if (witnesses.size() < limit) {
            ObjectIterator<Int2ObjectMap.Entry<Int2IntMap>> iterator = Int2ObjectMaps.fastIterator(updatedRecords);
            while (iterator.hasNext()) {
                Int2ObjectMap.Entry<Int2IntMap> entry = iterator.next();
                int valueId = entry.getIntKey();
                if (!seenValues.add(valueId))
                    continue;
                Int2IntMap membership = entry.getValue();
                if (isViolation(membership, lhsCol, rhsCol)) {
                    witnesses.add(valueId);
                    if (witnesses.size() == limit)
                        break;
                }
            }
        }

        if (witnesses.size() < limit && !pendingRecords.isEmpty()) {
            for (Map.Entry<Long, Int2IntMap> entry : pendingRecords.entrySet()) {
                long compositeKey = entry.getKey();
                if (overlayKeyBucket(compositeKey) != bucketId)
                    continue;
                int valueId = overlayKeyValueId(compositeKey);
                if (!seenValues.add(valueId))
                    continue;
                if (isViolation(entry.getValue(), lhsCol, rhsCol)) {
                    witnesses.add(valueId);
                    if (witnesses.size() == limit)
                        break;
                }
            }
        }
        return witnesses.toIntArray();
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static boolean isEncodedViolation(byte[] encoded, int lhsCol, int rhsCol) {

        if (encoded.length < Integer.BYTES)
            throw new IllegalStateException("Truncated membership record");

        int size = readInt(encoded, 0);
        long expectedLength = Integer.BYTES + (long) size * 2 * Integer.BYTES;

        if (size < 0 || expectedLength != encoded.length)
            throw new IllegalStateException("Invalid membership record size");

        boolean lhsPresent = false;
        int offset = Integer.BYTES;

        for (int index = 0; index < size; index++) {
            int column = readInt(encoded, offset);
            offset += 2 * Integer.BYTES;
            if (column == rhsCol)
                return false;
            if (column == lhsCol)
                lhsPresent = true;
        }

        return lhsPresent;
    }

    private static byte[] bucketPrefix(int bucketId) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(bucketId).array();
    }

    private static boolean hasPrefix(byte[] key, byte[] prefix) {
        if (key.length < prefix.length)
            return false;
        for (int index = 0; index < prefix.length; index++) {
            if (key[index] != prefix[index])
                return false;
        }
        return true;
    }

    private static boolean isViolation(Int2IntMap membership, int lhsCol, int rhsCol) {
        return membership.containsKey(lhsCol) && !membership.containsKey(rhsCol);
    }

    // Removed Sorted one
    private static byte[] encode(Int2IntMap columns) {
        ByteBuffer buffer = ByteBuffer.allocate(
                Math.addExact(
                        Integer.BYTES,
                        Math.multiplyExact(
                                columns.size(),
                                2 * Integer.BYTES)));

        buffer.putInt(columns.size());

        ObjectIterator<Int2IntMap.Entry> iterator = Int2IntMaps.fastIterator(columns);

        while (iterator.hasNext()) {
            Int2IntMap.Entry entry = iterator.next();

            buffer.putInt(entry.getIntKey());
            buffer.putInt(entry.getIntValue());
        }

        return buffer.array();
    }

    private static Int2IntMap decode(byte[] encoded) {
        if (encoded.length < Integer.BYTES)
            throw new IllegalStateException("Truncated membership record");

        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        int size = buffer.getInt();

        if (size < 0 || encoded.length != Integer.BYTES + size * 2 * Integer.BYTES)
            throw new IllegalStateException("Invalid membership record size");

        Int2IntMap columns = new AdaptiveColumnCounts(size);
        for (int index = 0; index < size; index++)
            columns.put(buffer.getInt(), buffer.getInt());

        return columns;
    }

    @Override
    public void close() {
        flushOverlay();
        candidateStateCache.invalidateAll();

        for (BucketMembershipCache cache : bucketCaches) {
            if (cache != null)
                cache.clear();
        }
        db.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
