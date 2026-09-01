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
import akka.actor.typed.ActorRef;

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
import java.util.concurrent.atomic.AtomicLong;

import disIND.valueBased.protocol.MembershipWriteProtocol.CandidateWrite;
import disIND.valueBased.protocol.MembershipWriteProtocol.EncodedWriteBatch;
import disIND.valueBased.protocol.ValueOwnerProtocol;
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
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import disIND.valueBased.monitor.WorkerMembershipMetrics;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

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
    private final BucketMembershipCache[] bucketMembershipCaches;
    private final WorkerMembershipMetrics metrics;

    private final AtomicLong pinnedEstimatedBytes = new AtomicLong();

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
    private final CandidateWriteBackCache[] candidateWriteBackByBucket;

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

    private static final class MembershipCacheEntry {
        private Int2IntMap membership;
        private boolean dirty;
        private boolean inFlight;
        private boolean queued;

        private MembershipCacheEntry(Int2IntMap membership, boolean dirty, boolean inFlight) {
            this.membership = membership;
            this.dirty = dirty;
            this.inFlight = inFlight;
        }

        private boolean mutated() {
            return dirty || inFlight;
        }

        private boolean removeable() {
            return !mutated();
        }
    }

    private static final class CandidateWriteBackEntry {
        private CandidateState state;
        private boolean dirty = true;
        private boolean inFlight;
        private boolean queued;

        private CandidateWriteBackEntry(CandidateState state) {
            this.state = state;
        }
    }

    private static final class CandidateWriteBackCache {

        private final Long2ObjectOpenHashMap<CandidateWriteBackEntry> entries = new Long2ObjectOpenHashMap<>();
        private final LongArrayFIFOQueue dirtyKeys = new LongArrayFIFOQueue();

        private void enqueueDirty(long pairKey, CandidateWriteBackEntry entry) {
            if (entry.queued)
                return;
            entry.queued = true;
            dirtyKeys.enqueue(pairKey);
        }
    }

    public record InFlightWrite(long batchId, int[] membershipValueIds, long[] candidateKeys, long encodedBytes) {
    }

    public record PreparedWriteBatch(EncodedWriteBatch message, InFlightWrite cleanup) {
        public boolean isEmpty() {
            return message == null;
        }

        public static PreparedWriteBatch empty() {
            return new PreparedWriteBatch(null, null);
        }
    }

    private final class BucketMembershipCache {

        private final Int2ObjectLinkedOpenHashMap<MembershipCacheEntry> entries = new Int2ObjectLinkedOpenHashMap<>();
        private final IntOpenHashSet overlayValueIds = new IntOpenHashSet();
        private final IntArrayFIFOQueue dirtyValueIds = new IntArrayFIFOQueue();
        private final long maxWeight;
        private long currentWeight;

        private BucketMembershipCache(long maxWeight) {
            this.maxWeight = maxWeight;
        }

        Int2IntMap takeForUpdate(int valueId) {
            // MembershipCacheEntry cached = entries.getAndMoveToLast(valueId);
            // Return a mutable copy because MembershipUpdater modifies it.
            // return cached == null ? null : new AdaptiveColumnCounts(cached.membership);
            MembershipCacheEntry entry = entries.get(valueId);

            if (entry == null)
                return null;

            // dirty or inflight entries must be back into the cache after membershipupdate
            // hence move to last and copy it instead
            if (entry.mutated()) {
                entries.getAndMoveToLast(valueId);
                return new AdaptiveColumnCounts(entry.membership);
            }

            // Clean entry removed for update without creating copy.
            entries.remove(valueId);
            currentWeight -= weight(entry.membership);

            return entry.membership;
        }

        MembershipCacheEntry peek(int valueId) {
            return entries.get(valueId);
        }

        private static long weight(Int2IntMap membership) {
            return 64L + membership.size() * 16L;
        }

        void clear() {
            entries.clear();
            overlayValueIds.clear();
            dirtyValueIds.clear();
            currentWeight = 0;
        }

        void putOwned(int valueId, Int2IntMap membership) {
            MembershipCacheEntry entry = entries.remove(valueId);
            long previousWeight = entry == null ? 0L : weight(entry.membership);
            if (entry != null)
                currentWeight -= previousWeight;

            // No map copy. The caller must never mutate membership again.
            boolean wasmutated = entry != null && entry.mutated();
            if (entry == null)
                entry = new MembershipCacheEntry(membership, true, false);
            else {
                entry.membership = membership;
                entry.dirty = true;
            }
            entries.putAndMoveToLast(valueId, entry);
            long updatedWeight = weight(membership);
            currentWeight += updatedWeight;
            overlayValueIds.add(valueId);
            pinnedEstimatedBytes.addAndGet(wasmutated ? updatedWeight - previousWeight : updatedWeight);
            enqueueDirty(valueId, entry);
            evictCleanEntriesIfNecessary();
        }

        private void enqueueDirty(int valueId, MembershipCacheEntry entry) {
            if (entry.queued)
                return;
            entry.queued = true;
            dirtyValueIds.enqueue(valueId);
        }

        private void evictCleanEntriesIfNecessary() {
            while (currentWeight > maxWeight && evictOneCleanEntry()) {
                // Continue until the soft limit is met or every remaining entry is pinned.
            }
        }

        private boolean evictOneCleanEntry() {
            ObjectIterator<Int2ObjectMap.Entry<MembershipCacheEntry>> iterator = Int2ObjectMaps.fastIterator(entries);
            while (iterator.hasNext()) {
                Int2ObjectMap.Entry<MembershipCacheEntry> mapEntry = iterator.next();
                MembershipCacheEntry entry = mapEntry.getValue();
                if (!entry.removeable())
                    continue;
                currentWeight -= weight(entry.membership);
                iterator.remove();
                metrics.cacheEviction();
                return true;
            }
            return false;
        }
    }

    public ValueOwnerMembershipStore(Path directory, long hotEntries, CandidateTrackingMode trackingMode,
            int bucketCount, WorkerMembershipMetrics metrics) {

        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (bucketCount <= 0)
            throw new IllegalArgumentException("bucketCount must be positive");
        try {
            Files.createDirectories(directory);
            this.bucketMembershipCaches = new BucketMembershipCache[bucketCount];
            this.candidateWriteBackByBucket = new CandidateWriteBackCache[bucketCount];
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
        if (bucketId < 0 || bucketId >= bucketMembershipCaches.length)
            throw new IllegalArgumentException("Invalid bucketId: " + bucketId);
        BucketMembershipCache cache = bucketMembershipCaches[bucketId];
        if (cache == null) {
            long bytesPerBucket = MAX_MEMBERSHIP_CACHE_BYTES / bucketMembershipCaches.length;
            cache = new BucketMembershipCache(bytesPerBucket);
            bucketMembershipCaches[bucketId] = cache;
        }
        return cache;
    }

    private static byte[] candidateKey(CandidateKey key) {
        return candidateKey(key.bucketId(), key.lhsCol(), key.rhsCol());
    }

    private static byte[] encodeCandidateState(CandidateState state) {
        if (state instanceof CountState countState) {
            byte[] encoded = new byte[CANDIDATE_STATE_HEADER_BYTES + Integer.BYTES];
            encoded[0] = COUNT_STATE_TYPE;
            putInt(encoded, CANDIDATE_STATE_HEADER_BYTES, countState.violationCount());

            return encoded;
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

    private CandidateWriteBackCache candidateWriteBack(int bucketId) {

        bucketCache(bucketId);
        CandidateWriteBackCache cache = candidateWriteBackByBucket[bucketId];
        if (cache == null) {
            cache = new CandidateWriteBackCache();
            candidateWriteBackByBucket[bucketId] = cache;
        }
        return cache;
    }

    public Map<CandidateKey, CandidateState> loadCandidates(Set<CandidateKey> keys,
            CandidateTrackingMode trackingMode) {
        Objects.requireNonNull(trackingMode, "trackingMode");

        Map<CandidateKey, CandidateState> result = new HashMap<>(keys.size());
        List<CandidateKey> misses = new ArrayList<>(keys.size());
        List<byte[]> missKeys = new ArrayList<>(keys.size());

        for (CandidateKey key : keys) {
            CandidateWriteBackEntry writeBack = candidateWriteBack(key.bucketId()).entries
                    .get(candidatePairKey(key));
            if (writeBack != null) {
                requireTrackingMode(key, writeBack.state, trackingMode);
                result.put(key, writeBack.state);
                continue;
            }
            CandidateState cached = candidateStateCache.getIfPresent(key);
            if (cached != null) {
                requireTrackingMode(key, cached, trackingMode);
                result.put(key, cached);
                continue;
            }
            misses.add(key);
            missKeys.add(candidateKey(key.bucketId(), key.lhsCol(), key.rhsCol()));
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

        for (BucketMembershipCache cache : bucketMembershipCaches) {
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
        byte[] encoded = new byte[CANDIDATE_STATE_HEADER_BYTES + Integer.BYTES];
        encoded[0] = PRUNE_STATE_TYPE;
        putInt(encoded, CANDIDATE_STATE_HEADER_BYTES, state.witnessValueId());

        return encoded;
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
            Int2IntMap cached = bucketCache(bucketId).takeForUpdate(valueId);
            if (cached != null) {
                metrics.cacheHit();
                result.put(valueId, cached);
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

        ObjectIterator<Int2ObjectMap.Entry<Int2IntMap>> membershipIterator = Int2ObjectMaps
                .fastIterator(changedRecords);
        while (membershipIterator.hasNext()) {
            Int2ObjectMap.Entry<Int2IntMap> entry = membershipIterator.next();
            bucketCache(bucketId).putOwned(entry.getIntKey(), entry.getValue());
        }

        CandidateWriteBackCache writeBack = candidateWriteBack(bucketId);
        for (Map.Entry<CandidateKey, CandidateState> changed : changedCandidates.entrySet()) {
            CandidateKey key = changed.getKey();
            CandidateState state = changed.getValue();

            if (key.bucketId() != bucketId)
                throw new IllegalArgumentException("Candidate belongs to a different bucket: ");

            long pairKey = candidatePairKey(key);
            CandidateWriteBackEntry pending = writeBack.entries.get(pairKey);
            long newWeight = candidatePinnedWeight(state);
            if (pending == null) {
                pending = new CandidateWriteBackEntry(state);
                writeBack.entries.put(pairKey, pending);
                pinnedEstimatedBytes.addAndGet(newWeight);
            } else {
                long previousWeight = candidatePinnedWeight(pending.state);
                pending.state = state;
                pending.dirty = true;
                pinnedEstimatedBytes.addAndGet(newWeight - previousWeight);
            }
            writeBack.enqueueDirty(pairKey, pending);
        }
    }

    private static long candidatePinnedWeight(CandidateState state) {
        return 64L + candidateStateWeight(state) * 4L;
    }

    public long pinnedEstimatedBytes() {
        return pinnedEstimatedBytes.get();
    }

    public PreparedWriteBatch prepareWriteBatch(int bucketId, long batchId, int maximumEntries,
            long maximumBytes, ActorRef<ValueOwnerProtocol.Command> replyTo) {
        if (maximumEntries <= 0 || maximumBytes <= 0)
            throw new IllegalArgumentException("Write-batch limits must be positive");

        BucketMembershipCache cache = bucketCache(bucketId);
        int initialCapacity = Math.min(maximumEntries, 1_024);
        IntArrayList membershipIds = new IntArrayList(initialCapacity);
        List<byte[]> membershipValues = new ArrayList<>(initialCapacity);
        LongArrayList candidateKeys = new LongArrayList(initialCapacity);
        List<CandidateWrite> candidateWrites = new ArrayList<>(initialCapacity);
        long encodedBytes = 0L;

        int membershipsToInspect = cache.dirtyValueIds.size();
        while (membershipsToInspect-- > 0 && membershipIds.size() + candidateKeys.size() < maximumEntries
                && !cache.dirtyValueIds.isEmpty()) {
            int valueId = cache.dirtyValueIds.dequeueInt();
            MembershipCacheEntry entry = cache.peek(valueId);
            if (entry == null)
                continue;
            entry.queued = false;
            if (!entry.dirty)
                continue;
            if (entry.inFlight) {
                cache.enqueueDirty(valueId, entry);
                continue;
            }
            byte[] encoded = encode(entry.membership);
            long writeBytes = Integer.BYTES * 2L + encoded.length;
            if (encodedBytes > 0 && encodedBytes + writeBytes > maximumBytes) {
                cache.enqueueDirty(valueId, entry);
                break;
            }
            membershipIds.add(valueId);
            membershipValues.add(encoded);
            encodedBytes += writeBytes;
            entry.dirty = false;
            entry.inFlight = true;
        }

        CandidateWriteBackCache candidateCache = candidateWriteBack(bucketId);
        int candidatesToInspect = candidateCache.dirtyKeys.size();
        while (candidatesToInspect-- > 0 && membershipIds.size() + candidateKeys.size() < maximumEntries
                && !candidateCache.dirtyKeys.isEmpty()) {
            long pairKey = candidateCache.dirtyKeys.dequeueLong();
            CandidateWriteBackEntry entry = candidateCache.entries.get(pairKey);
            if (entry == null)
                continue;
            entry.queued = false;
            if (!entry.dirty)
                continue;
            if (entry.inFlight) {
                candidateCache.enqueueDirty(pairKey, entry);
                continue;
            }

            int lhsCol = candidateLhs(pairKey);
            int rhsCol = candidateRhs(pairKey);
            byte[] encodedKey = candidateKey(bucketId, lhsCol, rhsCol);
            byte[] encodedValue = entry.state.rejected() ? encodeCandidateState(entry.state) : null;

            long writeBytes = encodedKey.length + (encodedValue == null ? 0L : encodedValue.length);
            if (encodedBytes > 0 && encodedBytes + writeBytes > maximumBytes) {
                candidateCache.enqueueDirty(pairKey, entry);
                break;
            }
            candidateKeys.add(pairKey);
            candidateWrites.add(new CandidateWrite(encodedKey, encodedValue, encodedValue == null));
            encodedBytes += writeBytes;
            entry.dirty = false;
            entry.inFlight = true;
        }

        if (membershipIds.isEmpty() && candidateKeys.isEmpty())
            return PreparedWriteBatch.empty();

        int[] ids = membershipIds.toIntArray();
        long[] keys = candidateKeys.toLongArray();
        EncodedWriteBatch message = new EncodedWriteBatch(bucketId, batchId, ids,
                membershipValues.toArray(byte[][]::new), candidateWrites.toArray(CandidateWrite[]::new),
                encodedBytes, replyTo);
        return new PreparedWriteBatch(message, new InFlightWrite(batchId, ids, keys, encodedBytes));
    }

    public void acknowledgeWrite(int bucketId, InFlightWrite write) {
        BucketMembershipCache cache = bucketCache(bucketId);
        for (int valueId : write.membershipValueIds()) {
            MembershipCacheEntry entry = cache.peek(valueId);
            if (entry == null || !entry.inFlight)
                continue;
            entry.inFlight = false;
            if (entry.dirty)
                cache.enqueueDirty(valueId, entry);
            else {
                cache.overlayValueIds.remove(valueId);
                pinnedEstimatedBytes.addAndGet(-BucketMembershipCache.weight(entry.membership));
            }
        }
        CandidateWriteBackCache candidates = candidateWriteBack(bucketId);
        for (long pairKey : write.candidateKeys()) {
            CandidateWriteBackEntry entry = candidates.entries.get(pairKey);
            if (entry == null || !entry.inFlight)
                continue;
            entry.inFlight = false;
            if (entry.dirty) {
                candidates.enqueueDirty(pairKey, entry);
                continue;
            }
            pinnedEstimatedBytes.addAndGet(-candidatePinnedWeight(entry.state));
            CandidateKey key = candidateKeyObject(bucketId, pairKey);
            if (entry.state.rejected())
                candidateStateCache.put(key, entry.state);
            else
                candidateStateCache.invalidate(key);
            candidates.entries.remove(pairKey);
        }
        cache.evictCleanEntriesIfNecessary();
    }

    public void failWrite(int bucketId, InFlightWrite write) {
        BucketMembershipCache cache = bucketCache(bucketId);
        for (int valueId : write.membershipValueIds()) {
            MembershipCacheEntry entry = cache.peek(valueId);
            if (entry != null && entry.inFlight) {
                entry.inFlight = false;
                entry.dirty = true;
                cache.overlayValueIds.add(valueId);
                cache.enqueueDirty(valueId, entry);
            }
        }
        CandidateWriteBackCache candidates = candidateWriteBack(bucketId);
        for (long pairKey : write.candidateKeys()) {
            CandidateWriteBackEntry entry = candidates.entries.get(pairKey);
            if (entry != null && entry.inFlight) {
                entry.inFlight = false;
                entry.dirty = true;
                candidates.enqueueDirty(pairKey, entry);
            }
        }
    }

    public void writeEncodedBatch(EncodedWriteBatch encodedBatch) {
        long membershipWrites = encodedBatch.membershipValueIds().length;
        long candidateWrites = 0L;
        long candidateDeletes = 0L;
        try (WriteBatch batch = new WriteBatch()) {
            for (int index = 0; index < encodedBatch.membershipValueIds().length; index++)
                batch.put(key(encodedBatch.bucketId(), encodedBatch.membershipValueIds()[index]),
                        encodedBatch.membershipValues()[index]);
            for (CandidateWrite write : encodedBatch.candidateWrites()) {
                if (write.delete()) {
                    batch.delete(write.key());
                    candidateDeletes++;
                } else {
                    batch.put(write.key(), write.value());
                    candidateWrites++;
                }
            }
            long started = System.nanoTime();
            db.write(writeOptions, batch);
            metrics.rocksWrite(System.nanoTime() - started, encodedBatch.encodedBytes(), membershipWrites,
                    candidateWrites, candidateDeletes);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to write encoded VO batch", exception);
        }
    }

    public Long2ObjectMap<int[]> findWitnessesBatch(int bucketId, LongSet candidateKeys, int limit,
            Int2ObjectMap<Int2IntMap> updatedRecords) {
        if (limit <= 0 || limit > MAX_WITNESSES) {
            throw new IllegalArgumentException("Witness limit must be between 1 and " + MAX_WITNESSES);
        }

        Objects.requireNonNull(candidateKeys, "candidateKeys");
        Objects.requireNonNull(updatedRecords, "updatedRecords");

        Long2ObjectOpenHashMap<IntArrayList> collected = new Long2ObjectOpenHashMap<>(candidateKeys.size());
        LongOpenHashSet unfinished = new LongOpenHashSet(candidateKeys);
        for (long candidateKey : candidateKeys)
            collected.put(candidateKey, new IntArrayList(limit));

        if (unfinished.isEmpty())
            return new Long2ObjectOpenHashMap<>();

        IntSet seenValues = new IntOpenHashSet();
        byte[] prefix = bucketPrefix(bucketId);
        BucketMembershipCache cache = bucketCache(bucketId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (!unfinished.isEmpty() && iterator.isValid() && hasPrefix(iterator.key(), prefix)) {
                byte[] storedKey = iterator.key();
                if (storedKey.length != Integer.BYTES * 2) {
                    iterator.next();
                    continue;
                }

                // int valueId = ByteBuffer.wrap(storedKey).getInt(Integer.BYTES);
                int valueId = readInt(storedKey, Integer.BYTES);
                seenValues.add(valueId);
                Int2IntMap membership = updatedRecords.get(valueId);
                MembershipCacheEntry cached = cache.peek(valueId);
                if (membership == null && cached != null && cached.mutated())
                    membership = cached.membership;
                if (membership == null)
                    membership = decode(iterator.value());

                collectWitnesses(valueId, membership, unfinished, collected, limit);
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException(
                    "Unable to find witnesses for bucket=" + bucketId,
                    exception);
        }

        if (!unfinished.isEmpty()) {
            ObjectIterator<Int2ObjectMap.Entry<Int2IntMap>> iterator = Int2ObjectMaps.fastIterator(updatedRecords);
            while (!unfinished.isEmpty() && iterator.hasNext()) {
                Int2ObjectMap.Entry<Int2IntMap> entry = iterator.next();
                int valueId = entry.getIntKey();
                if (!seenValues.add(valueId))
                    continue;
                collectWitnesses(valueId, entry.getValue(), unfinished, collected, limit);
            }
        }

        if (!unfinished.isEmpty()) {
            for (int valueId : cache.overlayValueIds) {
                if (!seenValues.add(valueId))
                    continue;
                MembershipCacheEntry entry = cache.peek(valueId);
                if (entry != null && entry.mutated())
                    collectWitnesses(valueId, entry.membership, unfinished, collected, limit);
                if (unfinished.isEmpty())
                    break;
            }
        }

        Long2ObjectOpenHashMap<int[]> result = new Long2ObjectOpenHashMap<>(collected.size());
        ObjectIterator<Long2ObjectMap.Entry<IntArrayList>> iterator = Long2ObjectMaps.fastIterator(collected);
        while (iterator.hasNext()) {
            Long2ObjectMap.Entry<IntArrayList> entry = iterator.next();
            result.put(entry.getLongKey(), entry.getValue().toIntArray());
        }
        return result;
    }

    private static void collectWitnesses(int valueId, Int2IntMap membership, LongSet unfinished,
            Long2ObjectMap<IntArrayList> collected, int limit) {
        LongIterator iterator = unfinished.iterator();
        while (iterator.hasNext()) {
            long candidateKey = iterator.nextLong();
            if (!isViolation(membership, candidateLhs(candidateKey), candidateRhs(candidateKey)))
                continue;
            IntArrayList witnesses = collected.get(candidateKey);
            witnesses.add(valueId);
            if (witnesses.size() == limit)
                iterator.remove();
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static byte[] bucketPrefix(int bucketId) {
        byte[] prefix = new byte[Integer.BYTES];
        putInt(prefix, 0, bucketId);
        return prefix;
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

    private static byte[] encode(Int2IntMap columns) {
        int size = columns.size();
        int encodedLength = Math.addExact(Integer.BYTES, Math.multiplyExact(size, 2 * Integer.BYTES));
        byte[] encoded = new byte[encodedLength];
        putInt(encoded, 0, size);
        int offset = Integer.BYTES;

        ObjectIterator<Int2IntMap.Entry> iterator = Int2IntMaps.fastIterator(columns);
        while (iterator.hasNext()) {
            Int2IntMap.Entry entry = iterator.next();

            putInt(encoded, offset, entry.getIntKey());
            offset += Integer.BYTES;

            putInt(encoded, offset, entry.getIntValue());
            offset += Integer.BYTES;
        }

        return encoded;
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

    private static long candidatePairKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }

    private static long candidatePairKey(CandidateKey key) {
        return candidatePairKey(key.lhsCol(), key.rhsCol());
    }

    private static int candidateLhs(long pairKey) {
        return (int) (pairKey >>> Integer.SIZE);
    }

    private static int candidateRhs(long pairKey) {
        return (int) pairKey;
    }

    private static CandidateKey candidateKeyObject(int bucketId, long pairKey) {
        return new CandidateKey(bucketId, candidateLhs(pairKey), candidateRhs(pairKey));
    }

    private static byte[] candidateKey(int bucketId, int lhsCol, int rhsCol) {
        byte[] encoded = new byte[1 + Integer.BYTES * 3];
        encoded[0] = CANDIDATE_PREFIX;
        putInt(encoded, 1, bucketId);
        putInt(encoded, 1 + Integer.BYTES, lhsCol);
        putInt(encoded, 1 + Integer.BYTES * 2, rhsCol);
        return encoded;
    }

    @Override
    public void close() {
        candidateStateCache.invalidateAll();

        for (BucketMembershipCache cache : bucketMembershipCaches) {
            if (cache != null)
                cache.clear();
        }
        db.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
