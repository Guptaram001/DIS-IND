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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import disIND.valueBased.model.SharedModel.CandidateTrackingMode;
import disIND.valueBased.utility.UserConfig;

/**
 * Worker-local, disk-backed membership state shared by all ValueOwner
 */
public final class ValueOwnerMembershipStore implements AutoCloseable {
    private static final float BLOOM_FILTER_BITS_PER_KEY = UserConfig.BLOOM_FILTER_BITS_PER_KEY;

    private record CacheKey(int bucketId, int valueId) {}
    public record MembershipWrite(int bucketId, Map<Integer, Map<Integer, Integer>> changedRecords) {}

    static {
        RocksDB.loadLibrary();
    }

    private final BloomFilter bloomFilter;
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;
    private final Cache<CacheKey, Map<Integer, Integer>> hotCache;

    private static final byte CANDIDATE_PREFIX = 0x43;
    private static final byte COUNT_STATE_TYPE = 1;
    private static final byte WITNESS_STATE_TYPE = 2;
    private static final byte PRUNE_STATE_TYPE = 3;
    private static final int CANDIDATE_STATE_HEADER_BYTES = Byte.BYTES;

    public static final int MAX_WITNESSES = UserConfig.MAX_VALUE_OWNER_WITNESSES;
    private static final int CANDIDATE_CACHE_BASE_WEIGHT = 10;

    public record CandidateKey(int bucketId,int lhsCol,int rhsCol) {}
    private final Cache<CandidateKey, CandidateState> hotRejectedCandidates;
    public sealed interface CandidateState permits CountState, WitnessState, PruneState {
        boolean rejected();
        CandidateTrackingMode trackingMode();
    }

    public record CountState(int violationCount)implements CandidateState {
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

        public PruneState {
            if (witnessValueId < CLUSTER_PROOF)
                throw new IllegalArgumentException("Invalid prune proof value: "+ witnessValueId);
        }

        public static PruneState valid() {
            return new PruneState(NO_WITNESS);
        }

        public static PruneState rejected(
            int witnessValueId) {
            if (witnessValueId < 0) 
                throw new IllegalArgumentException("Witness value ID must be non-negative");
            return new PruneState(witnessValueId);
        }

        public static PruneState rejectedByCardinality() {
            return new PruneState(CARDINALITY_PROOF);
        }

        public static PruneState rejectedByCluster() {
            return new PruneState(CLUSTER_PROOF);
        }

        public boolean hasWitness() {
            return witnessValueId >= 0;
        }

        public boolean hasCardinalityProof() {
            return witnessValueId == CARDINALITY_PROOF;
        }

        public boolean hasClusterProof() {
            return witnessValueId == CLUSTER_PROOF;
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

    public record WitnessState(List<Integer> witnesses) implements CandidateState {

        public WitnessState {
            Objects.requireNonNull(witnesses, "witnesses");
            LinkedHashSet<Integer> unique = new LinkedHashSet<>();
            for (Integer witness : witnesses) {
                unique.add(Objects.requireNonNull(witness, "witness"));
                if (unique.size() == MAX_WITNESSES)
                    break;
            }
            witnesses = List.copyOf(unique);
        }

        @Override
        public boolean rejected() {
            return !witnesses.isEmpty();
        }

        @Override
        public CandidateTrackingMode trackingMode() {
            return CandidateTrackingMode.WITNESS;
        }
    }

    public ValueOwnerMembershipStore(Path directory, long hotEntries) {
        this(directory, hotEntries, CandidateTrackingMode.COUNT);
    }

    public ValueOwnerMembershipStore(Path directory, long hotEntries,CandidateTrackingMode trackingMode) {
        try {
            Files.createDirectories(directory);
            this.bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setFilterPolicy(bloomFilter)
                    .setWholeKeyFiltering(true);
            this.options = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig);
            this.writeOptions = new WriteOptions();
            if (trackingMode == CandidateTrackingMode.WITNESS)
                hotRejectedCandidates = CacheBuilder.newBuilder()
                    .maximumWeight(Math.multiplyExact(hotEntries, CANDIDATE_CACHE_BASE_WEIGHT))
                    .weigher((CandidateKey ignored, CandidateState state) -> candidateStateWeight(state))
                    .build();
            else
                 hotRejectedCandidates =CacheBuilder.newBuilder().maximumSize(hotEntries).build();
            this.db = RocksDB.open(options, directory.toString());
            this.hotCache = CacheBuilder.newBuilder().maximumSize(hotEntries).build();
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("Unable to open ValueOwner membership store at " + directory, exception);
        }
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

        if (state instanceof WitnessState witnessState) {
            int count = witnessState.witnesses().size();
            ByteBuffer buffer = ByteBuffer.allocate(CANDIDATE_STATE_HEADER_BYTES+ Integer.BYTES+ count * Integer.BYTES)
                    .put(WITNESS_STATE_TYPE)
                    .putInt(count);
            for (int valueId : witnessState.witnesses())
                buffer.putInt(valueId);
            return buffer.array();
        }
        throw new IllegalArgumentException("Unsupported candidate state: "+ state.getClass().getName());
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
                throw invalidCandidateState("invalid prune witness "+ witnessValueId);
        
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
            throw invalidCandidateState( "invalid length for " + count + " witnesses");

        List<Integer> witnesses = new ArrayList<>(count);
        for (int index = 0; index < count; index++)
            witnesses.add(buffer.getInt());
        return new WitnessState(witnesses);
    }

    public Map<CandidateKey, Integer>findOneWitnessPerCandidate(int bucketId,Set<CandidateKey> candidates,
                Map<Integer, Map<Integer, Integer>>updatedRecords) {

        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(updatedRecords,"updatedRecords");

        if (candidates.isEmpty()) 
            return Map.of();

        Map<Integer, List<CandidateKey>> candidatesByLhs =new HashMap<>();
        for (CandidateKey candidate : candidates) {
            if (candidate.bucketId() != bucketId) 
                throw new IllegalArgumentException("Candidate belongs to another bucket: "+ candidate);
            candidatesByLhs.computeIfAbsent(candidate.lhsCol(),ignored -> new ArrayList<>()).add(candidate);
        }

        Set<CandidateKey> unresolved =new HashSet<>(candidates);
        Map<CandidateKey, Integer> witnesses = new HashMap<>();
        Set<Integer> seenUpdatedValues =new HashSet<>();

        byte[] prefix =bucketPrefix(bucketId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid()&& hasPrefix(iterator.key(), prefix) && !unresolved.isEmpty()) {
                byte[] storedKey = iterator.key();
                if (storedKey.length!= Integer.BYTES * 2) {
                    iterator.next();
                    continue;
                }
                int valueId =ByteBuffer.wrap(storedKey).getInt(Integer.BYTES);
                Map<Integer, Integer> membership =updatedRecords.get(valueId);
                if (membership != null) 
                    seenUpdatedValues.add(valueId);
                else
                    membership =decode(iterator.value());

                findWitnessesInMembership(valueId,membership,candidatesByLhs,unresolved,witnesses);
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to scan unresolved candidates "+ "for bucket " + bucketId,
                    exception);
        }

        if (!unresolved.isEmpty()) {
            for (Map.Entry<Integer, Map<Integer, Integer>> entry : updatedRecords.entrySet()) {
                if (seenUpdatedValues.contains(entry.getKey())) 
                    continue;
                findWitnessesInMembership(entry.getKey(),entry.getValue(),candidatesByLhs,
                    unresolved,witnesses);
                if (unresolved.isEmpty()) 
                    break;
            }
        }
        return Map.copyOf(witnesses);
    }

    public Map<CandidateKey, Integer> verifyCandidateWitnesses(int bucketId,Map<CandidateKey, Integer> proposals,
                Map<Integer, Map<Integer, Integer>> updatedRecords) {
            Objects.requireNonNull(proposals, "proposals");
            Objects.requireNonNull(updatedRecords, "updatedRecords");
            if (proposals.isEmpty())
                return Map.of();

            Set<Integer> diskValueIds = new HashSet<>();
            for (int valueId : proposals.values()) {
                if (!updatedRecords.containsKey(valueId))
                    diskValueIds.add(valueId);
            }

            Map<Integer, Map<Integer, Integer>> storedRecords =loadBatch(bucketId, diskValueIds);
            Map<CandidateKey, Integer> verified = new HashMap<>();

            proposals.forEach((candidate, valueId) -> {
                if (candidate.bucketId() != bucketId)
                    throw new IllegalArgumentException("Candidate belongs to another bucket: " + candidate);

                Map<Integer, Integer> membership = updatedRecords.get(valueId);
                if (membership == null)
                    membership = storedRecords.get(valueId);

                if (membership != null && membership.containsKey(candidate.lhsCol()) &&
                        !membership.containsKey(candidate.rhsCol())) 
                    verified.put(candidate, valueId);
            });

            return Map.copyOf(verified);
        }

    private static void findWitnessesInMembership(int valueId,Map<Integer, Integer> membership,
            Map<Integer, List<CandidateKey>> candidatesByLhs,Set<CandidateKey> unresolved,
            Map<CandidateKey, Integer> witnesses) {

        for (Integer lhsCol : membership.keySet()) {
            List<CandidateKey> candidates =candidatesByLhs.get(lhsCol);

            if (candidates == null) 
                continue;
        
            for (CandidateKey candidate : candidates) {
                if (!unresolved.contains(candidate)) 
                    continue;
                
                if (!membership.containsKey(candidate.rhsCol())) {
                    witnesses.put(candidate, valueId);
                    unresolved.remove(candidate);
                }
            }
        }
    }

    private static byte[] candidateBucketPrefix(int bucketId) {
        return ByteBuffer.allocate(Byte.BYTES + Integer.BYTES)
                .put(CANDIDATE_PREFIX)
                .putInt(bucketId)
                .array();
    }

    private static CandidateKey decodeCandidateKey(byte[] encoded) {
        int expectedLength =Byte.BYTES + Integer.BYTES * 3;
        if (encoded.length != expectedLength) {
            throw new IllegalStateException( "Invalid candidate key length: "+ encoded.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        byte prefix = buffer.get();
        if (prefix != CANDIDATE_PREFIX) {
            throw new IllegalStateException( "Invalid candidate key prefix: "+ prefix);
        }

        return new CandidateKey( buffer.getInt(), buffer.getInt(),buffer.getInt());
    }

    public Set<CandidateKey> loadRejectedCandidates(int bucketId,CandidateTrackingMode trackingMode) {

        Objects.requireNonNull(trackingMode,"trackingMode");
        Set<CandidateKey> rejected =new HashSet<>();
        byte[] prefix =candidateBucketPrefix(bucketId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid()&& hasPrefix(iterator.key(), prefix)) {
                CandidateKey key =decodeCandidateKey(iterator.key());
                CandidateState state =decodeCandidateState(iterator.value());
                requireTrackingMode(key,state,trackingMode);

                if (state.rejected()) {
                    rejected.add(key);
                    hotRejectedCandidates.put(key, state);
                }
                iterator.next();
            }
            iterator.status();
            return Set.copyOf(rejected);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to restore rejected candidates for bucket " + bucketId);
        }
    }

    public Map<CandidateKey, CandidateState> loadCandidates(Set<CandidateKey> keys, CandidateTrackingMode trackingMode) {
        Objects.requireNonNull(trackingMode, "trackingMode"); //Throws NPE 

        Map<CandidateKey, CandidateState> result =new HashMap<>(keys.size());
        List<CandidateKey> misses = new ArrayList<>();
        List<byte[]> missKeys = new ArrayList<>();

        for (CandidateKey key : keys) {
            CandidateState cached =hotRejectedCandidates.getIfPresent(key);
            if (cached != null) {
                requireTrackingMode(key, cached, trackingMode);
                result.put(key, cached);
            } else {
                misses.add(key);
                missKeys.add(candidateKey(key));
            }
        }

        if (missKeys.isEmpty())
            return result;

        try {
            List<byte[]> encodedStates = db.multiGetAsList(missKeys);

            for (int index = 0; index < misses.size(); index++) {
                CandidateKey key = misses.get(index);
                byte[] encoded = encodedStates.get(index);
                //encoded = null represent the key is valid and not therefore in store.
                CandidateState state = encoded == null ? validCandidateState(trackingMode): decodeCandidateState(encoded);
                requireTrackingMode(key, state, trackingMode);
                result.put(key, state);
                if (state.rejected())
                    hotRejectedCandidates.put(key, state);
            }
            return result;
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to load candidate states", exception);
        }
    }

    private static CandidateState validCandidateState(CandidateTrackingMode trackingMode) {
        return switch (trackingMode) {
            case COUNT -> new CountState(0);
            case WITNESS -> new WitnessState(List.of());
            case PRUNE -> PruneState.valid();
        };
    }

    private static byte[] encodePruneState(PruneState state) {
        return ByteBuffer.allocate(
                    CANDIDATE_STATE_HEADER_BYTES+ Integer.BYTES)
            .put(PRUNE_STATE_TYPE)
            .putInt(state.witnessValueId())
            .array();
    }

    private static void requireTrackingMode(CandidateKey key, CandidateState state, CandidateTrackingMode expected) {
        if (state.trackingMode() != expected) {
            throw new IllegalStateException("Candidate " + key + " was stored using "+ state.trackingMode() +
             " tracking but this run uses " + expected );
        }
    }

    private static int candidateStateWeight(CandidateState state) {
        if (state instanceof WitnessState witnessState) {
            return Math.max(CANDIDATE_CACHE_BASE_WEIGHT, witnessState.witnesses().size());
        }
        return CANDIDATE_CACHE_BASE_WEIGHT;
    }

    private static IllegalStateException invalidCandidateState(String detail) {
        return new IllegalStateException("Invalid persisted candidate state "+detail);
    }


    public Map<Integer, Map<Integer, Integer>> loadBatch(int bucketId, Set<Integer> valueIds) {
        Map<Integer, Map<Integer, Integer>> result = new HashMap<>(valueIds.size());
        List<Integer> misses = new ArrayList<>();
        List<byte[]> missKeys = new ArrayList<>();

        for (int valueId : valueIds) {
            Map<Integer, Integer> cached = hotCache.getIfPresent(new CacheKey(bucketId, valueId));
            if (cached == null) {
                misses.add(valueId);
                missKeys.add(key(bucketId, valueId));
            } else {
                result.put(valueId, new HashMap<>(cached));
            }
        }

        if (!missKeys.isEmpty()) {
            try {
                List<byte[]> encoded = db.multiGetAsList(missKeys);
                for (int index = 0; index < misses.size(); index++) {
                    int valueId = misses.get(index);
                    Map<Integer, Integer> record = encoded.get(index) == null
                            ? new HashMap<>()
                            : decode(encoded.get(index));
                    hotCache.put(new CacheKey(bucketId, valueId), Map.copyOf(record));
                    result.put(valueId, record);
                }
            } catch (RocksDBException exception) {
                throw new IllegalStateException("Unable to batch-load VO bucket " + bucketId, exception);
            }
        }
        return result;
    }


    public void writeBatch(int bucketId,Map<Integer, Map<Integer, Integer>> changedRecords,
        Map<CandidateKey, CandidateState> changedCandidates) {

        try (WriteBatch batch = new WriteBatch()) {
            // Membership updates
            for (var entry : changedRecords.entrySet()) {
                batch.put(key(bucketId, entry.getKey()),encode(entry.getValue()));
            }

            // Candidate updates
            for (var entry : changedCandidates.entrySet()) {
                CandidateKey key = entry.getKey();
                CandidateState state = entry.getValue();
                if (state.rejected()) {
                    batch.put(
                            candidateKey(key),
                            encodeCandidateState(state));
                } else {
                    batch.delete(candidateKey(key));
                }
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to persist VO batch", exception);
        }

        // Update caches only after RocksDB succeeds.
        changedRecords.forEach((valueId, record) ->hotCache.put(new CacheKey(bucketId, valueId),Map.copyOf(record)));
        changedCandidates.forEach((key, state) -> {
            if (state.rejected())
                hotRejectedCandidates.put(key, state);
            else
                hotRejectedCandidates.invalidate(key);
        });
    }


    
    public List<Integer> findWitnesses(int bucketId,int lhsCol,int rhsCol,int limit,
            Map<Integer, Map<Integer, Integer>> updatedRecords) {
        if (limit <= 0 || limit > MAX_WITNESSES) {
            throw new IllegalArgumentException("Witness limit must be between 1 and " + MAX_WITNESSES);
        }

        List<Integer> witnesses = new ArrayList<>(limit);
        Set<Integer> seenUpdatedValues = new HashSet<>();
        byte[] prefix = bucketPrefix(bucketId);

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && hasPrefix(iterator.key(), prefix)) {
                byte[] storedKey = iterator.key();
                if (storedKey.length != Integer.BYTES * 2) {
                    iterator.next();
                    continue;
                }

                int valueId = ByteBuffer.wrap(storedKey).getInt(Integer.BYTES);
                Map<Integer, Integer> membership = updatedRecords.get(valueId);
                if (membership != null) 
                    seenUpdatedValues.add(valueId);
                else 
                    membership = decode(iterator.value());

                if (isViolation(membership, lhsCol, rhsCol)) {
                    witnesses.add(valueId);
                    if (witnesses.size() == limit)
                        return List.copyOf(witnesses);
                }
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("Unable to find witnesses for bucket=" + bucketId+ " candidate=" + lhsCol + "⊆" + rhsCol,exception);
        }

        // New values in this StoreBatch are not in RocksDB yet.
        for (Map.Entry<Integer, Map<Integer, Integer>> entry : updatedRecords.entrySet()) {
            if (seenUpdatedValues.contains(entry.getKey()))
                continue;
            if (isViolation(entry.getValue(), lhsCol, rhsCol)) {
                witnesses.add(entry.getKey());
                if (witnesses.size() == limit)
                    break;
            }
        }
        return List.copyOf(witnesses);
    }

    public Map<Integer, Map<Integer, Integer>> snapshotBucket(int bucketId) {
        Map<Integer, Map<Integer, Integer>> snapshot = new LinkedHashMap<>();
        byte[] prefix = bucketPrefix(bucketId);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid() && hasPrefix(iterator.key(), prefix)) {
                int valueId = ByteBuffer.wrap(iterator.key()).getInt(Integer.BYTES);
                snapshot.put(valueId, decode(iterator.value()));
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException(
                    "Unable to read VO bucket snapshot " + bucketId, exception);
        }
        return snapshot;
    }

    private static byte[] bucketPrefix(int bucketId) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(bucketId).array();
    }

    private static byte[] key(int bucketId, int valueId) {
        return ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt(bucketId)
                .putInt(valueId)
                .array();
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

    private static boolean isViolation(Map<Integer, Integer> membership, int lhsCol, int rhsCol) {
        return membership.containsKey(lhsCol) && !membership.containsKey(rhsCol);
    }

    private static byte[] encode(Map<Integer, Integer> columns) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    Integer.BYTES + columns.size() * (Integer.BYTES + Integer.BYTES));
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(columns.size());
                columns.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> {
                            try {
                                output.writeInt(entry.getKey());
                                output.writeInt(entry.getValue());
                            } catch (IOException exception) {
                                throw new EncodingException(exception);
                            }
                        });
            }
            return bytes.toByteArray();
        } catch (IOException | EncodingException exception) {
            Throwable cause = exception instanceof EncodingException ? exception.getCause() : exception;
            throw new IllegalStateException("Unable to encode VO membership", cause);
        }
    }

    private static Map<Integer, Integer> decode(byte[] encoded) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
            int size = input.readInt();
            Map<Integer, Integer> columns = new LinkedHashMap<>(size);
            for (int index = 0; index < size; index++)
                columns.put(input.readInt(), input.readInt());
            return columns;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to decode VO membership", exception);
        }
    }

    private static final class EncodingException extends RuntimeException {
        private EncodingException(IOException cause) {
            super(cause);
        }
    }

    @Override
    public void close() {
        hotCache.invalidateAll();
        hotRejectedCandidates.invalidateAll();
        db.close();
        writeOptions.close();
        options.close();
        bloomFilter.close();
    }
}
