package disIND.streamBasedShardedDispatcher.structures;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared atomic watermark register.
 *
 * Replaces O(N) broadcast to all AttributeActor shards after every
 * ViolationReport. CandidateManagerActor writes via setBinary/setNary.
 * AttributeActors poll readBinary/readNary every GC_INTERVAL epochs.
 *
 * For a real multi-node cluster: back with Pekko Distributed Data LWWRegister
 * so all nodes see advances without explicit messaging.
 */
public final class WatermarkRegister {

    private final AtomicLong binaryWm = new AtomicLong(0L);
    private final AtomicLong naryWm   = new AtomicLong(0L);

    public void setBinary(long wm) { binaryWm.updateAndGet(cur -> Math.max(cur, wm)); }
    public void setNary(long wm)   { naryWm.updateAndGet(cur -> Math.max(cur, wm));   }
    public long readBinary()       { return binaryWm.get(); }
    public long readNary()         { return naryWm.get();   }
}
