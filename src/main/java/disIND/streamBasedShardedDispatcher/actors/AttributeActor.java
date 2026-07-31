package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.typed.Cluster;
import disIND.streamBasedShardedDispatcher.model.SharedModel.*;
import disIND.streamBasedShardedDispatcher.monitor.StatsCommand;
import disIND.streamBasedShardedDispatcher.structures.*;
import disIND.streamBasedShardedDispatcher.utility.Debug;
import org.roaringbitmap.RoaringBitmap;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static disIND.streamBasedShardedDispatcher.utility.Debug.formLog;

public class AttributeActor extends AbstractBehavior<AACommand> {
    public static final EntityTypeKey<AACommand> TYPE_KEY = EntityTypeKey.create(AACommand.class, "AttributeActor");
    private final ActorRef<StatsCommand> statsRef;
    private final ClusterSharding sharding;
    private final DatasetMetadata metadata;
    private final int ownerTableId;
    private final NodeValueToRowsStore nodeValueToRowsStore;

    private final int colId;
    private final AtomicReference<ActorRef<CMCommand>> cmRef;
    private final Map<UnaryPair, EntityRef<CMCommand>> lhsSubscriptions = new HashMap<>();
    private final Map<UnaryPair, EntityRef<CMCommand>> rhsSubscriptions = new HashMap<>();

    private final BitmapStore bitmapStore=new BitmapStore();
    private final ValueToRowsStore valueToRowsDelta = new ValueToRowsStore();
    private final SketchStore sketchStore = new SketchStore();
    private long epochsProcessed = 0L;

    private AttributeCheckPoint checkPoint ;
    private final Deque<AttributeDelta> deltaLogDequeue = new ArrayDeque<>();
    private final Map<Integer, NavigableSet<Integer>> receivedBatchIdsByTable = new HashMap<>();
    private final Set<String> appliedBatchKeys = new HashSet<>();

    private static String key(int tableId, int batchId) {
        return tableId + ":" + batchId;
    }

    public static Behavior<AACommand> create(int colId,AtomicReference<ActorRef<CMCommand>> cmRef,
        ActorRef<StatsCommand> statsRef,DatasetMetadata metadata, NodeValueToRowsStore nodeValueToRowsStore) {
        return Behaviors.setup(ctx -> new AttributeActor(ctx, colId, cmRef,statsRef,metadata,nodeValueToRowsStore));
    }

    private AttributeActor(ActorContext<AACommand> ctx, int colId,
                           AtomicReference<ActorRef<CMCommand>> cmRef,ActorRef<StatsCommand> statsRef,
                           DatasetMetadata metadata, NodeValueToRowsStore nodeValueToRowsStore) {
        super(ctx);
        this.colId  = colId;
        this.sharding = ClusterSharding.get(ctx.getSystem());
        this.cmRef= cmRef;
        this.statsRef= statsRef;
        this.metadata=metadata;
        this.nodeValueToRowsStore = nodeValueToRowsStore;
        this.ownerTableId = findOwnerTable(colId, metadata);
        getContext().getLog().info("[PLACEMENT] type=AA col={} node={}",
                colId, Cluster.get(ctx.getSystem()).selfMember().address());
        if(Debug.STATE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                    "PLACEMENT type=AA col={} node={}",
                    colId, Cluster.get(ctx.getSystem()).selfMember().address());
       
    }


    @Override
    public Receive<AACommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(AACommand.InsertBatch.class,this::onInsertBatch)
                .onMessage(AACommand.EmitSketch.class,this::onEmitSketch)
                .onMessage(AACommand.CheckPoint.class,this::onCheckPoint)
                .onMessage(AACommand.SendColumnData.class,this::onSendColumnData)
                .onMessage(AACommand.CompareBitmap.class,this::onCompareBitmap)
                .onMessage(AACommand.CheckMembership.class,this::onCheckMembership)
                .onMessage(AACommand.DeactiveUnaryPair.class,this::onDeactivePair)
                .onMessage(AACommand.RequestSketch.class, this::onRequestSketch)
                .build();
    }

    private Behavior<AACommand> onDeactivePair(AACommand.DeactiveUnaryPair msg) {
        if (msg.lhsSide()) {
            lhsSubscriptions.remove(msg.pair());
        } else {
            rhsSubscriptions.remove(msg.pair());
        }
        return this;
    }

    private final Map<Integer, SketchSummary> cleanCheckpointSketches = new HashMap<>();
    private final Map<Integer, List<ActorRef<AppraiserCommand>>> pendingSketchRequests = new HashMap<>();

    private Behavior<AACommand> onRequestSketch(AACommand.RequestSketch msg) {
        SketchSummary summary = cleanCheckpointSketches.get(msg.round());
        if (summary != null) {
            msg.replyTo().tell(new AppraiserCommand.SketchArrived(summary));
        } else {
            if(Debug.STATE)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.STATE), Debug.attr(),colId,"-",
                        String.valueOf(Debug.State.NONE),
                        "Requesting sketch for round {} col {} epoch {}", msg.round(), colId, msg.epoch());
            pendingSketchRequests.computeIfAbsent(msg.round(), r -> new ArrayList<>()).add(msg.replyTo());
        }
        return this;
    }

    private Behavior<AACommand> onCheckMembership(AACommand.CheckMembership msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                    "Checking membership for col {} epoch {}", colId, msg.round());
        RoaringBitmap missing = msg.values().clone();
        bitmapStore.removeContainedFrom(missing);
        sharding.entityRefFor(CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(msg.replyOwnerCol()))
                .tell(new CMCommand.MembershipResult(msg.pair(), msg.round(), missing));
        return this;
    }

    private Behavior<AACommand> onSendColumnData(AACommand.SendColumnData msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),colId,Debug.pairTag(msg.candidate().pair()),
                    String.valueOf(Debug.State.NONE),
                    "Invoked for sending column col:{} to col: {}", msg.candidate().pair().lhsCol(), msg.candidate().pair().rhsCol());
        RoaringBitmap lhsBitmap = checkPoint.bitmapStore().getBitmap();
        int checkpointRound = checkPoint.round();
        EntityRef<CMCommand> cmRef = sharding.entityRefFor(
                CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(msg.candidate().pair().lhsCol()));
        EntityRef<AACommand> rhsRef = sharding.entityRefFor(
                AttributeActor.TYPE_KEY, AACommand.entityId(msg.candidate().pair().rhsCol()));
        //lhsSubscribers.add(sendColumnData.cmRef());
        rhsRef.tell(new AACommand.CompareBitmap(msg.candidate(), lhsBitmap));
        RoaringBitmap mergedSinceCheckpoint = accumulateDistinctSince(checkpointRound);
        cmRef.tell(new CMCommand.LhsReplayDelta(msg.candidate().pair(), colId, msg.candidate().checkpointRound(),
                msg.candidate().checkpointRound(), mergedSinceCheckpoint));
        lhsSubscriptions.put(msg.candidate().pair(), cmRef);
        return this;
    }

    private Behavior<AACommand> onCompareBitmap(AACommand.CompareBitmap msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),colId,Debug.pairTag(msg.candidate().pair()), String.valueOf(Debug.State.NONE),
                    "Comparing {} ⊆ {}", msg.candidate().pair().lhsCol(), msg.candidate().pair().rhsCol());
        int checkPointRound=checkPoint.round();
        ScanResult result = checkPoint.bitmapStore().compareAgainst(
                                msg.lhsBitmap(),
                                msg.candidate().pair(),
                                msg.candidate().checkpointRound());
        EntityRef<CMCommand> cmRef = sharding.entityRefFor(
                CandidateManagerActor_.TYPE_KEY, CMCommand.entityId(msg.candidate().pair().lhsCol()));
        cmRef.tell(new CMCommand.UnaryViolationReport(result));
        RoaringBitmap mergedSinceCheckpoint = accumulateDistinctSince(checkPointRound);
        cmRef.tell(new CMCommand.RhsReplayDelta(msg.candidate().pair(), colId, msg.candidate().checkpointRound(),
                msg.candidate().checkpointRound(), mergedSinceCheckpoint));
        rhsSubscriptions.put(msg.candidate().pair(), cmRef);
        return this;
    }

    private RoaringBitmap accumulateDistinctSince(int checkpointRound) {
        RoaringBitmap accumulated = new RoaringBitmap();
        for (AttributeDelta delta : deltaLogDequeue) {
            if (delta.round() <= checkpointRound)
                continue;
            accumulated.or(distinctValuesFromDelta(delta));
        }
        return accumulated;
    }

    private Behavior<AACommand> onCheckPoint(AACommand.CheckPoint msg) {
        if(Debug.CHECKPOINT)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.CHECKPOINT), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                    "Received Checkpoint Message for round {}  for col {} epoch {}", msg.round(), msg.colId(), msg.epoch());

        List<InputBatchDetails> missing = findMissingForCheckpoint(msg.round(), msg.maxBatchIdByTable());
        if (!missing.isEmpty()) {
            msg.replyTo().tell(new BDCommand.AaCheckpointStatus(msg.round(), colId, false, missing));
            //Can be optimized to send once
            for (InputBatchDetails m : missing) {
                msg.replyTo().tell(new BDCommand.MissingBatchRequest(m.tableId(), m.batchId(), colId));
            }
            return this;
        }

        nodeValueToRowsStore.mergeCheckpoint(colId, msg.round(), valueToRowsDelta);
        valueToRowsDelta.clear();
        checkPoint = new AttributeCheckPoint(msg.round(), bitmapStore.deepCopy(),
            sketchStore.getSummary(msg.round(),colId, msg.epoch()));

        msg.replyTo().tell(new BDCommand.AaCheckpointStatus(msg.round(), colId, true, List.of()));

        cleanupOldDeltas(msg.round());
        publishCheckpointSketch(msg.round(), msg.epoch(), msg.appraiserRef());
        return this;
    }

    private List<InputBatchDetails> findMissingForCheckpoint(int round, Map<Integer, Integer> maxBatchIdByTable) {
        Integer maxBatchId = maxBatchIdByTable.get(ownerTableId);
        if (maxBatchId == null) return List.of();
        NavigableSet<Integer> received = receivedBatchIdsByTable.getOrDefault(ownerTableId, new TreeSet<>());
        List<InputBatchDetails> missing = new ArrayList<>();
        for (int b = 0; b <= maxBatchId; b++) {
            if (!received.contains(b)) {
                missing.add(new InputBatchDetails(ownerTableId, -1, b, -1, round, colId));
            }
        }

        return missing;
    }

    private Behavior<AACommand> onEmitSketch(AACommand.EmitSketch msg) {
        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                    "Emitting sketch for col {} epoch {} round {}", colId, msg.epoch(), msg.round());
        msg.replyTo().tell(new AppraiserCommand.SketchArrived(sketchStore.getSummary(msg.round(),colId, msg.epoch())));
        return this;
    }

    private Behavior<AACommand> onInsertBatch(AACommand.InsertBatch msg) {

        InputBatchDetails ibd = msg.inputBatchDetails();
        String batchKey = key(ibd.tableId(), ibd.batchId());
        if (appliedBatchKeys.contains(batchKey)) {
            if (msg.ackTo() != null) {
                msg.ackTo().tell(new BDCommand.BatchFlushed(ibd, colId));
            }
            return this;
        }

        if(Debug.MESSAGE)
            formLog(getContext().getLog(), String.valueOf(Debug.LogType.MESSAGE), Debug.attr(),colId,"-",
                    String.valueOf(Debug.State.NONE),
                    "Insert Batch received epoch: {}, batchID:{} rows:{} round {}",msg.inputBatchDetails().epoch(),
                    msg.inputBatchDetails().batchId(),msg.rows().length, msg.inputBatchDetails().round());

        appliedBatchKeys.add(batchKey);
        receivedBatchIdsByTable.computeIfAbsent(ibd.tableId(), t -> new TreeSet<>()).add(ibd.batchId());
        epochsProcessed = Math.max(epochsProcessed, ibd.epoch());

        int[] valueIds    = msg.valueIds();
        long[] rows   = msg.rows();
        AttributeDeltaBuilder deltaBuilder = new AttributeDeltaBuilder(msg.inputBatchDetails().round());
        RoaringBitmap newDistinctThisBatch = new RoaringBitmap();

        for (int i = 0; i < rows.length; i++) {
            int vid  = valueIds[i];
            int rowI = (int) rows[i];
            sketchStore.insert(vid);
            if(Debug.INTERNAL)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                        "Adding row {} to bitmap for value {}",rowI, vid);

            boolean newValue = !bitmapStore.contains(vid);
            if (newValue){
                bitmapStore.insertIds(new int[]{vid});
                newDistinctThisBatch.add(vid);
            }
            valueToRowsDelta.add(vid, rowI);
            deltaBuilder.addInsert(vid,rowI);
        }

        deltaLogDequeue.addLast(deltaBuilder.build());
        if(!newDistinctThisBatch.isEmpty()) {
            publishLiveDistinctDelta(msg.inputBatchDetails().round(), newDistinctThisBatch);
            if(Debug.INTERNAL)
                formLog(getContext().getLog(), String.valueOf(Debug.LogType.INTERNAL), Debug.attr(),colId,"-", String.valueOf(Debug.State.NONE),
                        "Published Live distinct delta for col {} epoch {} batchID {} round {}" ,
                        colId, msg.inputBatchDetails().epoch(),msg.inputBatchDetails().batchId(), msg.inputBatchDetails().round());
        }
        if (msg.ackTo() != null)
            msg.ackTo().tell(new BDCommand.BatchFlushed(ibd, colId));
        return this;
    }

    private void publishLiveDistinctDelta(int round, RoaringBitmap newValues) {
        if (newValues == null || newValues.isEmpty())
            return;

        for (EntityRef<CMCommand> cm : lhsSubscriptions.values())
            cm.tell(new CMCommand.LhsLiveDelta(colId, round, newValues));

        for (EntityRef<CMCommand> cm : rhsSubscriptions.values())
            cm.tell(new CMCommand.RhsLiveDelta(colId, round, newValues));
    }

    private void cleanupOldDeltas(int checkpointRound){
        while(!deltaLogDequeue.isEmpty() && deltaLogDequeue.peekFirst().round() <= checkpointRound){
            deltaLogDequeue.removeFirst();
        }
    }

    private void publishCheckpointSketch(int round,long epoch, ActorRef<AppraiserCommand> appraiserRef) {
        SketchSummary summary = checkPoint.sketchSummary();
        cleanCheckpointSketches.put(round, summary);
        appraiserRef.tell(new AppraiserCommand.SketchArrived(summary));
        List<ActorRef<AppraiserCommand>> waiters = pendingSketchRequests.remove(round);
        if (waiters != null) {
            for (ActorRef<AppraiserCommand> ref : waiters) {
                ref.tell(new AppraiserCommand.SketchArrived(summary));
            }
        }
    }

    private RoaringBitmap distinctValuesFromDelta(AttributeDelta delta) {
        return delta.distinctValues().clone();
    }

    private static int findOwnerTable(int colId, DatasetMetadata metadata) {
        for (int t = 0; t < metadata.offsets().size(); t++) {
            int offset = metadata.offsets().get(t);
            int nCols = metadata.nCols().get(t);
            if (colId >= offset && colId < offset + nCols) {
                return t;
            }
        }
        throw new IllegalArgumentException("No table owns colId=" + colId);
    }

}
