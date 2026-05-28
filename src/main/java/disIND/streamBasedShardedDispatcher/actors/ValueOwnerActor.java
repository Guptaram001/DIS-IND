package disIND.streamBasedShardedDispatcher.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import disIND.streamBasedShardedDispatcher.model.Ack;
import disIND.streamBasedShardedDispatcher.model.AkkaSerializable;
import disIND.streamBasedShardedDispatcher.model.WorkType;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
public class ValueOwnerActor extends AbstractBehavior<ValueOwnerActor.Command> {

    public interface Command extends AkkaSerializable {}

    public record BatchUpdate(
            int batchId,
            long entityHash,
            Map<Short, Integer> attrCounts,
            ActorRef<Ack> replyTo
    ) implements Command {}

    private Behavior<Command> onBatchUpdate(BatchUpdate cmd) {
        BitSet before = (BitSet) attrsContainingValue.clone();

        for (Map.Entry<Short, Integer> e : cmd.attrCounts().entrySet()) {
            short attr = e.getKey();
            int delta = e.getValue();
            int oldCount = counts.getOrDefault(attr, 0);
            int newCount = oldCount + delta;
            if (newCount > 0) {
                counts.put(attr, newCount);
                attrsContainingValue.set(attr);
            } else {
                counts.remove(attr);
                attrsContainingValue.clear(attr);
            }
        }

        BitSet after = (BitSet) attrsContainingValue.clone();

        if (!before.equals(after)) {
            propagateTransition(cmd.entityHash(), before, after);
        }

        BitSet addedAttrs = (BitSet) after.clone();
        addedAttrs.andNot(before);       // 0 -> 1

        BitSet removedAttrs = (BitSet) before.clone();
        removedAttrs.andNot(after);      // 1 -> 0

        cmd.replyTo().tell(new Ack(cmd.batchId(), cmd.entityHash(), WorkType.VALUE));

        if (!addedAttrs.isEmpty()) {
            //propagateAddedChange(cmd.entityHash(), after, addedAttrs);
            //propagateAddedChange(cmd.entityHash(), addedAttrs);
            updateRebuildActor((int) cmd.entityHash(), addedAttrs, true);
        }

        if (!removedAttrs.isEmpty()) {
            //propagateRemovedChange(cmd.entityHash(), after, removedAttrs);
            //propagateRemovedChange(cmd.entityHash(), removedAttrs);
            updateRebuildActor((int) cmd.entityHash(), removedAttrs, false);
        }
//        getContext().getLog().info(
//                "ValueOwner {} batch={} update={} state={}",
//                entityId,
//                cmd.batchId(),
//                cmd.attrCounts(),
//                counts
//        );

        if (cmd.batchId() % 1000 == 0) {
            getContext().getLog().info("ValueOwner {} batch={} attrs={}", entityId, cmd.batchId(), counts.size());
        }

        return this;
    }

    private void updateRebuildActor(int valueId, BitSet attrs, boolean added) {
        for (int attr = attrs.nextSetBit(0); attr >= 0; attr = attrs.nextSetBit(attr + 1)) {
            rebuildRegion.tell(
                    new ShardingEnvelope<>(
                            String.valueOf(attr),
                            new RebuildActor.UpdateMembership(
                                    valueId,
                                    added
                            )
                    )
            );
        }
    }


    private void propagateTransition(long valueHash, BitSet before, BitSet after) {
        BitSet touched = (BitSet) before.clone();
        touched.or(after);

        for (int attr = touched.nextSetBit(0);
             attr >= 0;
             attr = touched.nextSetBit(attr + 1)) {

            candidateRegion.tell(
                    new ShardingEnvelope<>(
                            String.valueOf(attr),
                            new CandidateManagerActor.SemanticTransition(
                                    valueHash,
                                    (BitSet) before.clone(),
                                    (BitSet) after.clone()
                            )
                    )
            );
        }
    }

//    private void propagateAddedChange(long valueHash, BitSet after, BitSet addedAttrs) {
//        for (int lhs = addedAttrs.nextSetBit(0);
//             lhs >= 0;
//             lhs = addedAttrs.nextSetBit(lhs + 1)) {
//
//            candidateRegion.tell(
//                    new ShardingEnvelope<>(
//                            String.valueOf(lhs),
//                            new CandidateManagerActor.ChangePropagate(
//                                    valueHash,
//                                    (BitSet) after.clone()
//                            )
//                    )
//            );
//        }
//    }
//
//    private void propagateAddedChange(long valueHash, BitSet addedAttrs) {
//
//        for (int attr = addedAttrs.nextSetBit(0);
//             attr >= 0;
//             attr = addedAttrs.nextSetBit(attr + 1)) {
//
//            candidateRegion.tell(
//                    new ShardingEnvelope<>(
//                            String.valueOf(attr),
//                            new CandidateManagerActor.Change(
//                                    valueHash,
//                                    (short) attr,
//                                    true
//                            )
//                    )
//            );
//        }
//    }
//
//    private void propagateRemovedChange(long valueHash, BitSet after, BitSet removedAttrs) {
//        for (int lhs = after.nextSetBit(0);
//             lhs >= 0;
//             lhs = after.nextSetBit(lhs + 1)) {
//
//            candidateRegion.tell(
//                    new ShardingEnvelope<>(
//                            String.valueOf(lhs),
//                            new CandidateManagerActor.ChangePropagate(
//                                    valueHash,
//                                    (BitSet) after.clone()
//                            )
//                    )
//            );
//        }
//    }
//
//    private void propagateRemovedChange(long valueHash, BitSet removedAttrs) {
//
//        for (int attr = removedAttrs.nextSetBit(0);
//             attr >= 0;
//             attr = removedAttrs.nextSetBit(attr + 1)) {
//
//            candidateRegion.tell(
//                    new ShardingEnvelope<>(
//                            String.valueOf(attr),
//                            new CandidateManagerActor.Change(
//                                    valueHash,
//                                    (short) attr,
//                                    false
//                            )
//                    )
//            );
//        }
//    }

    private final String entityId;
    private final BitSet attrsContainingValue = new BitSet();
    private final Map<Short, Integer> counts = new HashMap<>();
    private final ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion;
    private final ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion;

    public static Behavior<Command> create(String entityId,ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion
            ,ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion) {
        return Behaviors.setup(ctx -> new ValueOwnerActor(ctx, entityId, candidateRegion,rebuildRegion));
    }

    private ValueOwnerActor(ActorContext<Command> ctx, String entityId, ActorRef<ShardingEnvelope<CandidateManagerActor.Command>> candidateRegion,
                            ActorRef<ShardingEnvelope<RebuildActor.Command>> rebuildRegion) {
        super(ctx);
        this.entityId = entityId;
        this.candidateRegion = candidateRegion;
        this.rebuildRegion=rebuildRegion;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(BatchUpdate.class, this::onBatchUpdate)
                .build();
    }

}