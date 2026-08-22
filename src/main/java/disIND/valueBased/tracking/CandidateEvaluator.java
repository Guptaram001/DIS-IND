package disIND.valueBased.tracking;

import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.membership.ColumnSetFactory;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

import java.util.BitSet;

public final class CandidateEvaluator {
    private final ColumnSetFactory columnSets;
    private final ModeSpecificContext modeSpecificContext;
    // metrics
    private final long[] exactComparisonsByLhs;
    private final long[] candidateEvaluationsByLhs;

    private final int totalColumns;
    private final BitSet[] compatibleRhsByLhs;

    private final LongOpenHashSet evaluatedCandidates = new LongOpenHashSet();
    private final LongOpenHashSet newlyRejectedThisBatch = new LongOpenHashSet();

    public CandidateEvaluator(DatasetMetadata metadata, ColumnSetFactory columnSets,
            ModeSpecificContext modeSpecificContext) {
        this.columnSets = columnSets;
        this.modeSpecificContext = modeSpecificContext;
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.candidateEvaluationsByLhs = new long[metadata.totalCols()];
        this.totalColumns = metadata.totalCols();
        this.compatibleRhsByLhs = buildCompatibility(metadata);

    }

    public void evaluate(Int2ObjectMap<ColumnSet> addedColumnsByValue, Int2ObjectMap<Int2IntMap> updatedRecordsByValue,
            CandidateViolationAfterApplyingUpdates changes) {

        evaluatedCandidates.clear();
        newlyRejectedThisBatch.clear();
        boolean prune = modeSpecificContext.pruningEnabled();

        ObjectIterator<Int2ObjectMap.Entry<ColumnSet>> valueIterator = Int2ObjectMaps.fastIterator(
                addedColumnsByValue);

        while (valueIterator.hasNext()) {
            Int2ObjectMap.Entry<ColumnSet> valueEntry = valueIterator.next();
            int valueId = valueEntry.getIntKey();
            ColumnSet addedColumns = valueEntry.getValue();
            Int2IntMap updatedRecord = updatedRecordsByValue.get(valueId);
            if (updatedRecord == null)
                throw new IllegalStateException("No updated membership for value " + valueId);

            ColumnSet after = buildColumnSet(updatedRecord);
            ColumnSet before = after.copy();
            before.andNot(addedColumns);

            for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0; lhsCol = addedColumns.nextSetBit(lhsCol + 1))
                evaluateCreatedViolations(valueId, lhsCol, after, prune, changes);
            for (int rhsCol = addedColumns.nextSetBit(0); rhsCol >= 0; rhsCol = addedColumns.nextSetBit(rhsCol + 1))
                evaluateRepairedViolations(valueId, rhsCol, before, prune, changes);
        }
    }

    private void evaluateCreatedViolations(int valueId, int lhsCol, ColumnSet after, boolean prune,
            CandidateViolationAfterApplyingUpdates changes) {

        BitSet compatibleRhs = compatibleRhsByLhs[lhsCol];
        for (int rhsCol = compatibleRhs.nextSetBit(0); rhsCol >= 0; rhsCol = compatibleRhs.nextSetBit(
                rhsCol + 1)) {
            long compactKey = candidateKey(lhsCol, rhsCol);
            if (prune) {
                if (modeSpecificContext.locallyRejected(compactKey)) {
                    modeSpecificContext.invalidLhsSkipped(lhsCol);
                    continue;
                }
                if (newlyRejectedThisBatch.contains(compactKey)) {
                    modeSpecificContext.sameBatchSkipped(lhsCol);
                    continue;
                }
            }
            countComparison(lhsCol, compactKey);
            if (!after.contains(rhsCol)) {
                changes.violationCreated(lhsCol, rhsCol, valueId);
                if (prune)
                    newlyRejectedThisBatch.add(compactKey);
            }
        }
    }

    private void evaluateRepairedViolations(int valueId, int rhsCol, ColumnSet before, boolean prune,
            CandidateViolationAfterApplyingUpdates changes) {

        for (int lhsCol = before.nextSetBit(0); lhsCol >= 0; lhsCol = before.nextSetBit(lhsCol + 1)) {
            if (!compatibleRhsByLhs[lhsCol].get(rhsCol))
                continue;
            long compactKey = candidateKey(lhsCol, rhsCol);
            if (prune) {
                if (newlyRejectedThisBatch.contains(compactKey)) {
                    modeSpecificContext.sameBatchSkipped(lhsCol);
                    continue;
                }
                if (!modeSpecificContext.locallyRejected(compactKey)) {
                    modeSpecificContext.validRhsSkipped(lhsCol);
                    continue;
                }
            }
            countComparison(lhsCol, compactKey);
            changes.violationRepaired(lhsCol, rhsCol, valueId);
        }
    }

    private ColumnSet buildColumnSet(Int2IntMap membership) {
        ColumnSet result = columnSets.create();
        IntIterator iterator = membership.keySet().iterator();
        while (iterator.hasNext())
            result.add(iterator.nextInt());
        return result;
    }

    public long candidateEvaluationsFor(int lhsCol) {
        return candidateEvaluationsByLhs[lhsCol];
    }

    public long exactComparisonsFor(int lhsCol) {
        return exactComparisonsByLhs[lhsCol];
    }

    private void countComparison(int lhsCol, long compactKey) {
        exactComparisonsByLhs[lhsCol] = Math.addExact(exactComparisonsByLhs[lhsCol], 1);
        if (evaluatedCandidates.add(compactKey)) {
            candidateEvaluationsByLhs[lhsCol] = Math.addExact(candidateEvaluationsByLhs[lhsCol], 1);
        }
    }

    private BitSet[] buildCompatibility(DatasetMetadata metadata) {
        BitSet[] result = new BitSet[totalColumns];
        for (int lhs = 0; lhs < totalColumns; lhs++) {
            BitSet compatible = new BitSet(totalColumns);
            for (int rhs = 0; rhs < totalColumns; rhs++) {
                if (lhs != rhs && testCompatibility(metadata.typeOf(lhs), metadata.typeOf(rhs)))
                    compatible.set(rhs);
            }
            result[lhs] = compatible;
        }
        return result;
    }

    public static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }
}
