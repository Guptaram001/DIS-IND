
package disIND.valueBased.tracking;

import disIND.valueBased.membership.CandidateDomain;
import disIND.valueBased.membership.CandidateIndex;
import disIND.valueBased.membership.CandidateSet;
import disIND.valueBased.membership.CandidateSetFactory;
import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.membership.ColumnSetFactory;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;

public final class CandidateEvaluator {
    private final ModeSpecificContext modeSpecificContext;
    // metrics
    private final long[] exactComparisonsByLhs;
    private final long[] candidateEvaluationsByLhs;

    private final int totalColumns;
    private final CandidateDomain candidateDomain;
    private final CandidateIndex candidateIndex;
    private final CandidateSet evaluatedCandidates;
    private final CandidateSet newlyRejectedThisBatch;

    private final ColumnSet afterChangeSet;

    public CandidateEvaluator(DatasetMetadata metadata, ColumnSetFactory columnSets,
            ModeSpecificContext modeSpecificContext, CandidateDomain candidateDomain) {
        this.modeSpecificContext = modeSpecificContext;
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.candidateEvaluationsByLhs = new long[metadata.totalCols()];
        this.totalColumns = metadata.totalCols();
        this.candidateDomain = candidateDomain;
        this.candidateIndex = new CandidateIndex(totalColumns);
        this.evaluatedCandidates = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
        this.newlyRejectedThisBatch = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
        this.afterChangeSet = columnSets.create();
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

            loadAfter(updatedRecord);

            for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0; lhsCol = addedColumns.nextSetBit(lhsCol + 1))
                evaluateCreatedViolations(valueId, lhsCol, afterChangeSet, prune, changes);
            for (int rhsCol = addedColumns.nextSetBit(0); rhsCol >= 0; rhsCol = addedColumns.nextSetBit(rhsCol + 1))
                evaluateRepairedViolations(valueId, rhsCol, updatedRecord, addedColumns, prune, changes);
        }
    }

    private void evaluateCreatedViolations(int valueId, int lhsCol, ColumnSet after, boolean prune,
            CandidateViolationAfterApplyingUpdates changes) {

        for (int rhsCol = candidateDomain.firstCompatibleRhs(lhsCol); rhsCol >= 0; rhsCol = candidateDomain
                .nextCompatibleRhs(lhsCol, rhsCol)) {
            int index = candidateIndex.index(lhsCol, rhsCol);
            if (prune) {
                if (modeSpecificContext.locallyRejected(index)) {
                    modeSpecificContext.invalidLhsSkipped(lhsCol);
                    continue;
                }
                if (newlyRejectedThisBatch.contains(index)) {
                    modeSpecificContext.sameBatchSkipped(lhsCol);
                    continue;
                }
            }
            countComparison(lhsCol, index);
            if (!after.contains(rhsCol)) {
                changes.violationCreated(lhsCol, rhsCol, valueId);
                if (prune)
                    newlyRejectedThisBatch.add(index);
            }
        }
    }

    private void evaluateRepairedViolations(int valueId, int rhsCol, Int2IntMap updatedRecord,
            ColumnSet addedColumns, boolean prune, CandidateViolationAfterApplyingUpdates changes) {

        IntIterator iterator = updatedRecord.keySet().iterator();
        while (iterator.hasNext()) {
            int lhsCol = iterator.nextInt();
            if (addedColumns.contains(lhsCol))
                continue;
            if (!candidateDomain.isCompatible(lhsCol, rhsCol))
                continue;
            int index = candidateIndex.index(lhsCol, rhsCol);
            if (prune) {
                if (newlyRejectedThisBatch.contains(index)) {
                    modeSpecificContext.sameBatchSkipped(lhsCol);
                    continue;
                }
                if (!modeSpecificContext.locallyRejected(index)) {
                    modeSpecificContext.validRhsSkipped(lhsCol);
                    continue;
                }
            }
            countComparison(lhsCol, index);
            changes.violationRepaired(lhsCol, rhsCol, valueId);
        }
    }

    private void loadAfter(Int2IntMap membership) {
        afterChangeSet.clear();

        IntIterator iterator = membership.keySet().iterator();
        while (iterator.hasNext())
            afterChangeSet.add(iterator.nextInt());
    }

    public long candidateEvaluationsFor(int lhsCol) {
        return candidateEvaluationsByLhs[lhsCol];
    }

    public long exactComparisonsFor(int lhsCol) {
        return exactComparisonsByLhs[lhsCol];
    }

    private void countComparison(int lhsCol, int candidateIndex) {
        exactComparisonsByLhs[lhsCol] = Math.addExact(exactComparisonsByLhs[lhsCol], 1);
        if (evaluatedCandidates.add(candidateIndex)) {
            candidateEvaluationsByLhs[lhsCol] = Math.addExact(candidateEvaluationsByLhs[lhsCol], 1);
        }
    }

    public static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }
}
