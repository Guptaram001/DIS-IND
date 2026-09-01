
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
    private final CandidateSet affectedCandidatesForValue;

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
        this.affectedCandidatesForValue = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
    }

    public void evaluate(Int2ObjectMap<Int2IntMap> updatedRecordsByValue, Int2ObjectMap<ColumnSet> addedColumnsByValue,
            Int2ObjectMap<ColumnSet> removedColumnsByValue, CandidateViolationAfterApplyingUpdates changes) {

        if (removedColumnsByValue.isEmpty()) {
            evaluateInsertions(addedColumnsByValue, updatedRecordsByValue, changes);
            return;
        }
        evaluateMembershipTransitions(updatedRecordsByValue, addedColumnsByValue, removedColumnsByValue, changes);
    }

    private void evaluateMembershipTransitions(Int2ObjectMap<Int2IntMap> updatedRecordsByValue,
            Int2ObjectMap<ColumnSet> addedColumnsByValue, Int2ObjectMap<ColumnSet> removedColumnsByValue,
            CandidateViolationAfterApplyingUpdates changes) {

        evaluatedCandidates.clear();
        newlyRejectedThisBatch.clear();

        ObjectIterator<Int2ObjectMap.Entry<ColumnSet>> removedIterator = Int2ObjectMaps.fastIterator(
                removedColumnsByValue);

        while (removedIterator.hasNext()) {
            Int2ObjectMap.Entry<ColumnSet> entry = removedIterator.next();
            int valueId = entry.getIntKey();
            Int2IntMap membershipAfter = updatedRecordsByValue.get(valueId);
            if (membershipAfter == null)
                throw new IllegalStateException("No updated membership for value " + valueId);

            evaluateValueTransitions(valueId, membershipAfter, addedColumnsByValue.get(valueId), entry.getValue(),
                    changes);
        }

        ObjectIterator<Int2ObjectMap.Entry<ColumnSet>> addedIterator = Int2ObjectMaps.fastIterator(
                addedColumnsByValue);
        while (addedIterator.hasNext()) {
            Int2ObjectMap.Entry<ColumnSet> entry = addedIterator.next();
            int valueId = entry.getIntKey();
            if (removedColumnsByValue.containsKey(valueId))
                continue;

            Int2IntMap membershipAfter = updatedRecordsByValue.get(valueId);
            if (membershipAfter == null)
                throw new IllegalStateException("No updated membership for value " + valueId);
            evaluateValueTransitions(valueId, membershipAfter, entry.getValue(), null, changes);
        }
    }

    private static boolean containedBefore(int columnId, Int2IntMap membershipAfter, ColumnSet addedColumns,
            ColumnSet removedColumns) {

        if (removedColumns != null && removedColumns.contains(columnId))
            return true;
        if (addedColumns != null && addedColumns.contains(columnId))
            return false;
        return membershipAfter.containsKey(columnId);
    }

    private void evaluateValueTransitions(int valueId, Int2IntMap membershipAfter, ColumnSet addedColumns,
            ColumnSet removedColumns, CandidateViolationAfterApplyingUpdates changes) {

        affectedCandidatesForValue.clear();
        if (addedColumns != null)
            evaluateChangedColumns(valueId, membershipAfter, addedColumns, addedColumns, removedColumns, changes);

        if (removedColumns != null)
            evaluateChangedColumns(valueId, membershipAfter, removedColumns, addedColumns, removedColumns, changes);

    }

    private void evaluateChangedColumns(int valueId, Int2IntMap membershipAfter, ColumnSet changedColumns,
            ColumnSet addedColumns, ColumnSet removedColumns, CandidateViolationAfterApplyingUpdates changes) {

        for (int changedColumn = changedColumns.nextSetBit(0); changedColumn >= 0; changedColumn = changedColumns
                .nextSetBit(changedColumn + 1)) {

            for (int rhsCol = candidateDomain.firstCompatibleRhs(changedColumn); rhsCol >= 0; rhsCol = candidateDomain
                    .nextCompatibleRhs(changedColumn, rhsCol)) {
                evaluateCandidateOnce(valueId, changedColumn, rhsCol, membershipAfter, addedColumns, removedColumns,
                        changes);
            }

            for (int lhsCol = candidateDomain.firstCompatibleRhs(changedColumn); lhsCol >= 0; lhsCol = candidateDomain
                    .nextCompatibleRhs(changedColumn, lhsCol)) {
                boolean lhsAfter = membershipAfter.containsKey(lhsCol);
                boolean lhsBefore = containedBefore(lhsCol, membershipAfter, addedColumns, removedColumns);
                if (!lhsBefore && !lhsAfter)
                    continue;

                evaluateCandidateOnce(valueId, lhsCol, changedColumn, membershipAfter, addedColumns, removedColumns,
                        changes);
            }
        }
    }

    private void evaluateCandidateOnce(int valueId, int lhsCol, int rhsCol, Int2IntMap membershipAfter,
            ColumnSet addedColumns, ColumnSet removedColumns, CandidateViolationAfterApplyingUpdates changes) {

        int index = candidateIndex.index(lhsCol, rhsCol);

        if (!affectedCandidatesForValue.add(index))
            return;

        boolean lhsBefore = containedBefore(lhsCol, membershipAfter, addedColumns, removedColumns);
        boolean rhsBefore = containedBefore(rhsCol, membershipAfter, addedColumns, removedColumns);
        boolean violatedBefore = lhsBefore && !rhsBefore;
        boolean violatedAfter = membershipAfter.containsKey(lhsCol) && !membershipAfter.containsKey(rhsCol);
        if (violatedBefore == violatedAfter)
            return;

        boolean prune = modeSpecificContext.pruningEnabled();
        if (prune) {
            if (newlyRejectedThisBatch.contains(index)) {
                modeSpecificContext.sameBatchSkipped(lhsCol);
                return;
            }
            boolean rejected = modeSpecificContext.locallyRejected(index);
            if (violatedAfter && rejected) {
                modeSpecificContext.invalidLhsSkipped(lhsCol);
                return;
            }
            if (!violatedAfter && !rejected) {
                modeSpecificContext.validRhsSkipped(lhsCol);
                return;
            }
        }

        countComparison(lhsCol, index);
        if (violatedAfter) {
            changes.violationCreated(lhsCol, rhsCol, valueId);
            if (prune)
                newlyRejectedThisBatch.add(index);
        } else {
            changes.violationRepaired(lhsCol, rhsCol, valueId);
        }
    }

    public void evaluateInsertions(Int2ObjectMap<ColumnSet> addedColumnsByValue,
            Int2ObjectMap<Int2IntMap> updatedRecordsByValue,
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
