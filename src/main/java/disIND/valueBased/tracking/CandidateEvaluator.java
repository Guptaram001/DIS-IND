
package disIND.valueBased.tracking;

import java.util.BitSet;

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

    private final CandidateSet affectedCandidatesForValue;
    private final BitSet beforeChangeSet;
    private final BitSet afterChangeSet;
    private final BitSet candidateScratch;
    private final BitSet[] newlyRejectedRhsByLhs;
    private final BitSet touchedNewlyRejectedLhs;

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
        this.affectedCandidatesForValue = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
        this.beforeChangeSet = new BitSet(totalColumns);
        this.afterChangeSet = new BitSet(totalColumns);
        this.candidateScratch = new BitSet(totalColumns);
        if (modeSpecificContext.candidateEventFilteringEnabled()) {
            this.newlyRejectedRhsByLhs = new BitSet[totalColumns];
            this.touchedNewlyRejectedLhs = new BitSet(totalColumns);
        } else {
            this.newlyRejectedRhsByLhs = null;
            this.touchedNewlyRejectedLhs = null;
        }
    }

    public void evaluate(Int2ObjectMap<Int2IntMap> updatedRecordsByValue, Int2ObjectMap<ColumnSet> addedColumnsByValue,
            Int2ObjectMap<ColumnSet> removedColumnsByValue, CandidateViolationAfterApplyingUpdates changes) {

        clearNewlyRejectedRows();

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
        loadAfter(membershipAfter);
        loadBefore(addedColumns, removedColumns);

        if (addedColumns != null) {
            for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns.nextSetBit(column + 1)) {
                // New LHS membership creates violations only in RHS columns that do not contain the value.
                candidateDomain.copyCompatibleRhs(column, candidateScratch);
                candidateScratch.andNot(afterChangeSet);
                evaluateCandidates(valueId, column, true, membershipAfter, addedColumns, removedColumns, changes);

                // New RHS membership repairs only LHS memberships that existed before this update.
                candidateDomain.copyCompatibleLhs(column, candidateScratch);
                candidateScratch.and(beforeChangeSet);
                evaluateCandidates(valueId, column, false, membershipAfter, addedColumns, removedColumns, changes);
            }
        }

        if (removedColumns != null) {
            for (int column = removedColumns.nextSetBit(0); column >= 0; column = removedColumns.nextSetBit(column + 1)) {
                // Removed LHS membership repairs only violations that existed before the update.
                candidateDomain.copyCompatibleRhs(column, candidateScratch);
                candidateScratch.andNot(beforeChangeSet);
                evaluateCandidates(valueId, column, true, membershipAfter, addedColumns, removedColumns, changes);

                // Removed RHS membership creates violations only for LHS columns still containing the value.
                candidateDomain.copyCompatibleLhs(column, candidateScratch);
                candidateScratch.and(afterChangeSet);
                evaluateCandidates(valueId, column, false, membershipAfter, addedColumns, removedColumns, changes);
            }
        }
    }

    private void evaluateCandidates(int valueId, int changedColumn, boolean changedColumnIsLhs,
            Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns,
            CandidateViolationAfterApplyingUpdates changes) {
        for (int candidateColumn = candidateScratch.nextSetBit(0); candidateColumn >= 0;
                candidateColumn = candidateScratch.nextSetBit(candidateColumn + 1)) {
            int lhsCol = changedColumnIsLhs ? changedColumn : candidateColumn;
            int rhsCol = changedColumnIsLhs ? candidateColumn : changedColumn;
            evaluateCandidateOnce(valueId, lhsCol, rhsCol, membershipAfter, addedColumns, removedColumns, changes);
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

        boolean filterEvents = modeSpecificContext.candidateEventFilteringEnabled();
        if (filterEvents) {
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
            if (filterEvents)
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
        boolean filterEvents = modeSpecificContext.candidateEventFilteringEnabled();

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
            for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0;
                    lhsCol = addedColumns.nextSetBit(lhsCol + 1))
                evaluateCreatedViolations(valueId, lhsCol, filterEvents, changes);
            for (int rhsCol = addedColumns.nextSetBit(0); rhsCol >= 0;
                    rhsCol = addedColumns.nextSetBit(rhsCol + 1))
                evaluateRepairedViolations(valueId, rhsCol, addedColumns, filterEvents, changes);
        }
    }

    private void evaluateCreatedViolations(int valueId, int lhsCol, boolean filterEvents,
            CandidateViolationAfterApplyingUpdates changes) {

        candidateDomain.copyCompatibleRhs(lhsCol, candidateScratch);
        candidateScratch.andNot(afterChangeSet);
        if (filterEvents) {
            modeSpecificContext.removeLocallyRejected(lhsCol, candidateScratch);
            BitSet newlyRejected = newlyRejectedRhsByLhs[lhsCol];
            if (newlyRejected != null) {
                int before = candidateScratch.cardinality();
                candidateScratch.andNot(newlyRejected);
                modeSpecificContext.sameBatchSkipped(lhsCol, before - candidateScratch.cardinality());
            }
        }
        for (int rhsCol = candidateScratch.nextSetBit(0); rhsCol >= 0;
                rhsCol = candidateScratch.nextSetBit(rhsCol + 1)) {
            int index = candidateIndex.index(lhsCol, rhsCol);
            countComparison(lhsCol, index);
            changes.violationCreated(lhsCol, rhsCol, valueId);
            if (filterEvents)
                markNewlyRejected(lhsCol, rhsCol);
        }
    }

    private void evaluateRepairedViolations(int valueId, int rhsCol, ColumnSet addedColumns, boolean filterEvents,
            CandidateViolationAfterApplyingUpdates changes) {

        candidateDomain.copyCompatibleLhs(rhsCol, candidateScratch);
        candidateScratch.and(afterChangeSet);
        for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0; lhsCol = addedColumns.nextSetBit(lhsCol + 1))
            candidateScratch.clear(lhsCol);

        for (int lhsCol = candidateScratch.nextSetBit(0); lhsCol >= 0;
                lhsCol = candidateScratch.nextSetBit(lhsCol + 1)) {
            int index = candidateIndex.index(lhsCol, rhsCol);
            if (filterEvents) {
                if (isNewlyRejected(lhsCol, rhsCol)) {
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
            afterChangeSet.set(iterator.nextInt());
    }

    private void loadBefore(ColumnSet addedColumns, ColumnSet removedColumns) {
        beforeChangeSet.clear();
        beforeChangeSet.or(afterChangeSet);
        if (addedColumns != null) {
            for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns.nextSetBit(column + 1))
                beforeChangeSet.clear(column);
        }
        if (removedColumns != null) {
            for (int column = removedColumns.nextSetBit(0); column >= 0; column = removedColumns.nextSetBit(column + 1))
                beforeChangeSet.set(column);
        }
    }

    private void markNewlyRejected(int lhsCol, int rhsCol) {
        BitSet row = newlyRejectedRhsByLhs[lhsCol];
        if (row == null) {
            row = new BitSet(totalColumns);
            newlyRejectedRhsByLhs[lhsCol] = row;
        }
        row.set(rhsCol);
        touchedNewlyRejectedLhs.set(lhsCol);
    }

    private boolean isNewlyRejected(int lhsCol, int rhsCol) {
        BitSet row = newlyRejectedRhsByLhs[lhsCol];
        return row != null && row.get(rhsCol);
    }

    private void clearNewlyRejectedRows() {
        if (touchedNewlyRejectedLhs == null)
            return;
        for (int lhs = touchedNewlyRejectedLhs.nextSetBit(0); lhs >= 0;
                lhs = touchedNewlyRejectedLhs.nextSetBit(lhs + 1))
            newlyRejectedRhsByLhs[lhs].clear();
        touchedNewlyRejectedLhs.clear();
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
