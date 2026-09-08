
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

    private final int totalColumns;
    private final CandidateDomain candidateDomain;
    private final CandidateIndex candidateIndex;
    private final CandidateSet newlyRejectedThisBatch; // For skipping, invalidated candidate.

    private final CandidateSet affectedCandidatesForValue;
    private final BitSet beforeChangeSet;
    private final BitSet afterChangeSet;
    private final BitSet tempCandiBitSet; // Temp to look into a candidate.
    private final BitSet[] newlyRejectedRhsByLhs; // For a lhs, which rhs is rejected
    private final BitSet touchedNewlyRejectedLhs; // Faster clearing of rejected rhs by lhs

    public CandidateEvaluator(DatasetMetadata metadata, ColumnSetFactory columnSets,
            ModeSpecificContext modeSpecificContext, CandidateDomain candidateDomain) {
        this.modeSpecificContext = modeSpecificContext;
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.totalColumns = metadata.totalCols();
        this.candidateDomain = candidateDomain;
        this.candidateIndex = new CandidateIndex(totalColumns);
        this.newlyRejectedThisBatch = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
        this.affectedCandidatesForValue = CandidateSetFactory.create(totalColumns, candidateIndex.capacity());
        this.beforeChangeSet = new BitSet(totalColumns);
        this.afterChangeSet = new BitSet(totalColumns);
        this.tempCandiBitSet = new BitSet(totalColumns);
        if (modeSpecificContext.candidateEventFilteringEnabled()) {
            this.newlyRejectedRhsByLhs = new BitSet[totalColumns];
            this.touchedNewlyRejectedLhs = new BitSet(totalColumns);
        } else {
            this.newlyRejectedRhsByLhs = null;
            this.touchedNewlyRejectedLhs = null;
        }
    }

    public void evaluate(Int2ObjectMap<Int2IntMap> updatedRecordsByValue, Int2ObjectMap<ColumnSet> addedColumnsByValue,
            Int2ObjectMap<ColumnSet> removedColumnsByValue, ViolationHandler violationHandler) {

        clearNewlyRejectedRows();

        if (removedColumnsByValue.isEmpty()) {
            // Only insertions, so no before/after , simple insert logic
            evaluateInsertions(addedColumnsByValue, updatedRecordsByValue, violationHandler);
            return;
        }
        // Both insertion and deletion handling
        evaluateMembershipTransitions(updatedRecordsByValue, addedColumnsByValue, removedColumnsByValue,
                violationHandler);
    }

    private void evaluateMembershipTransitions(Int2ObjectMap<Int2IntMap> updatedRecordsByValue,
            Int2ObjectMap<ColumnSet> addedColumnsByValue, Int2ObjectMap<ColumnSet> removedColumnsByValue,
            ViolationHandler violationHandler) {

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
                    violationHandler);
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
            evaluateValueTransitions(valueId, membershipAfter, entry.getValue(), null, violationHandler);
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
            ColumnSet removedColumns, ViolationHandler violationHandler) {

        affectedCandidatesForValue.clear();
        loadAfter(membershipAfter);
        loadBefore(addedColumns, removedColumns);

        if (addedColumns != null) {
            for (int column = addedColumns.nextSetBit(0); column >= 0; column = addedColumns.nextSetBit(column + 1)) {
                // New LHS membership creates violations only in RHS columns that do not contain
                // the value.
                candidateDomain.copyCompatibleRhs(column, tempCandiBitSet);
                tempCandiBitSet.andNot(afterChangeSet);
                evaluateCandidates(valueId, column, true, membershipAfter, addedColumns, removedColumns,
                        violationHandler);

                // New RHS membership repairs only LHS memberships that existed before this
                // update.
                candidateDomain.copyCompatibleLhs(column, tempCandiBitSet);
                tempCandiBitSet.and(beforeChangeSet);
                evaluateCandidates(valueId, column, false, membershipAfter, addedColumns, removedColumns,
                        violationHandler);
            }
        }

        if (removedColumns != null) {
            for (int column = removedColumns.nextSetBit(0); column >= 0; column = removedColumns
                    .nextSetBit(column + 1)) {
                // Removed LHS membership repairs only violations that existed before the
                // update.
                candidateDomain.copyCompatibleRhs(column, tempCandiBitSet);
                tempCandiBitSet.andNot(beforeChangeSet);
                evaluateCandidates(valueId, column, true, membershipAfter, addedColumns, removedColumns,
                        violationHandler);

                // Removed RHS membership creates violations only for LHS columns still
                // containing the value.
                candidateDomain.copyCompatibleLhs(column, tempCandiBitSet);
                tempCandiBitSet.and(afterChangeSet);
                evaluateCandidates(valueId, column, false, membershipAfter, addedColumns, removedColumns,
                        violationHandler);
            }
        }
    }

    private void evaluateCandidates(int valueId, int changedColumn, boolean changedColumnIsLhs,
            Int2IntMap membershipAfter, ColumnSet addedColumns, ColumnSet removedColumns,
            ViolationHandler changes) {
        for (int candidateColumn = tempCandiBitSet.nextSetBit(
                0); candidateColumn >= 0; candidateColumn = tempCandiBitSet.nextSetBit(candidateColumn + 1)) {
            int lhsCol = changedColumnIsLhs ? changedColumn : candidateColumn;
            int rhsCol = changedColumnIsLhs ? candidateColumn : changedColumn;
            evaluateCandidateOnce(valueId, lhsCol, rhsCol, membershipAfter, addedColumns, removedColumns, changes);
        }
    }

    private void evaluateCandidateOnce(int valueId, int lhsCol, int rhsCol, Int2IntMap membershipAfter,
            ColumnSet addedColumns, ColumnSet removedColumns, ViolationHandler changes) {

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
                modeSpecificContext.sameBatchRejectedCandidateSkipped(lhsCol);
                return;
            }
            boolean rejected = modeSpecificContext.locallyRejected(index);
            if (violatedAfter == rejected) {
                recordExistingStatusSkip(lhsCol, rhsCol, lhsBefore, rhsBefore, violatedAfter);
                return;
            }
        }

        countComparison(lhsCol);
        if (violatedAfter) {
            changes.violationCreated(lhsCol, rhsCol, valueId);
            if (filterEvents)
                newlyRejectedThisBatch.add(index);
        } else {
            changes.violationRepaired(lhsCol, rhsCol, valueId);
        }
    }

    private void recordExistingStatusSkip(int lhsCol, int rhsCol, boolean lhsBefore,
            boolean rhsBefore, boolean violatedAfter) {
        boolean lhsChanged = lhsBefore != afterChangeSet.get(lhsCol);
        boolean rhsChanged = rhsBefore != afterChangeSet.get(rhsCol);
        if (lhsChanged && rhsChanged) {
            modeSpecificContext.mixedUpdateSkipped(lhsCol);
        } else if (violatedAfter) {
            if (lhsChanged)
                modeSpecificContext.lhsInsertionInvalidSkipped(lhsCol);
            else
                modeSpecificContext.rhsDeletionInvalidSkipped(lhsCol);
        } else {
            if (rhsChanged)
                modeSpecificContext.rhsInsertionValidSkipped(lhsCol);
            else
                modeSpecificContext.lhsDeletionValidSkipped(lhsCol);
        }
    }

    private void evaluateInsertions(Int2ObjectMap<ColumnSet> addedColumnsByValue,
            Int2ObjectMap<Int2IntMap> updatedRecordsByValue, ViolationHandler violationHandler) {
        // Evaluate added only case.
        // Clear candidates newly rejected in the previous batch.
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

            // Add the updated changes by value to the afterchangeset.
            loadAfter(updatedRecord);
            // Added column is considered as LHS - can create violation.
            for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0; lhsCol = addedColumns.nextSetBit(lhsCol + 1))
                evaluateCreatedViolations(valueId, lhsCol, filterEvents, violationHandler);
            // Added column is considered as RHS - can repair.
            for (int rhsCol = addedColumns.nextSetBit(0); rhsCol >= 0; rhsCol = addedColumns.nextSetBit(rhsCol + 1))
                evaluateRepairedViolations(valueId, rhsCol, addedColumns, filterEvents, violationHandler);
        }
    }

    private void evaluateCreatedViolations(int valueId, int lhsCol, boolean filterEvents,
            ViolationHandler violationHandler) {

        // Current compatiblerhs bitset updated in tempCandidateBitset
        candidateDomain.copyCompatibleRhs(lhsCol, tempCandiBitSet);
        // tempbistest hold the compatbíble only rhs changed bitset.
        tempCandiBitSet.andNot(afterChangeSet);
        if (filterEvents) {
            // Look if already some rhs is rejected for that lhs
            BitSet newlyRejected = newlyRejectedRhsByLhs[lhsCol];
            if (newlyRejected != null) {
                int before = tempCandiBitSet.cardinality();
                tempCandiBitSet.andNot(newlyRejected);
                modeSpecificContext.sameBatchRejectedCandidateSkipped(lhsCol, before - tempCandiBitSet.cardinality());
            }
            modeSpecificContext.removeLocallyRejected(lhsCol, tempCandiBitSet);
        }
        // For remaining rhs, iterate and generate a violation.
        for (int rhsCol = tempCandiBitSet.nextSetBit(0); rhsCol >= 0; rhsCol = tempCandiBitSet
                .nextSetBit(rhsCol + 1)) {
            countComparison(lhsCol);
            violationHandler.violationCreated(lhsCol, rhsCol, valueId);
            if (filterEvents)
                markNewlyRejected(lhsCol, rhsCol);
        }
    }

    private void evaluateRepairedViolations(int valueId, int rhsCol, ColumnSet addedColumns, boolean filterEvents,
            ViolationHandler violationHandler) {

        candidateDomain.copyCompatibleLhs(rhsCol, tempCandiBitSet); // Compatiblelhs
        tempCandiBitSet.and(afterChangeSet); // Compatible lhs on updated records
        for (int lhsCol = addedColumns.nextSetBit(0); lhsCol >= 0; lhsCol = addedColumns.nextSetBit(lhsCol + 1))
            tempCandiBitSet.clear(lhsCol);

        for (int lhsCol = tempCandiBitSet.nextSetBit(0); lhsCol >= 0; lhsCol = tempCandiBitSet
                .nextSetBit(lhsCol + 1)) {
            int index = candidateIndex.index(lhsCol, rhsCol);
            if (filterEvents) {
                if (isNewlyRejected(lhsCol, rhsCol)) {
                    modeSpecificContext.sameBatchRejectedCandidateSkipped(lhsCol);
                    continue;
                }
                if (!modeSpecificContext.locallyRejected(index)) {
                    modeSpecificContext.rhsInsertionValidSkipped(lhsCol);
                    continue;
                }
            }
            countComparison(lhsCol);
            violationHandler.violationRepaired(lhsCol, rhsCol, valueId);
        }
    }

    private void loadAfter(Int2IntMap membership) {
        // Create reusable afterset like afterChangeSet = {1, 4, 7} for {1-3,4--1,7-2}
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
        // Stores which lhs needs to be clear at next batch.
        touchedNewlyRejectedLhs.set(lhsCol);
    }

    private boolean isNewlyRejected(int lhsCol, int rhsCol) {
        BitSet row = newlyRejectedRhsByLhs[lhsCol];
        if (row == null)
            return false;
        else
            return row.get(rhsCol);
    }

    private void clearNewlyRejectedRows() {
        if (touchedNewlyRejectedLhs == null)
            return;
        for (int lhs = touchedNewlyRejectedLhs.nextSetBit(0); lhs >= 0; lhs = touchedNewlyRejectedLhs
                .nextSetBit(lhs + 1))
            newlyRejectedRhsByLhs[lhs].clear();
        touchedNewlyRejectedLhs.clear();
    }

    public long exactComparisonsFor(int lhsCol) {
        return exactComparisonsByLhs[lhsCol];
    }

    private void countComparison(int lhsCol) {
        exactComparisonsByLhs[lhsCol] = Math.addExact(exactComparisonsByLhs[lhsCol], 1);
    }

    public static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }
}
