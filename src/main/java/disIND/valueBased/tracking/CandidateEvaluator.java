package disIND.valueBased.tracking;

import disIND.valueBased.membership.ColumnSet;
import disIND.valueBased.membership.ColumnSetFactory;
import disIND.valueBased.model.SharedModel.DatasetMetadata;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.Map;

import static disIND.valueBased.utility.ColTypeCompatibility.testCompatibility;

public final class CandidateEvaluator {
    private final DatasetMetadata metadata;
    private final ColumnSetFactory columnSets;
    private final ModeSpecificContext modeSpecificContext;
    // metrics
    private final long[] exactComparisonsByLhs;
    private final long[] candidateEvaluationsByLhs;

    public CandidateEvaluator(DatasetMetadata metadata, ColumnSetFactory columnSets,
            ModeSpecificContext modeSpecificContext) {
        this.metadata = metadata;
        this.columnSets = columnSets;
        this.modeSpecificContext = modeSpecificContext;
        this.exactComparisonsByLhs = new long[metadata.totalCols()];
        this.candidateEvaluationsByLhs = new long[metadata.totalCols()];
    }

    public void evaluate(Map<Integer, ColumnSet> addedColumnsByValue,
            Map<Integer, Int2IntMap> updatedRecordsByValue,
            CandidateViolationAfterApplyingUpdates candidateViolationAfterApplyingUpdates) {

        LongSet evaluatedCandidates = new LongOpenHashSet();
        boolean prune = modeSpecificContext.pruningEnabled();
        LongSet newlyRejectedThisBatch = prune ? new LongOpenHashSet() : null;

        addedColumnsByValue.forEach((valueId, addedColumns) -> {
            ColumnSet after = columnSets.create();
            updatedRecordsByValue.get(valueId).keySet().forEach(after::add);
            ColumnSet before = after.copy();
            before.andNot(addedColumns);

            addedColumns.forEach(lhsCol -> {
                for (int rhsCol = 0; rhsCol < metadata.totalCols(); rhsCol++) {
                    if (!compatible(lhsCol, rhsCol))
                        continue;

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

                    countComparison(evaluatedCandidates, lhsCol, rhsCol);
                    if (!after.contains(rhsCol)) {
                        candidateViolationAfterApplyingUpdates.violationCreated(lhsCol, rhsCol, valueId);
                        if (prune)
                            newlyRejectedThisBatch.add(compactKey);
                    }
                }
            });

            addedColumns.forEach(rhsCol -> before.forEach(lhsCol -> {
                if (compatible(lhsCol, rhsCol)) {
                    long compactKey = candidateKey(lhsCol, rhsCol);
                    if (prune) {
                        if (newlyRejectedThisBatch.contains(compactKey)) {
                            modeSpecificContext.sameBatchSkipped(lhsCol);
                            return;
                        }
                        if (!modeSpecificContext.locallyRejected(compactKey)) {
                            modeSpecificContext.validRhsSkipped(lhsCol);
                            return;
                        }
                    }
                    countComparison(evaluatedCandidates, lhsCol, rhsCol);
                    candidateViolationAfterApplyingUpdates.violationRepaired(lhsCol, rhsCol, valueId);
                }
            }));
        });
    }

    public long candidateEvaluationsFor(int lhsCol) {
        return candidateEvaluationsByLhs[lhsCol];
    }

    public long exactComparisonsFor(int lhsCol) {
        return exactComparisonsByLhs[lhsCol];
    }

    private boolean compatible(int lhsCol, int rhsCol) {
        return lhsCol != rhsCol && testCompatibility(metadata.typeOf(lhsCol), metadata.typeOf(rhsCol));
    }

    private void countComparison(LongSet evaluatedCandidates, int lhsCol, int rhsCol) {
        exactComparisonsByLhs[lhsCol] = Math.addExact(exactComparisonsByLhs[lhsCol], 1L);
        long key = candidateKey(lhsCol, rhsCol);
        if (evaluatedCandidates.add(key)) {
            candidateEvaluationsByLhs[lhsCol] = Math.addExact(candidateEvaluationsByLhs[lhsCol], 1);
        }
    }

    public static long candidateKey(int lhsCol, int rhsCol) {
        return ((long) lhsCol << Integer.SIZE) | (rhsCol & 0xffffffffL);
    }
}
