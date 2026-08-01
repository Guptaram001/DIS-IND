package disIND.valueBased.monitor;

import disIND.valueBased.model.AkkaSerializable;

public sealed interface StatsCommand extends AkkaSerializable permits StatsCommand.RowBatchProcessed,
        StatsCommand.CandidateCreated, StatsCommand.CandidateRemoved, StatsCommand.UnaryRebuild, StatsCommand.NaryRebuild,
        StatsCommand.IndDiscovered, StatsCommand.PrintStats, StatsCommand.AttributeStats {

    record RowBatchProcessed(long rows) implements StatsCommand {}

    record CandidateCreated() implements StatsCommand {}

    record CandidateRemoved() implements StatsCommand {}

    record UnaryRebuild() implements StatsCommand {}

    record NaryRebuild() implements StatsCommand {}

    record IndDiscovered() implements StatsCommand {}

    record PrintStats() implements StatsCommand {}

    record AttributeStats(int colId, long distinctValues, long bitmapCardinality,
                          long sketchCardinality) implements StatsCommand {}
}