package disIND.valueBased.model;

import disIND.valueBased.utility.UserConfig;
import java.util.Objects;

/** Coordinator-selected options shared by every exact/prune value owner. */
public record ClusterOptions(IndCalculation calculation, boolean changeDetection,
        boolean partitionCounts, boolean partitionHierarchy, int partitions,
        boolean cqf, boolean transitive) implements AkkaSerializable {
    public enum IndCalculation { BATCH, FINAL }

    public ClusterOptions {
        Objects.requireNonNull(calculation);
        if (partitions <= 0 || (partitions & (partitions - 1)) != 0)
            throw new IllegalArgumentException("partitions must be a positive power of two");
    }

    public static ClusterOptions configured() {
        return new ClusterOptions(UserConfig.IND_CALCULATION, UserConfig.CLUSTER_CHANGE_DETECTION,
                UserConfig.PRUNE_PARTITION_COUNTS_ENABLED, UserConfig.PRUNE_PARTITION_HIERARCHY_ENABLED,
                UserConfig.PRUNE_COUNT_PARTITIONS, UserConfig.PRUNE_CQF_ENABLED, UserConfig.PRUNE_TRANSITIVE_ENABLED);
    }
}
