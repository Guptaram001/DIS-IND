package disIND.valueBased.utility;

import java.util.LinkedHashMap;
import java.util.Map;

import disIND.valueBased.model.SharedModel.DataOrientation;
import disIND.valueBased.model.SharedModel.CandidateTrackingMode;

public final class UserConfig {
    private UserConfig() {
    }

    public static final String DEFAULT_INPUT_DIR = "data/synthetic";
    public static final String DEFAULT_OUTPUT_FILE = "output/ind-report.txt";
    public static final int DEFAULT_BATCH_SIZE = 2;
    public static final int DEFAULT_MAX_TRACKED_VIOLATIONS = 500;
    public static final int MAX_VALUE_OWNER_WITNESSES = 500;
    public static final double DEFAULT_KMV_PRUNE_THRESHOLD = 0.7;
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 5;
    public static final int DEFAULT_VO_BATCH_EVICTION_LIMIT = 100;
    public static final int DEFAULT_DL_BD_CREDIT_WINDOW = 2;
    public static final int DEFAULT_BD_AA_CREDIT_WINDOW = 2;
    public static final int DEFAULT_VALUE_OWNER_BUCKETS = 256;
    public static final int DEFAULT_CM_PARTITIONS = 128;
    public static final float BLOOM_FILTER_BITS_PER_KEY = 10.0f;
    public static final int DEFAULT_BATCH_ACK_TIMEOUT_SECONDS = 120;
    public static final int DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS = 5;
    public static final int DEFAULT_DRAIN_MAX_IN_FLIGHT = 128;
    public static final int DEFAULT_DRAIN_BATCH_SIZE = 16;
    public static final int DEFAULT_DRAIN_RETRY_SECONDS = 2;
    public static final int DEFAULT_CHECKPOINT_WRITERS_PER_NODE = 4;
    public static final boolean DEFAULT_STORE_VALUE_STRINGS = true;
    public static final boolean DEFAULT_PRUNE_CQF_ENABLED = true;
    public static final boolean DEFAULT_PRUNE_PARTITION_COUNTS_ENABLED = true;
    public static final int DEFAULT_PRUNE_COUNT_PARTITIONS = 64;
    public static final int DEFAULT_VALUE_ID_HOT_ENTRIES = 100_000;
    public static final int DEFAULT_VALUE_OWNER_HOT_ENTRIES = 100_000;
    public static final DataOrientation DEFAULT_DATA_ORIENTATION = DataOrientation.VALUE_MAJOR;
    public static final CandidateTrackingMode DEFAULT_CANDIDATE_TRACKING = CandidateTrackingMode.COUNT;
    public static final String DEFAULT_VALUE_ID_DISK_DIR = System.getProperty("java.io.tmpdir") + "/dis-ind-value-ids";
    public static final String DEFAULT_VALUE_TO_ROWS_DISK_DIR = System.getProperty("java.io.tmpdir")
            + "/dis-ind-value-to-rows";
    public static final String DEFAULT_VALUE_OWNER_DISK_DIR = System.getProperty("java.io.tmpdir")
            + "/dis-ind-value-owners";

    public static String inputDir = DEFAULT_INPUT_DIR;
    public static String outputDir = DEFAULT_OUTPUT_FILE;
    public static int BATCH_SIZE = DEFAULT_BATCH_SIZE;
    public static int MAX_TRACKED_VIOLATIONS = DEFAULT_MAX_TRACKED_VIOLATIONS;
    public static double KMV_PRUNE_THRESHOLD = DEFAULT_KMV_PRUNE_THRESHOLD;
    public static int CHECKPOINT_INTERVAL = DEFAULT_CHECKPOINT_INTERVAL;
    public static int DL_BD_CREDIT_WINDOW = DEFAULT_DL_BD_CREDIT_WINDOW;
    public static int BD_AA_CREDIT_WINDOW = DEFAULT_BD_AA_CREDIT_WINDOW;
    public static int VALUE_OWNER_BUCKETS = DEFAULT_VALUE_OWNER_BUCKETS;
    public static int BATCH_ACK_TIMEOUT_SECONDS = DEFAULT_BATCH_ACK_TIMEOUT_SECONDS;
    public static int FINAL_CM_DRAIN_TIMEOUT_SECONDS = DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS;
    public static int DRAIN_MAX_IN_FLIGHT = DEFAULT_DRAIN_MAX_IN_FLIGHT;
    public static int DRAIN_BATCH_SIZE = DEFAULT_DRAIN_BATCH_SIZE;
    public static int DRAIN_RETRY_SECONDS = DEFAULT_DRAIN_RETRY_SECONDS;
    public static int CHECKPOINT_WRITERS_PER_NODE = DEFAULT_CHECKPOINT_WRITERS_PER_NODE;
    public static boolean STORE_VALUE_STRINGS = DEFAULT_STORE_VALUE_STRINGS;
    public static boolean PRUNE_CQF_ENABLED = DEFAULT_PRUNE_CQF_ENABLED;
    public static boolean PRUNE_PARTITION_COUNTS_ENABLED = DEFAULT_PRUNE_PARTITION_COUNTS_ENABLED;
    public static int PRUNE_COUNT_PARTITIONS = DEFAULT_PRUNE_COUNT_PARTITIONS;
    public static int VALUE_ID_HOT_ENTRIES = DEFAULT_VALUE_ID_HOT_ENTRIES;
    public static int VALUE_OWNER_HOT_ENTRIES = DEFAULT_VALUE_OWNER_HOT_ENTRIES;
    public static String VALUE_ID_DISK_DIR = DEFAULT_VALUE_ID_DISK_DIR;
    public static String VALUE_TO_ROWS_DISK_DIR = DEFAULT_VALUE_TO_ROWS_DISK_DIR;
    public static String VALUE_OWNER_DISK_DIR = DEFAULT_VALUE_OWNER_DISK_DIR;
    public static DataOrientation DATA_ORIENTATION = DEFAULT_DATA_ORIENTATION;
    public static CandidateTrackingMode CANDIDATE_TRACKING = DEFAULT_CANDIDATE_TRACKING;
    private static final Map<String, String> CLI_PROPERTIES = new LinkedHashMap<>();

    static {
        CLI_PROPERTIES.put("input-dir", "dis.ind.input-dir");
        CLI_PROPERTIES.put("output-file", "dis.ind.output-file");
        CLI_PROPERTIES.put("batch-size", "dis.ind.batch-size");
        CLI_PROPERTIES.put("max-tracked-violations", "dis.ind.max-tracked-violations");
        CLI_PROPERTIES.put("kmv-prune-threshold", "dis.ind.kmv-prune-threshold");
        CLI_PROPERTIES.put("checkpoint-interval", "dis.ind.checkpoint-interval");
        CLI_PROPERTIES.put("dl-bd-credit-window", "dis.ind.dl-bd-credit-window");
        CLI_PROPERTIES.put("bd-aa-credit-window", "dis.ind.bd-aa-credit-window");
        CLI_PROPERTIES.put("value-owner-buckets", "dis.ind.value-owner-buckets");
        CLI_PROPERTIES.put("batch-ack-timeout-seconds", "dis.ind.batch-ack-timeout-seconds");
        CLI_PROPERTIES.put("final-cm-drain-timeout-seconds", "dis.ind.final-cm-drain-timeout-seconds");
        CLI_PROPERTIES.put("drain-max-in-flight", "dis.ind.drain-max-in-flight");
        CLI_PROPERTIES.put("drain-batch-size", "dis.ind.drain-batch-size");
        CLI_PROPERTIES.put("drain-retry-seconds", "dis.ind.drain-retry-seconds");
        CLI_PROPERTIES.put("checkpoint-writers-per-node", "dis.ind.checkpoint-writers-per-node");
        CLI_PROPERTIES.put("store-value-strings", "dis.ind.store-value-strings");
        CLI_PROPERTIES.put("prune-cqf-enabled", "dis.ind.prune-cqf-enabled");
        CLI_PROPERTIES.put("prune-partition-counts-enabled", "dis.ind.prune-partition-counts-enabled");
        CLI_PROPERTIES.put("prune-count-partitions", "dis.ind.prune-count-partitions");
        CLI_PROPERTIES.put("value-id-hot-entries", "dis.ind.value-id-hot-entries");
        CLI_PROPERTIES.put("value-id-disk-dir", "dis.ind.value-id-disk-dir");
        CLI_PROPERTIES.put("value-to-rows-disk-dir", "dis.ind.value-to-rows-disk-dir");
        CLI_PROPERTIES.put("value-owner-hot-entries", "dis.ind.value-owner-hot-entries");
        CLI_PROPERTIES.put("value-owner-disk-dir", "dis.ind.value-owner-disk-dir");
        CLI_PROPERTIES.put("data-orientation", "dis.ind.data-orientation");
        CLI_PROPERTIES.put("candidate-tracking", "dis.ind.candidate-tracking");
    }

    /**
     * Resolves application settings once, before the application starts.
     * Precedence is CLI, JVM system property, environment variable, then the
     * single default declared in this class.
     */
    public static void initialize(String[] args) {
        applyCommandLine(args);

        inputDir = stringSetting("DIS_IND_INPUT_DIR", "dis.ind.input-dir", DEFAULT_INPUT_DIR);
        outputDir = stringSetting("DIS_IND_OUTPUT_FILE", "dis.ind.output-file", DEFAULT_OUTPUT_FILE);
        BATCH_SIZE = positiveIntSetting("DIS_IND_BATCH_SIZE", "dis.ind.batch-size", DEFAULT_BATCH_SIZE);
        MAX_TRACKED_VIOLATIONS = positiveIntSetting("DIS_IND_MAX_TRACKED_VIOLATIONS",
                "dis.ind.max-tracked-violations", DEFAULT_MAX_TRACKED_VIOLATIONS);
        KMV_PRUNE_THRESHOLD = doubleSetting("DIS_IND_KMV_PRUNE_THRESHOLD",
                "dis.ind.kmv-prune-threshold", DEFAULT_KMV_PRUNE_THRESHOLD);
        CHECKPOINT_INTERVAL = positiveIntSetting("DIS_IND_CHECKPOINT_INTERVAL",
                "dis.ind.checkpoint-interval", DEFAULT_CHECKPOINT_INTERVAL);
        DL_BD_CREDIT_WINDOW = positiveIntSetting("DIS_IND_DL_BD_CREDIT_WINDOW",
                "dis.ind.dl-bd-credit-window", DEFAULT_DL_BD_CREDIT_WINDOW);
        BD_AA_CREDIT_WINDOW = positiveIntSetting("DIS_IND_BD_AA_CREDIT_WINDOW",
                "dis.ind.bd-aa-credit-window", DEFAULT_BD_AA_CREDIT_WINDOW);
        VALUE_OWNER_BUCKETS = positiveIntSetting("DIS_IND_VALUE_OWNER_BUCKETS",
                "dis.ind.value-owner-buckets", DEFAULT_VALUE_OWNER_BUCKETS);
        BATCH_ACK_TIMEOUT_SECONDS = positiveIntSetting("DIS_IND_BATCH_ACK_TIMEOUT_SECONDS",
                "dis.ind.batch-ack-timeout-seconds", DEFAULT_BATCH_ACK_TIMEOUT_SECONDS);
        FINAL_CM_DRAIN_TIMEOUT_SECONDS = positiveIntSetting("DIS_IND_FINAL_CM_DRAIN_TIMEOUT_SECONDS",
                "dis.ind.final-cm-drain-timeout-seconds", DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS);
        DRAIN_MAX_IN_FLIGHT = positiveIntSetting("DIS_IND_DRAIN_MAX_IN_FLIGHT",
                "dis.ind.drain-max-in-flight", DEFAULT_DRAIN_MAX_IN_FLIGHT);
        DRAIN_BATCH_SIZE = positiveIntSetting("DIS_IND_DRAIN_BATCH_SIZE",
                "dis.ind.drain-batch-size", DEFAULT_DRAIN_BATCH_SIZE);
        DRAIN_RETRY_SECONDS = positiveIntSetting("DIS_IND_DRAIN_RETRY_SECONDS",
                "dis.ind.drain-retry-seconds", DEFAULT_DRAIN_RETRY_SECONDS);
        CHECKPOINT_WRITERS_PER_NODE = positiveIntSetting("DIS_IND_CHECKPOINT_WRITERS_PER_NODE",
                "dis.ind.checkpoint-writers-per-node", DEFAULT_CHECKPOINT_WRITERS_PER_NODE);
        STORE_VALUE_STRINGS = booleanSetting("DIS_IND_STORE_VALUE_STRINGS",
                "dis.ind.store-value-strings", DEFAULT_STORE_VALUE_STRINGS);
        PRUNE_CQF_ENABLED = booleanSetting("DIS_IND_PRUNE_CQF_ENABLED",
                "dis.ind.prune-cqf-enabled", DEFAULT_PRUNE_CQF_ENABLED);
        PRUNE_PARTITION_COUNTS_ENABLED = booleanSetting("DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED",
                "dis.ind.prune-partition-counts-enabled", DEFAULT_PRUNE_PARTITION_COUNTS_ENABLED);
        PRUNE_COUNT_PARTITIONS = powerOfTwoSetting("DIS_IND_PRUNE_COUNT_PARTITIONS",
                "dis.ind.prune-count-partitions", DEFAULT_PRUNE_COUNT_PARTITIONS);
        VALUE_ID_HOT_ENTRIES = positiveIntSetting("DIS_IND_VALUE_ID_HOT_ENTRIES",
                "dis.ind.value-id-hot-entries", DEFAULT_VALUE_ID_HOT_ENTRIES);
        VALUE_ID_DISK_DIR = stringSetting("DIS_IND_VALUE_ID_DISK_DIR",
                "dis.ind.value-id-disk-dir", DEFAULT_VALUE_ID_DISK_DIR);
        VALUE_TO_ROWS_DISK_DIR = stringSetting("DIS_IND_VALUE_TO_ROWS_DISK_DIR",
                "dis.ind.value-to-rows-disk-dir", DEFAULT_VALUE_TO_ROWS_DISK_DIR);
        VALUE_OWNER_HOT_ENTRIES = positiveIntSetting("DIS_IND_VALUE_OWNER_HOT_ENTRIES",
                "dis.ind.value-owner-hot-entries", DEFAULT_VALUE_OWNER_HOT_ENTRIES);
        VALUE_OWNER_DISK_DIR = stringSetting("DIS_IND_VALUE_OWNER_DISK_DIR",
                "dis.ind.value-owner-disk-dir", DEFAULT_VALUE_OWNER_DISK_DIR);
        DATA_ORIENTATION = orientationSetting("DIS_IND_DATA_ORIENTATION",
                "dis.ind.data-orientation", DEFAULT_DATA_ORIENTATION);
        CANDIDATE_TRACKING = candidateTrackingSetting("DIS_IND_CANDIDATE_TRACKING",
                "dis.ind.candidate-tracking", DEFAULT_CANDIDATE_TRACKING);
        // if (CANDIDATE_TRACKING == CandidateTrackingMode.WITNESS &&
        // MAX_TRACKED_VIOLATIONS > MAX_VALUE_OWNER_WITNESSES) {
        // throw new IllegalArgumentException("dis.ind.max-tracked-violations / "
        // + "DIS_IND_MAX_TRACKED_VIOLATIONS must be at most "
        // + MAX_VALUE_OWNER_WITNESSES
        // + " when witness candidate tracking is selected: "
        // + MAX_TRACKED_VIOLATIONS);
        // }
    }

    private static void applyCommandLine(String[] args) {
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + argument);
            }
            String option = argument.substring(2);
            String value;
            int equals = option.indexOf('=');
            if (equals >= 0) {
                value = option.substring(equals + 1);
                option = option.substring(0, equals);
            } else {
                if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                    throw new IllegalArgumentException("--" + option + " requires a value");
                }
                value = args[++index];
            }
            String property = CLI_PROPERTIES.get(option);
            if (property == null) {
                throw new IllegalArgumentException("Unknown option --" + option
                        + ". Supported options: --" + String.join(", --", CLI_PROPERTIES.keySet()));
            }
            if (value.isBlank()) {
                throw new IllegalArgumentException("--" + option + " cannot be blank");
            }
            System.setProperty(property, value);
        }
    }

    private static String stringSetting(String environmentName, String propertyName, String fallback) {
        String property = System.getProperty(propertyName);
        if (property != null && !property.isBlank()) {
            return property;
        }
        String environment = System.getenv(environmentName);
        return environment == null || environment.isBlank() ? fallback : environment;
    }

    private static int positiveIntSetting(String environmentName, String propertyName, int fallback) {
        String value = stringSetting(environmentName, propertyName, null);
        if (value == null) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(
                        settingName(environmentName, propertyName) + " must be greater than zero: " + value);
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    settingName(environmentName, propertyName) + " must be an integer: " + value, exception);
        }
    }

    private static int powerOfTwoSetting(String environmentName, String propertyName, int fallback) {
        int value = positiveIntSetting(environmentName, propertyName, fallback);
        if ((value & (value - 1)) != 0) {
            throw new IllegalArgumentException(
                    settingName(environmentName, propertyName) + " must be a power of two: " + value);
        }
        return value;
    }

    private static double doubleSetting(String environmentName, String propertyName, double fallback) {

        String value = stringSetting(environmentName, propertyName, null);
        if (value == null) {
            return fallback;
        }
        try {
            double parsed = Double.parseDouble(value);
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    settingName(environmentName, propertyName) + " must be a number: " + value, exception);
        }
    }

    private static boolean booleanSetting(String environmentName, String propertyName, boolean fallback) {

        String value = stringSetting(environmentName, propertyName, null);

        if (value == null) {
            return fallback;
        }

        return switch (value.toLowerCase()) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException(
                    settingName(environmentName, propertyName) + " must be true or false: " + value);
        };
    }

    private static String settingName(String environmentName, String propertyName) {
        return propertyName + " / " + environmentName;
    }

    private static DataOrientation orientationSetting(String environmentName, String propertyName,
            DataOrientation fallback) {

        String value = stringSetting(environmentName, propertyName, null);

        if (value == null)
            return fallback;

        return switch (value.trim().toLowerCase()) {
            case "value", "value-major" -> DataOrientation.VALUE_MAJOR;
            case "column", "column-major" -> DataOrientation.COLUMN_MAJOR;
            default -> throw new IllegalArgumentException(settingName(environmentName, propertyName) +
                    " must be value or column: " + value);
        };
    }

    private static CandidateTrackingMode candidateTrackingSetting(String environmentName, String propertyName,
            CandidateTrackingMode fallback) {

        String value = stringSetting(environmentName, propertyName, null);
        if (value == null)
            return fallback;

        return switch (value.trim().toLowerCase()) {
            case "count", "counter" -> CandidateTrackingMode.COUNT;
            case "witness", "witnesses" -> CandidateTrackingMode.WITNESS;
            case "prune", "pruner" -> CandidateTrackingMode.PRUNE;
            case "exact" -> CandidateTrackingMode.EXACT;
            default -> throw new IllegalArgumentException(
                    settingName(environmentName, propertyName) + " must be count, exact,prune,or witness: " + value);
        };
    }
}
