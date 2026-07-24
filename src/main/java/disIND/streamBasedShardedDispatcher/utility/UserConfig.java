package disIND.streamBasedShardedDispatcher.utility;

import java.util.LinkedHashMap;
import java.util.Map;

public final class UserConfig {
    private UserConfig() {}

    public static final String DEFAULT_INPUT_DIR = "data/tpch-10-corrected";
    public static final String DEFAULT_OUTPUT_FILE = "output/ind-report.txt";
    public static final int DEFAULT_BATCH_SIZE = 15000;
    public static final int DEFAULT_MAX_TRACKED_VIOLATIONS = 500;
    public static final double DEFAULT_KMV_PRUNE_THRESHOLD = 0.7;
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 5;
    public static final int DEFAULT_DL_BD_CREDIT_WINDOW = 4;
    public static final int DEFAULT_BD_AA_CREDIT_WINDOW = 2;
    public static final int DEFAULT_BATCH_ACK_TIMEOUT_SECONDS = 120;
    public static final int DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS = 5;
    public static final boolean DEFAULT_STORE_VALUE_STRINGS = false;

    public static String inputDir = DEFAULT_INPUT_DIR;
    public static String outputDir = DEFAULT_OUTPUT_FILE;
    public static int BATCH_SIZE = DEFAULT_BATCH_SIZE;
    public static int MAX_TRACKED_VIOLATIONS = DEFAULT_MAX_TRACKED_VIOLATIONS;
    public static double KMV_PRUNE_THRESHOLD = DEFAULT_KMV_PRUNE_THRESHOLD;
    public static int CHECKPOINT_INTERVAL = DEFAULT_CHECKPOINT_INTERVAL;
    public static int DL_BD_CREDIT_WINDOW = DEFAULT_DL_BD_CREDIT_WINDOW;
    public static int BD_AA_CREDIT_WINDOW = DEFAULT_BD_AA_CREDIT_WINDOW;
    public static int BATCH_ACK_TIMEOUT_SECONDS = DEFAULT_BATCH_ACK_TIMEOUT_SECONDS;
    public static int FINAL_CM_DRAIN_TIMEOUT_SECONDS = DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS;
    public static boolean STORE_VALUE_STRINGS = DEFAULT_STORE_VALUE_STRINGS;

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
        CLI_PROPERTIES.put("batch-ack-timeout-seconds", "dis.ind.batch-ack-timeout-seconds");
        CLI_PROPERTIES.put("final-cm-drain-timeout-seconds", "dis.ind.final-cm-drain-timeout-seconds");
        CLI_PROPERTIES.put("store-value-strings", "dis.ind.store-value-strings");
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
        BATCH_ACK_TIMEOUT_SECONDS = positiveIntSetting("DIS_IND_BATCH_ACK_TIMEOUT_SECONDS",
                "dis.ind.batch-ack-timeout-seconds", DEFAULT_BATCH_ACK_TIMEOUT_SECONDS);
        FINAL_CM_DRAIN_TIMEOUT_SECONDS = positiveIntSetting("DIS_IND_FINAL_CM_DRAIN_TIMEOUT_SECONDS",
                "dis.ind.final-cm-drain-timeout-seconds", DEFAULT_FINAL_CM_DRAIN_TIMEOUT_SECONDS);
        STORE_VALUE_STRINGS = booleanSetting("DIS_IND_STORE_VALUE_STRINGS",
                "dis.ind.store-value-strings", DEFAULT_STORE_VALUE_STRINGS);
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

    private static int positiveIntSetting(String environmentName,String propertyName,int fallback) {
        String value = stringSetting(environmentName,propertyName,null);
        if (value == null) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(settingName(environmentName, propertyName)+ " must be greater than zero: " + value);
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    settingName(environmentName, propertyName)+ " must be an integer: " + value,exception);
        }
    }

    private static double doubleSetting(String environmentName,String propertyName,double fallback) {

        String value = stringSetting(environmentName,propertyName,null);

        if (value == null) {
            return fallback;
        }

        try {
            double parsed = Double.parseDouble(value);
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(settingName(environmentName, propertyName)+ " must be a number: " + value,exception);
        }
    }

    private static boolean booleanSetting(String environmentName,String propertyName,boolean fallback) {

        String value = stringSetting(environmentName,propertyName,null);

        if (value == null) {
            return fallback;
        }

        return switch (value.toLowerCase()) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException(settingName(environmentName, propertyName)+ " must be true or false: " + value);
        };
    }

    private static String settingName(String environmentName,String propertyName) {
        return propertyName + " / " + environmentName;
    }
}
