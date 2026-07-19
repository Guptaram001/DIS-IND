package disIND.streamBasedShardedDispatcher.utility;

public final class UserConfig {
    public static final int MAX_TRACKED_VIOLATIONS = 500;
    public static final float KMV_PRUN_THRESHOLD = 0.7f;
    public static final int CHECKPOINT_INTERVAL = 5;    //Best based on # of table columns
    public static final String inputDir="/Users/gupta/Documents/DIS-IND/data/tpch-10-corrected";
    public static final String outputDir=null;
    public static final int BATCH_SIZE=15000;
    public static final int DL_BD_CREDIT_WINDOW = 4;
    public static final int BD_AA_CREDIT_WINDOW = 2;
    public static final int BATCH_ACK_TIMEOUT_SEC = 120;
    public static final int FINAL_CM_DRAIN_TIMEOUT_SEC = 5;
    public static final boolean STORE_VALUE_STRINGS = true;
}
