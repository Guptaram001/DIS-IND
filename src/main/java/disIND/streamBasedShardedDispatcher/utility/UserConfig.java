package disIND.streamBasedShardedDispatcher.utility;

public final class UserConfig {
    public static final int MAX_TRACKED_VIOLATIONS = 200;
    public static final float KMV_PRUN_THRESHOLD = 0.7f;
    public static final int CHECKPOINT_INTERVAL = 2;    //Best based on # of table columns
    public static final String inputDir="/Users/gupta/Documents/DIS-IND/data/tpch-1";
    public static final String outputDir=null;
    public static final int BATCH_SIZE=500;
}
