package disIND.streamBasedShardedDispatcher.utility;
import disIND.streamBasedShardedDispatcher.model.SharedModel;
import org.slf4j.Logger;


public final class Debug {

    /*
    *
    [TIME] [ACTOR] [ID] [PAIR] [STATE] Message
    [AA][6][-][-] InsertBatch epoch=5 rows=100
    [AA][6][6⊆2][-] Sending CompareBitmap
    [AA][2][6⊆2][-] Comparing bitmap
    [CM][6][6⊆2][REBUILDING] UnaryReport violations=3
    [CM][6][6⊆2][REPLAYING] Buffered LHS replay values=4
    [CM][6][6⊆2][TRACKED] MembershipResult missing=1
    * */

    private Debug() {}


    public enum State {NONE, REBUILDING, REPLAYING, TRACKED_CLEAN, TRACKED_VIOLATING}
    public enum LogType {MESSAGE, FLOW, DELTA, CHECKPOINT, PERF, STATE, INTERNAL}

    public static final boolean ENABLED = true;

    public static final boolean FLOW = true;
    public static final boolean MESSAGE = true;
    public static final boolean CHECKPOINT = true;
    public static final boolean DELTA = true;
    public static final boolean PERF = true;
    public static final boolean STATE = true;
    public static final boolean INTERNAL = true;


    public static String cm() {
        return "CM";
    }

    public static String bd() {
        return "BD";
    }

    public static String app() {
        return "APP";
    }

    public static String guardian() {
        return "GUARDIAN";
    }

    public static String loader() {
        return "LOADER";
    }

    public static String attr() {
        return "ATTR";
    }

    public static String pair(int lhs, int rhs) {
        return lhs + "⊆" + rhs;
    }

    public static String pairTag(SharedModel.UnaryPair pair) {
        return Debug.pair(pair.lhsCol(), pair.rhsCol());
    }

    public static void formLog(Logger log,String type,String actorName,int colId, String pair,String currentState,String msg,
                               Object... args){
        String prefix = String.format(
                "[%s][%s][%d][%s][%s] ",
                type,
                actorName,
                colId,
                pair,
                currentState
        );
        log.info(prefix + msg, args);
    }

    }


