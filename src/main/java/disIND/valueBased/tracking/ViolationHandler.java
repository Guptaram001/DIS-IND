package disIND.valueBased.tracking;

public interface ViolationHandler {
    default void comparisonRecorder(java.util.function.IntConsumer recorder) {
    }

    default boolean deferCandidate(int lhsCol, int rhsCol) {
        return false;
    }

    void violationCreated(int lhsCol, int rhsCol, int valueId);

    void violationRepaired(int lhsCol, int rhsCol, int valueId);
}
