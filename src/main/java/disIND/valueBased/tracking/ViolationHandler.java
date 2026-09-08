package disIND.valueBased.tracking;

public interface ViolationHandler {
    void violationCreated(int lhsCol, int rhsCol, int valueId);

    void violationRepaired(int lhsCol, int rhsCol, int valueId);
}
