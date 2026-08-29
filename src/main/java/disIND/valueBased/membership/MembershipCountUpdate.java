package disIND.valueBased.membership;

final class MembershipCountUpdate {

    enum Transition {
        NONE, ADDED, REMOVED
    }

    record Result(int updatedCount, Transition transition) {
    }

    private MembershipCountUpdate() {
    }

    static Result apply(int valueId, int columnId, int previousCount, int delta) {

        if (previousCount < 0)
            throw new IllegalArgumentException("previousCount must not be negative: ");
        int updatedCount = Math.addExact(previousCount, delta);
        if (updatedCount < 0)
            throw new IllegalStateException("Membership count became negative");

        Transition transition;

        if (previousCount == 0 && updatedCount > 0)
            transition = Transition.ADDED;
        else if (previousCount > 0 && updatedCount == 0)
            transition = Transition.REMOVED;
        else
            transition = Transition.NONE;
        return new Result(updatedCount, transition);
    }
}