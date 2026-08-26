package disIND.valueBased.membership;

public interface CandidateSet {

    boolean add(int index);

    boolean contains(int index);

    boolean remove(int index);

    void clear();

    int size();
}