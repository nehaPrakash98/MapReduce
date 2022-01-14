package reduce;

import model.Pair;

import java.util.List;

//A common interface for all reduce user functions to implement
public interface ReducerInterface<K,V> {
    Pair<K, V> reduce(Pair<K, List<V>> p);
}
