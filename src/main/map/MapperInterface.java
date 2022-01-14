package map;

import model.Pair;

import java.util.List;

//A common interface for all map user functions to implement
public interface MapperInterface<K,V> {
    List<Pair> map(Pair<K, V> p);
}
