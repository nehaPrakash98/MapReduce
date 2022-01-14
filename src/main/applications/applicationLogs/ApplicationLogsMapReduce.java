package applications.applicationLogs;

import map.MapperInterface;
import model.Pair;
import reduce.ReducerInterface;

import java.util.ArrayList;
import java.util.List;

public class ApplicationLogsMapReduce implements MapperInterface, ReducerInterface {

    public static final int NOT_FOUND_ERROR = 404;
    public static final int NO_SERVER_RESPONSE = 500;
    public static final int NOT_MODIFIED_ERROR = 304;

    @Override
    public List<Pair> map(Pair p) {
        String content = p.getKey().toString();
        String[] words = content.split(" ");

        List<Pair> pairs = new ArrayList<>();

        for(String word: words) {

            try {
                int num = Integer.parseInt(word);
                if(Integer.parseInt(word) == NOT_FOUND_ERROR ||
                        Integer.parseInt(word) == NO_SERVER_RESPONSE ||
                        Integer.parseInt(word) == NOT_MODIFIED_ERROR)
                pairs.add(new Pair(word, 1));
            } catch (Exception e) {

            }
        }

        return pairs;
    }

    @Override
    public Pair reduce(Pair p) {
        List<String> val = (List<String>) p.getValue();
        Integer sum = 0;
        for(String s: val) {
            sum += Integer.parseInt(s);
        }
        return new Pair(p.getKey(), sum.toString());
    }
}
