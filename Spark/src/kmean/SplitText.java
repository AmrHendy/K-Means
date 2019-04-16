import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.ArrayList;

// used to split the text line to point = list of doubles
public class SplitText extends FlatMapFunction<String, Double> {
    public Iterable<Double> call(String s) {
        String[] splits = s.split(" ");
        ArrayList<Double> point_values = new ArrayList<>();
        for (String str : splits) {
            point_values.add(Double.valueOf(str));
        }
        return point_values;
    }
}