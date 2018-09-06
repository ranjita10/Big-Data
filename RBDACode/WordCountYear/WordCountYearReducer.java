import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class WordCountYearReducer extends Reducer<Text, IntWritable, Text, Text> {

    static int firstYear = 1990;

    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int total = 0;
        int[] counts = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0};

        for (IntWritable year : value) {
            total++;
            counts[year.get() % firstYear]++;
        }

        if (total > 100) {
            context.write(key, new Text(Arrays.stream(counts).mapToObj(String::valueOf).collect(Collectors.joining(", "))));
        }
    }
}
