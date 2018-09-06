import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

public class TFIDFWordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        ArrayList<String> words = new ArrayList<String>();
        ArrayList<Integer> termFrequency = new ArrayList<Integer>();

        String[] wordList = value.toString().toLowerCase().split(" ");

        for (String word : wordList) {
            if (words.contains(word)) {
                termFrequency.set(words.indexOf(word), termFrequency.get(words.indexOf(word)) + 1);
            } else {
                words.add(word);
                termFrequency.add(1);
            }
        }

        for (String word : words) {
            context.write(new Text(word), new Text(key.get() + "\t" + termFrequency.get(words.indexOf(word))));
        }
    }
}
