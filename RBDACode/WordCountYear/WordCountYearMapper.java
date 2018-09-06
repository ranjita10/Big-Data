import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountYearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().toLowerCase().split("\t");
        String text = line[0];
        int year = Integer.parseInt(line[1]);

        StringTokenizer words = new StringTokenizer(text);

        while (words.hasMoreTokens()) {
            context.write(new Text(words.nextToken()), new IntWritable(year));
        }
    }
}
