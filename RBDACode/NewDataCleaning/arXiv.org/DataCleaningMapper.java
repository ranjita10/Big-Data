import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DataCleaningMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] valueList = value.toString().split(",");
        String summary = valueList[3];
        String year = valueList[4];

        context.write(new Text(summary), new Text(year));
    }
}
