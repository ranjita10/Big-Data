import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

public class TFIDFGetWordMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueList = value.toString().split("\t");
        context.write(new Text(valueList[0]), new Text(valueList[1] + "\t" + valueList[2]));


    }
}
