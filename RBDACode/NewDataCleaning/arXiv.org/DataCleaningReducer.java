import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DataCleaningReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            context.write(key, value.iterator().next());
        
    }
}
