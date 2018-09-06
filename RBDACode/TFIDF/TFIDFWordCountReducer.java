import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

public class TFIDFWordCountReducer extends Reducer<Text, Text, Text, Text> {



    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        double totalNumberOfDocument = 174176.0;
        int DF = 0;
        ArrayList<String> tempValue = new ArrayList<String>();

        for (Text text : value) {
            DF++;

            tempValue.add(text.toString());

        }

        double IDF = Math.log10(totalNumberOfDocument / DF);


        for (String text : tempValue) {

            String[] valueList = text.split("\t");
            int TF = Integer.parseInt(valueList[1]);

            context.write(new Text(valueList[0]), new Text((TF*IDF) + "\t" + key.toString()));
        }







    }
}
