import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by colntrev on 4/8/18.
 * This mapper implements the logic from the open source code available from the authors of
 *  Ant System: Optimization by a Colony of Cooperating Agents
 *  This code is for use in testing a MASS adaptation of the algorithm described in the above paper
 *
 *  MapReduce implementation was inspired by a similar approach to solving kmeans clustering using
 *  a batch based approach. Each mapper executes Ant TSP and a reducer task finds the best tour from all mappers.
 */
public class TSPReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer total = 0;
        String tour = null;
        char delimiter = ' ';
        Double tourLength = -1.0; // set to negative since we will have no negative tour lengths
        for(Text value : values){
            total++;
            String[] results = value.toString().split(" ");
            double length = Double.parseDouble(results[1]);
            if(tourLength == -1 || length < tourLength){
                tourLength = length;
                tour = results[0];
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Tour:");
        sb.append(delimiter);
        sb.append(tour);
        sb.append(delimiter);
        sb.append("Length:");
        sb.append(delimiter);
        sb.append(tourLength.toString());
        context.write(new IntWritable(total), new Text(sb.toString()));
    }
}
