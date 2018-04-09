import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
public class TSP {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path graph = new Path(args[2]);
        conf.set("graph.path", graph.toString());
        conf.set("limit", args[3]);

        Job job = Job.getInstance(conf);

        job.setMapperClass(TSPMapper.class);
        job.setReducerClass(TSPReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)? 0: 1);
    }
}
