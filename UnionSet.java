package RelationAlgebra;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UnionSet {

    public static class UnionSetMapper extends Mapper<Object, Text, RelationA, NullWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            context.write(record, NullWritable.get());
        }
    }

    public static class UnionSetReducer extends Reducer<RelationA, NullWritable, RelationA, NullWritable> {

        public void reduce(RelationA key, Iterable<NullWritable> value, Context context)
                throws IOException, InterruptedException{
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "intersection");
        job.setJarByClass(UnionSet.class);
        job.setMapperClass(UnionSetMapper.class);
        job.setReducerClass(UnionSetReducer.class);
        job.setMapOutputKeyClass(RelationA.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(RelationA.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
