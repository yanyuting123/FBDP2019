package RelationAlgebra;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Intersection {

    public static class IntersectionMapper extends Mapper<Object, Text, RelationA, IntWritable>{
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            context.write(record, one);
        }
    }

    public static class IntersectionReducer extends Reducer<RelationA, IntWritable, RelationA, NullWritable> {

        public void reduce(RelationA key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val:value){
                sum += val.get();
            }
            if(sum == 2){
                context.write(key,NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "intersection");
        job.setJarByClass(Intersection.class);
        job.setMapperClass(Intersection.IntersectionMapper.class);
        job.setReducerClass(Intersection.IntersectionReducer.class);
        job.setMapOutputKeyClass(RelationA.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(RelationA.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
