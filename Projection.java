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

public class Projection {

    public static class ProjectionMapper extends Mapper<Object, Text, Text, NullWritable>{
        private String name;
        private Text map_key = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            name = record.getName();
            map_key.set(name);
            context.write(map_key,NullWritable.get());
        }
    }

    public static class ProjectionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> value, Context context)
                throws IOException, InterruptedException{
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "projection");
        job.setJarByClass(Projection.class);
        job.setMapperClass(ProjectionMapper.class);
        job.setReducerClass(ProjectionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
