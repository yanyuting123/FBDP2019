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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DifferenceSet {

    public static class DifferenceSetMapper extends Mapper<Object, Text, RelationA, Text>{
        Text file_sign = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("1")){
                file_sign.set("1");
                context.write(record, file_sign);
            }
            else if(fileName.contains("2")){
                file_sign.set("2");
                context.write(record, file_sign);
            }
        }
    }

    public static class DifferenceSetReducer extends Reducer<RelationA, Text, RelationA, NullWritable> {

        public void reduce(RelationA key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException{
            String file_signs = "";
            for(Text val:value){
                file_signs = file_signs.concat(val.toString());
            }
            if(file_signs.contains("2") && !file_signs.contains("1")){
                context.write(key,NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "intersection");
        job.setJarByClass(DifferenceSet.class);
        job.setMapperClass(DifferenceSetMapper.class);
        job.setReducerClass(DifferenceSetReducer.class);
        job.setMapOutputKeyClass(RelationA.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(RelationA.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
