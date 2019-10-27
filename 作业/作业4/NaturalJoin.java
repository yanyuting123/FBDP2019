package RelationAlgebra;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class NaturalJoin {

    public static class NaturalJoinMapper extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable id = new IntWritable();
        private Text map_value = new Text();    //<file_sign>,<attribute1>, <attribute2>,...
        private Text file_sign = new Text();    //{fileA, fileB}

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("a")){
                file_sign.set("fileA");
            }
            else if(fileName.contains("b")){
                file_sign.set("fileB");
            }
            String[] tuple = value.toString().split(",");
            id.set(Integer.parseInt(tuple[0]));
            map_value.set(file_sign.toString() + StringUtils.replaceOnce(value.toString(),tuple[0],""));
            context.write(id, map_value);
        }
    }

    public static class NaturalJoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            String ra = "", rb = "";                                          //relationA, relationB
            for (Text val : value) {
                String file_sign = val.toString().split(",")[0];
                if (file_sign.equals("fileA")) {
                    //ra = val.toString().replace(file_sign, "");
                    ra = val.toString();
                } else if (file_sign.equals("fileB")) {
                    //rb = val.toString().replace(file_sign, "");
                    rb = val.toString();
                }
            }
            Text record = new Text();
            if(ra.isEmpty()) record.set(rb);
            else if(rb.isEmpty()) record.set(ra);
            else{
                String[] tuple_a = ra.split(",");
                String[] tuple_b = rb.split(",");
                record.set(tuple_a[1] + "," + tuple_a[2] + "," + tuple_b[1] + "," + tuple_a[3] + "," + tuple_b[2]);
            }
            context.write(key, record);
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /*把输出分隔符设置为逗号*/
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator",",");
        Job job = Job.getInstance(conf, "intersection");
        job.setJarByClass(RelationAlgebra.NaturalJoin.class);
        job.setMapperClass(NaturalJoinMapper.class);
        job.setReducerClass(NaturalJoinReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
