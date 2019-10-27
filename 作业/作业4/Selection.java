package RelationAlgebra;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Selection {

    public static class SelectionMapper extends Mapper<Object, Text, RelationA, NullWritable>{
        private int age;
        public void setup(Context context) throws IOException, InterruptedException{
            age = context.getConfiguration().getInt("age",0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            /*if(record.getAge() == age){       //获取年龄=18的记录
                context.write(record, NullWritable.get());
            }*/
            if(record.getAge() < age){          //获取年龄<18的记录
                context.write(record, NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("age",18);
        Job job = Job.getInstance(conf, "Selection");
        job.setJarByClass(Selection.class);
        job.setMapperClass(SelectionMapper.class);
        job.setOutputKeyClass(RelationA.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
