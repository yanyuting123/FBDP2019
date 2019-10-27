package MatrixMultiply1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixMultiply {
    private static int rowM = 0;
    private static int columnM = 0;
    private static int columnN = 0;

    public static class MatrixMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text map_key = new Text();
        private Text map_value = new Text();

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            columnN = Integer.parseInt(conf.get("columnN"));
            rowM = Integer.parseInt(conf.get("rowM"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("M")){
                String[] tuple = value.toString().split("[, \t]");
                int i = Integer.parseInt(tuple[0]);    //此处i, j 也可不转成int?
                int j = Integer.parseInt(tuple[1]);
                int Mij = Integer.parseInt(tuple[2]);
                for(int k = 1; k <= columnN; k++){
                    map_key.set(i + "," + k);
                    map_value.set("M" + "," + j + "," + Mij);
                    context.write(map_key, map_value);
                }
            }
            else if(fileName.contains("N")){
                String[] tuple = value.toString().split("[, \t]");
                int j = Integer.parseInt(tuple[0]);
                int k = Integer.parseInt(tuple[1]);
                int Njk = Integer.parseInt(tuple[2]);
                for(int i = 1; i <= rowM; i++){
                    map_key.set(i + "," + k);
                    map_value.set("N" + "," + j + "," + Njk);
                    context.write(map_key, map_value);
                }
            }
        }

    }


    public static class MatrixReducer
            extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void setup(Context context) throws IOException{
            Configuration conf = context.getConfiguration();
            columnM = Integer.parseInt(conf.get("columnM"));
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int[] M = new int[columnM + 1];
            int[] N = new int[columnM + 1];
            for(Text val:values){
                String[] tuple = val.toString().split(",");
                if(tuple[0].equals("M")){
                    M[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
                }
                else{
                    N[Integer.parseInt(tuple[1])] = Integer.parseInt(tuple[2]);
                }
            }
            for(int j = 1; j <= columnM; j++) {
                sum += M[j] * N[j];
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiply <inputPathM> <inputPathN> <outputPath>");
            System.exit(2);
        } else {
            String[] infoTupleM = args[0].split("_");
            rowM = Integer.parseInt(infoTupleM[1]);
            columnM = Integer.parseInt(infoTupleM[2]);
            String[] infoTupleN = args[1].split("_");
            columnN = Integer.parseInt(infoTupleN[2]);
        }

        Configuration conf = new Configuration();
        /** 设置三个全局共享变量 **/
        conf.setInt("rowM", rowM);
        conf.setInt("columnM", columnM);
        conf.setInt("columnN", columnN);

        Job job = Job.getInstance(conf, "matrix multiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
