package Experiment3;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jasper.tagplugins.jstl.core.Out;

import static Experiment3.ProductConcernCount.productConcernMain;
import static Experiment3.ProductPurchaseCount.productPurchaseMain;
/* 第一个mapreduce仅输出了省份，产品id和关注量，但未进行排序。第二个mapreduce对结果按省份+关注量筛选，输出每个省份的前十。
 * Mapper和Reducer中利用Map存储省份名和对应的前十产品，利用优先队列存储各省份的前十产品，优先队列实现按照关注量排序，
 * 相当于每个map先筛选一次局部数据中的前十，再交给一个reduce，筛选全局数据各省份的前十。
 * 当优先队列存储的键值对数大于10时，取出排在队头的（同地区关注量最少的）元素进行删除。
 * 但不尽如人意的地方在于，因为使用优先队列，只能将队头出队，最后无法实现按关注量倒序排序，而是正序排序的。
 * //一开始尝试使用了TreeMap存储每个省份的前十并筛选，但TreeMap只能根据key来排序，而关注量不能作为key，会有关注量相同的数据互相覆盖的问题。
 * */
public class DescSort {

    public static class DescSortMapper extends Mapper<Text, Text, Area_Number, IntWritable> {
        public Area_Number map_key;
        public IntWritable map_value;
        public static Map<String, PriorityQueue<OutInfo>> outInfo = new HashMap<>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            map_key = new Area_Number(key.toString());
            map_value = new IntWritable(Integer.parseInt(value.toString()));
            String area = map_key.getArea();
            if (!outInfo.containsKey(area)) {
                outInfo.put(area, new PriorityQueue<>());
            }
            outInfo.get(area).add(new OutInfo(map_key, map_value));
            if (outInfo.get(area).size() > 10) {
                outInfo.get(area).remove();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            OutInfo info = null;
            for (PriorityQueue<OutInfo> v : outInfo.values()) {
                while ((info = v.poll()) != null) {
                    context.write(info.getArea_itemId(), info.getCount());
                }
            }
        }
    }

    public static class DescSortReducer extends Reducer<Area_Number, IntWritable, Area_Number, IntWritable> {
        public static Area_Number reduce_key;
        public static IntWritable reduce_value;
        public static Map<String, PriorityQueue<OutInfo>> outInfo = new HashMap<>();

        public void reduce(Area_Number key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            reduce_key = new Area_Number(key.toString());
            reduce_value = new IntWritable(value.iterator().next().get());
            String area = reduce_key.getArea();
            if (!outInfo.containsKey(area)) {
                outInfo.put(area, new PriorityQueue<>());
            }
            outInfo.get(area).add(new OutInfo(reduce_key, reduce_value));
            if (outInfo.get(area).size() > 10) {
                outInfo.get(area).remove();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            OutInfo info = null;
            for (PriorityQueue<OutInfo> v : outInfo.values()) {
                while ((info = v.poll()) != null) {
                    Area_Number key = new Area_Number(info.getArea_itemId().getArea(),info.getCount().get());
                    IntWritable value = new IntWritable(info.getArea_itemId().getProduct_id());
                    context.write(key, value);
                }
            }
        }
    }
        public static void main(String[] args) throws Exception {
            //0:input path for count; 1:output path for count i.e. input path for sort; 2:output path for sort 3:concern/purchase
            if (args[3].equals("concern")) productConcernMain(args);
            else if (args[3].equals("purchase")) productPurchaseMain(args);
            else {
                System.out.println("参数输入错误！请输入 concern 或 purchase\n");
                System.exit(-1);
            }
            String inputPath = args[1];
            String outputPath = args[2];
            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", ",");
            Job job = Job.getInstance(conf, "ProductConcernSortResult");
            job.setJarByClass(DescSort.class);
            job.setNumReduceTasks(1);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapperClass(DescSortMapper.class);
            job.setReducerClass(DescSortReducer.class);
            job.setMapOutputKeyClass(Area_Number.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Area_Number.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            System.out.println(job.waitForCompletion(true) ? "success" : "failure");
        }
}
