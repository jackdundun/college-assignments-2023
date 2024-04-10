import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
 public static class CasesMapper extends Mapper < Object, Text, Text, Text > {
  public void map(Object key, Text value, Context context)
  throws IOException,
  InterruptedException {
   String record = value.toString(); //Read each record
   String[] parts = record.split(","); // Parse CSV file
   context.write(new Text(parts[1]), new Text("Cases:" + parts[2] )); //Label Cases
  }
 }

 public static class TestsMapper extends Mapper < Object, Text, Text, Text > {
  public void map(Object key, Text value, Context context)
  throws IOException,
  InterruptedException {
   String record = value.toString(); // Read each record
   String[] parts = record.split(","); // Parse CSV File
   context.write(new Text(parts[1]), new Text("Tests:" + parts[2])); // Label Tests
  }
 }

 public static class COVID19Reducer extends Reducer < Text, Text, Text, Text > {
  public void reduce(Text key, Iterable < Text > values, Context context)
  throws IOException,
  InterruptedException {
   String year_week = "";
   double tests_done = 0.0;
   double cases = 0.0;


   for (Text t: values) {
    String parts[] = t.toString().split("  ");
    if (parts[1].equals("Cases")) {
     total_cases += Float.parseFloat(parts[2]); // add up their total
    }
    else if (parts[1].equals("Tests")) {
     date = parts[1]
     tests = Float.parseFloat(parts[2]); // count the number of customers
    }
   }
   String str = String.format("%d %.2f", total_cases, tests);
   context.write(new Text(date), new Text(str));
  }
 }

 public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = new Job(conf, "Reduce-side-Join");
  job.setJarByClass(ReduceJoin.class);
  job.setReducerClass(COVID19Reducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CasesMapper.class);
  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TestsMapper.class);
  Path outputPath = new Path(args[2]);
  FileOutputFormat.setOutputPath(job, outputPath);
  outputPath.getFileSystem(conf).delete(outputPath);
  System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
