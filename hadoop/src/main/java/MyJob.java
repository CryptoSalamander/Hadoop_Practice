import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MyJob extends Configured implements Tool{
    public static class MapClass extends MapReduceBase
        implements Mapper<Text,Text,Text,Text>{
    	
    	
        public void map(Text key, Text value, OutputCollector<Text,Text> output,
                Reporter rep) throws IOException {
            // TODO Auto-generated method stub
            output.collect(value, key);
        }
	}
    
    

	public static class Reduce extends MapReduceBase
        implements Reducer<Text,Text,Text,Text>{
		int mostnum = 0;//가장 큰 인용 숫자
		String mostcited;// 가장 많이 인용된 특허 번호
		int leastnum = 1000000;//가장 작은 인용 숫자(초기값을 일부로 크게 주었음)
		String leastcited;//가장 적게 인용된 특허 번호
		int keynum = 0;//총 특허 키의 수
		int citedsum = 0;//총 인용 수
		double mean = 0;//평균 인용

        public void reduce(Text key, Iterator<Text> values,
                OutputCollector<Text,Text> output, Reporter rep)
                throws IOException {
            // TODO Auto-generated method stub
            String mystring = "";
            int num = 0;
            while (values.hasNext()){
                if(mystring.length() > 0 ) mystring += ",";
                mystring += values.next().toString();
                num++;
            }
            if(mostnum < num)
            {
            	mostnum = num;
            	mostcited = key.toString();
            }
            else if(num < leastnum)
            {
            	leastnum = num;
            	leastcited = key.toString();
            }
            keynum++;
            citedsum += num;
            if(keynum != 0)
            {
            	mean = (double)citedsum/(double)keynum;
            }
            mystring += " mostcited : "+mostcited+" mostnum :"+mostnum+" leastcited :"+leastcited+" leastnum : "+leastnum+" Mean : "+mean;
            output.collect(key, new Text(mystring));
        }
    }
    
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, MyJob.class);
        
        Path in = new Path(arg0[0]);
        Path out = new Path(arg0[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("cite20150291");
        job.setJarByClass(MyJob.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", ",");
        JobClient.runJob(job);
        return 0;
    }
    
    public static void main( String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }

}