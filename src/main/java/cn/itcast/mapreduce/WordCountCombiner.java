package cn.itcast.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Combiner 组件在map阶段完成聚合，减少往reduce传送流量（此时类似reduce）
 *
 */
public class WordCountCombiner  extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int count =0;
		for(IntWritable v :values){
			count += v.get();
		}
		
		context.write(key, new IntWritable(count));
	}

}
