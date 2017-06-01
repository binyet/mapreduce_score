package org.apache.hadoop.score;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Score {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		//----------------- 实现map函数---------------------------------------------------------//
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        		String line = value.toString();// 将输入的纯文本文件的数据转化成String
        		StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");// 将输入的数据首先按行进行分割
        		while (tokenizerArticle.hasMoreElements()) { // 分别对每一行进行处理
        			StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken()); // 每行按空格划分
        			String strName = tokenizerLine.nextToken();// 学生姓名部分
        			String strScore = tokenizerLine.nextToken();// 成绩部分
        			Text name = new Text(strName);
        			Text scoreText = new Text(strScore);
//        			System.out.println(strScore);
        			// 输出姓名和成绩
        			context.write(name, scoreText);
        		}
        }
	}
//	private static int lineSum = 0;
//	public static class Map_lines extends Mapper<LongWritable, Text, Text, Text> {
//		//----------------- 实现map函数---------------------------------------------------------//
//        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
//        		String line = value.toString();// 将输入的纯文本文件的数据转化成String
//        		StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");// 将输入的数据首先按行进行分割
//        		while (tokenizerArticle.hasMoreElements()) { // 分别对每一行进行处理
//        			StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken(), " "); // 每行按空格划分
//        			String strName = tokenizerLine.nextToken();// 学生姓名部分
//        			String strScore = tokenizerLine.nextToken();// 成绩部分
//        			Text name = new Text(strName);
//        			Text scoreText = new Text(strScore);
////        			System.out.println(strScore);
//        			// 输出姓名和成绩
//        			lineSum++;
//        			context.write(name, scoreText);
//        		}
//        }
//	}
//	
	public static class ReduceAversingle extends Reducer<Text, Text, Text, Text> {
        // --------------实现reduce函数----------------------------------------------------------------//
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> iterator = values.iterator();
			double sum = 0;
			double average = 0;
			int count = 0;
			while(iterator.hasNext()){
				for(Text val:values){					
					double value=Double.parseDouble(val.toString());
					sum+=value;     //计算总分
					count++;
				}
			}
			average = sum/count;
			context.write(new Text("average : "), new Text(average+""));
			
	    }
	}

//	
//	private static double[] averageSum = new double[10];
//	private static int[] countSum = new int[10];
//	private static double[] scoreSum = new double[10];
//	private static int lineNum = 0;
//	public static class ReduceAverSum extends Reducer<Text, Text, Text, Text> {
//        // --------------实现reduce函数----------------------------------------------------------------//
//		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//			Iterator<Text> iterator = values.iterator();
//			int sub_sum = 0;
//			while(iterator.hasNext()){
//				for(Text val:values){
//					String[] line = val.toString().split("\t");
//					sub_sum = line.length;
//					double sum = 0;
//					double average = 0;
//					int count = 0;
//					for(int i=0;i<line.length;i++){
//						double score = Double.valueOf(line[i]);
//						if(score != -1){
//							scoreSum[i]+=score;
//							countSum[i]++;
////							System.out.println(averageSum[i]+"  "+i+"  "+countSum[i]);
//						}
//					}
//				
//				}
//			}
//			DecimalFormat df   = new DecimalFormat("#.00");
//			String message = "";
//			lineNum++;
//			double dd = 5;
//			String str = "";
//			for(int i=0;i<sub_sum;i++){
//				averageSum[i] = scoreSum[i]/countSum[i];
////				System.out.println(averageSum[i]+"  "+countSum[i]);
////				message +=(i+1);
////				message+=" "+df.format(averageSum[i])+" ";
//				System.out.println("dff:"+lineNum+"   "+lineSum);
//				if(lineNum == lineSum ){
//					String num = (i+1)+"";
//					Text t = new Text(num);
//					Text text = new Text(df.format(averageSum[i]));
//					str +=(t.toString()+text.toString());
//				}
//			}
//			Text t1 = new Text(str);
//			System.out.println(t1.toString());
//			context.write(t1, t1);
//			
//	    }
//	}
//	
	
	private static double maxscore = 0;
	private static Queue<Info> maxInfo = new LinkedList<Info>();
	public static class Reduce_max extends Reducer<Text, Text, Text, Text> {
        // --------------实现reduce函数----------------------------------------------------------------//

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext()){
				for(Text val:values){
					double valDouble = Double.valueOf(val.toString());
					if( valDouble > maxscore){
						Info infoOut ;
						while((infoOut = maxInfo.poll())!=null);
						maxscore = valDouble;
						Info info = new Info(key.toString(), valDouble);
						maxInfo.offer(info);
						
					}
					else if(valDouble == maxscore){
						Info info = new Info(key.toString(), valDouble);
						maxInfo.offer(info);
					}

				}
			}

			Info info ;
			while((info = maxInfo.poll()) != null){
				Text Max = new Text(Double.toString(info.getScore()));
				context.write(new Text(info.getName()), Max);
			}

	    }
	}

	private static double minscore = 100;
	private static Queue<Info> minInfo = new LinkedList<Info>();
	public static class Reduce_min extends Reducer<Text, Text, Text, Text> {
        // --------------实现reduce函数----------------------------------------------------------------//
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iterator = values.iterator();
			while(iterator.hasNext()){
				for(Text val:values){
					double valDouble = Double.valueOf(val.toString());
					if(valDouble < minscore){
						minInfo.clear();
						minscore = valDouble;
						Info info = new Info(key.toString(), valDouble);
						minInfo.offer(info);
					}
					else if(valDouble == minscore){
						Info info = new Info(key.toString(), valDouble);
						minInfo.offer(info);
					}

				}
			}

			Info info ;
			while((info = minInfo.poll()) != null){
				Text Min = new Text(Double.toString(info.getScore()));
				Text t = new Text(info.getName());
				context.write(t, Min);
			}

	    }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
//		if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
//			System.err.println("Usage: score <in> <out> [-skip skipPatternFile]");
//			System.exit(2);
//		}
		
		for(int i=0;i<(remainingArgs.length)/2;i++){//
			Path path = new Path(remainingArgs[2*i+1]);
	        org.apache.hadoop.fs.FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
	        if (fileSystem.exists(path)) {  
	            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
	        } 
		}
		
		Job job_averageSingle = Job.getInstance(conf, "averagesingle");
		job_averageSingle.setJarByClass(Score.class);
		
		job_averageSingle.setMapperClass(Map.class);// 设置Map、Combine和Reduce处理类
		job_averageSingle.setCombinerClass(ReduceAversingle.class);
		job_averageSingle.setReducerClass(ReduceAversingle.class);
		
		job_averageSingle.setOutputKeyClass(Text.class); // 设置输出类型
		job_averageSingle.setOutputValueClass(Text.class);
		
		ControlledJob ctrljob_average = new ControlledJob(conf);
		ctrljob_average.setJob(job_averageSingle);
//		// 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
//		job_average.setInputFormatClass(TextInputFormat.class);
//		// 提供一个RecordWriter的实现，负责数据输出
//		job_average.setOutputFormatClass(TextOutputFormat.class);
         // 设置输入和输出目录
		FileInputFormat.addInputPath(job_averageSingle, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job_averageSingle, new Path(remainingArgs[1]));
		

		
		
		Job job_max = Job.getInstance(conf, "max");
		job_max.setJarByClass(Score.class);
		
		job_max.setMapperClass(Map.class);
		job_max.setCombinerClass(Reduce_max.class);
		job_max.setReducerClass(Reduce_max.class);
		
		job_max.setOutputKeyClass(Text.class); // 设置输出类型
		job_max.setOutputValueClass(Text.class);
		
		ControlledJob ctrljob_max = new ControlledJob(conf);
		ctrljob_max.setJob(job_max);
		FileInputFormat.addInputPath(job_max, new Path(remainingArgs[2]));
		FileOutputFormat.setOutputPath(job_max, new Path(remainingArgs[3]));
		
		
		Job job_min = Job.getInstance(conf, "min");
		job_min.setJarByClass(Score.class);
		
		job_min.setMapperClass(Map.class);
		job_min.setCombinerClass(Reduce_min.class);
		job_min.setReducerClass(Reduce_min.class);
		
		job_min.setOutputKeyClass(Text.class);
		job_min.setOutputValueClass(Text.class);
		
		ControlledJob ctrljob_min = new ControlledJob(conf);
		ctrljob_min.setJob(job_min);
		FileInputFormat.addInputPath(job_min, new Path(remainingArgs[4]));
		FileOutputFormat.setOutputPath(job_min, new Path(remainingArgs[5]));
		
		JobControl jobctrl = new JobControl("control");
		jobctrl.addJob(ctrljob_average);
		jobctrl.addJob(ctrljob_max);
		jobctrl.addJob(ctrljob_min);
		
		Thread t = new Thread(jobctrl);
		t.start();
		while(true){
			if(jobctrl.allFinished()){
				System.out.println(jobctrl.getSuccessfulJobList());
				jobctrl.stop();
				break;
			}
		}
//		System.exit(job_average.waitForCompletion(true) ? 0 : 1);  //提交任务
	}


}
