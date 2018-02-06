package hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hw1.Canopies.CanopyMapper;
import hw1.Canopies.CanopyReducer;
import hw1.DataProcessor.AggregateDataMapper;
import hw1.DataProcessor.AggregateDataReducer;
import hw1.KMeansIteration.FinalMapper;
import hw1.KMeansIteration.FinalReducer;
import hw1.KMeansIteration.KMeansMapper;
import hw1.KMeansIteration.KMeansReducer;
import hw1.Canopies.ProductToCanopiesMapper;
import hw1.Canopies.ProductToCanopiesReducer;

/**
 * @author Abhinav Gundlapalli
 * agundlapalli@uh.edu
 *
 */
public class AmazonReviewProcessor extends Configured implements Tool {

	public int formatData(String inputFile, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(new Configuration());

		job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 11);

		//System.out.format("--- NumLinesPerSplit: %d\n", NLineInputFormat.getNumLinesPerSplit(job));

		job.setJarByClass(AmazonReviewProcessor.class);

		job.setMapperClass(AggregateDataMapper.class);

		job.setReducerClass(AggregateDataReducer.class);

		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFile));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int SelectCanopy(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(AmazonReviewProcessor.class);

		job.setMapperClass(CanopyMapper.class);

		job.setReducerClass(CanopyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int ptc(String inputFolder, String outputFolder) throws Exception {

		Job job = new Job(new Configuration());
		job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 11);
		System.out.format("--- NumLinesPerSplit: %d\n", NLineInputFormat.getNumLinesPerSplit(job));
		job.setJarByClass(AmazonReviewProcessor.class);
		job.setMapperClass(ProductToCanopiesMapper.class);
		job.setReducerClass(ProductToCanopiesReducer.class);
		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int KMeans(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(AmazonReviewProcessor.class);

		job.setMapperClass(KMeansMapper.class);

		job.setReducerClass(KMeansReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	public int finalRun(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(AmazonReviewProcessor.class);

		job.setMapperClass(FinalMapper.class);

		job.setReducerClass(FinalReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String hostName = java.net.InetAddress.getLocalHost().getHostName();
		String root = "";
		if (hostName.toLowerCase().indexOf("whale") >= 0)
			root = "/cloudc16/";
		
		formatData(args[0], root + "formatdata/");
		SelectCanopy(root + "formatdata/", root + "canopy/");
		ptc(args[0], root + "product/");
		KMeans(root + "product/", root + "final/");
		finalRun(root + "formatdata/", args[1]);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new AmazonReviewProcessor(), args);
	}

}
