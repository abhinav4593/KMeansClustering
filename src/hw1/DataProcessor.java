package hw1;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Abhinav Gundlapalli
 * agundlapalli@uh.edu
 *
 */
public class DataProcessor {
	
	public static class AggregateDataMapper extends Mapper<LongWritable, Text, Text, Text> {

		private int count;

		private String itemID;
		private String userID;
		private String score;

		public AggregateDataMapper() {
			count = 0;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			String v = value.toString();

			if (value.find("product/productId:") >= 0)
				itemID = v.split(" ")[1];
			else if (value.find("review/userId:") >= 0)
				userID = v.split(" ")[1];
			else if (value.find("review/score:") >= 0)
				score = v.split(" ")[1];

			if (count == 11) {
				context.write(new Text(itemID), new Text(String.format("{%s,%s}", userID, score)));
			}
		}

	}

	public static class AggregateDataReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String s = new String();

			for (Text text : values) {
				s = s + text.toString() + ";";
			}
			context.write(key, new Text(s));
		}
	}

}
