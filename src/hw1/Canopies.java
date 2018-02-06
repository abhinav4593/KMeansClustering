package hw1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Abhinav Gundlapalli
 * agundlapalli@uh.edu
 *
 */
public class Canopies {

	public static class CanopyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, String> canopyCenters = new HashMap<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			String[] v_array = v.split("[\t]");

			String itemID = v_array[0];
			String userIDRatingString = v_array[1];

			if (canopyCenters.containsKey(itemID))
				return;

			if (canopyCenters.isEmpty()) {
				canopyCenters.put(itemID, userIDRatingString);
				return;
			}

			ArrayList<String> list2 = MapperSupporter.getListOfUsers(userIDRatingString);
			boolean shouldBeAdded = true;
			for (String k : canopyCenters.keySet()) {
				String users = canopyCenters.get(k);
				ArrayList<String> list1 = MapperSupporter.getListOfUsers(users);

				if (MapperSupporter.getCommontUsrCount(list1, list2) >= 8) {
					shouldBeAdded = false;
					break;
				}
			}

			if (shouldBeAdded)
				canopyCenters.put(itemID, userIDRatingString);

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			for (String k : canopyCenters.keySet()) {
				String users = canopyCenters.get(k);

				context.write(new Text("1"), new Text(k + "===" + users));
			}

			super.cleanup(context);
		}

	}

	public static class CanopyReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, String> canopyCenters = new HashMap<>();
			for (Text value : values) {

				String[] v = value.toString().split("===");

				String itemID = v[0];
				String userIDRatingString = v[1];

				if (canopyCenters.isEmpty()) {
					canopyCenters.put(itemID, userIDRatingString);
					continue;
				}

				ArrayList<String> list2 = MapperSupporter.getListOfUsers(userIDRatingString);
				boolean shouldBeAdded = true;
				for (String k : canopyCenters.keySet()) {
					String users = canopyCenters.get(k);
					ArrayList<String> list1 = MapperSupporter.getListOfUsers(users);

					if (MapperSupporter.getCommontUsrCount(list1, list2) >= 8) {
						shouldBeAdded = false;
						break;
					}
				}

				if (shouldBeAdded)
					canopyCenters.put(itemID, userIDRatingString);
			}

			for (String k : canopyCenters.keySet())
				context.write(new Text(k), new Text(canopyCenters.get(k)));

		}

	}
	
	
	public static class ProductToCanopiesMapper extends Mapper<LongWritable, Text, Text, Text> {

		private int count;

		private String iID;
		private String usrID;
		private String score;

		public ProductToCanopiesMapper() {
			count = 0;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			String v = value.toString();

			if (value.find("product/productId:") >= 0)
				iID = v.split(" ")[1];
			else if (value.find("review/userId:") >= 0)
				usrID = v.split(" ")[1];
			else if (value.find("review/score:") >= 0)
				score = v.split(" ")[1];

			if (count == 11) {
				context.write(new Text(iID), new Text(String.format("{%s,%s}", usrID, score)));
			}
		}

	}

	public static class ProductToCanopiesReducer extends Reducer<Text, Text, Text, Text> {

		private static final String CANOPY_FILENAME = "canopy/part-r-00000";
		HashMap<String, String> canopy = null;

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			canopy = MapperSupporter.readFileSeq(CANOPY_FILENAME);

			super.setup(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sum = new String();

			ArrayList<String> userList = new ArrayList<>();
			for (Text text : values) {
				String v = text.toString();
				userList.add(v.split(",")[0]);

				sum = sum + text.toString() + ";";
			}

			ArrayList<String> canopyList = new ArrayList<>();

			for (String k : canopy.keySet()) {
				ArrayList<String> canopyListofUsers = new ArrayList<>();

				String v = canopy.get(k);
				String[] v_arr = v.split(";");
				for (int i = 0; i < v_arr.length; i++) {
					if (v_arr[i].trim().length() == 0)
						continue;
					canopyListofUsers.add(v_arr[i].trim().split(",")[0]);
				}

				if (MapperSupporter.getCommontUsrCount(canopyListofUsers, userList) >= 2)
					canopyList.add(k);

			}
			
			if(!canopyList.isEmpty()) {
				sum = sum + "\t{";
				for (String t : canopyList)
					sum = sum + t + "-";
				sum = sum + "}";
	
				context.write(key, new Text(sum));
			}
		}

	}

}
