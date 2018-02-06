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
public class KMeansIteration {

	private static final String CANOPY_FILENAME = "product/part-r-00000";
	
	public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private HashMap<String, HashMap<String, Double>> Canopies = new HashMap<>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			HashMap<String, String> allRecords = MapperSupporter.readFileSeq(CANOPY_FILENAME);
			
			for(String k : allRecords.keySet()) {
				Canopies.put(k, MapperSupporter.getUserIdRating(allRecords.get(k)));
			}
			
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("\t");
			
			String itemID = v[0];
			String userIDRatingString = v[1];
			String canopiesString = v[2].substring(1, v[2].length()-2);
			
			String[] loadedMovie_canopyList = canopiesString.split("-");
			HashMap<String, Double> loadedMovie_scoreVector = MapperSupporter.getUserIdRating(userIDRatingString);
			
			double maxCosine = -1;
			String maxCosine_Center = new String("");
			
			for(int i=0;i<loadedMovie_canopyList.length;i++) {
				String c = loadedMovie_canopyList[i];
				
				if(Canopies.containsKey(c)) {
					HashMap<String, Double> hm = Canopies.get(c);
					
					double cs = MapperSupporter.similarity(hm, loadedMovie_scoreVector);
					if(cs > maxCosine) {
						maxCosine = cs;
						maxCosine_Center = c;
					}
				}
			}
			
			context.write(new Text(maxCosine_Center), new Text(itemID + ";" + userIDRatingString));
		}


	}

	public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, HashMap<String, Double>> allCanopies = new HashMap<>();
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			HashMap<String, String> allRecords = MapperSupporter.readFileSeq(CANOPY_FILENAME);
			
			for(String k : allRecords.keySet()) {
				allCanopies.put(k, MapperSupporter.getUserIdRating(allRecords.get(k)));
			}

			super.setup(context);
		}

		public void reduce(Text centerID, Iterable<Text> itemsInCluster, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> userID_avg = new HashMap<>();
			HashMap<String, Double> userID_count = new HashMap<>();
			for (Text t : itemsInCluster) {
				String[] v = t.toString().split(";");
				String itemID = v[0];
				String userIDRatingString = v[1];
				
				HashMap<String, Double> extractUserIDRating = MapperSupporter.getUserIdRating(userIDRatingString);
				
				for(String k : extractUserIDRating.keySet()) {
					if(userID_avg.containsKey(k)) {
						double sum = userID_avg.get(k) * userID_count.get(k) + extractUserIDRating.get(k);
						double count = userID_count.get(k) + 1;
						userID_avg.put(k, sum / count);
						userID_count.put(k, count);
					} else {
						userID_avg.put(k, extractUserIDRating.get(k));
						userID_count.put(k, 1.0);
					}
				}
			}
			
			String str = new String();
			for(String k : userID_avg.keySet()) {
				double cs = userID_avg.get(k);
				if(cs > 0.0)
					str = str + String.format("{%s,%.3f};", k, cs);
			}
			
			if(str.length() > 0)
				context.write(new Text("1"), new Text(str));
		}
	}
	
	
private static final String FileNamePath = "final/part-r-00000";
	
	public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private ArrayList<HashMap<String, Double>> centers = new ArrayList<HashMap<String,Double>>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			centers = MapperSupporter.readCenterFileSeq(FileNamePath);
			
			super.setup(context);
		}
		
		private String convertUserIDRatingToString(HashMap<String, Double> m) {
			String str = new String();
			
			for(String userID : m.keySet()) {
				str = str + String.format("{%s,%.3f};", userID, m.get(userID));
			}
			
			return str;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("\t");
			
			String itemID = v[0];
			String userIDRatingString = v[1];
			
			HashMap<String, Double> loadedMovie_scoreVector = MapperSupporter.getUserIdRating(userIDRatingString);
			
			for(HashMap<String, Double> center : centers) {
				double cs = MapperSupporter.similarity(center, loadedMovie_scoreVector);
				
				String centerID = convertUserIDRatingToString(center);
				String loadedMovieStr = convertUserIDRatingToString(loadedMovie_scoreVector);
				
				context.write(new Text(centerID), new Text(String.format("%.3f---%s", cs, loadedMovieStr)));
			}
			
		}


	}

	public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Integer> newCenters = new HashMap<String, Integer>();
		
		public void reduce(Text centerID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double maxScore = -1;
			String newCenter = new String("");
			for(Text t : values) {
				String[] v = t.toString().split("---");
				
				double score = Double.valueOf(v[0]);
				if(score > maxScore) {
					score = maxScore;
					newCenter = v[1];
				}
			}
			
			if(!newCenters.containsKey(newCenter))
				newCenters.put(newCenter, 1);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			for(String nc : newCenters.keySet()) 
				context.write(new Text("1"), new Text(nc));
			
			super.cleanup(context);
		}

	}

}
