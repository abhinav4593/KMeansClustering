package hw1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapperSupporter {
	public static ArrayList<String> getListOfUsers(String userIDRatingString) {
		ArrayList<String> list = new ArrayList<>();
		String[] tuples = userIDRatingString.split(";");
		for (int i = 0; i < tuples.length; i++) {
			if (tuples[i].length() == 0)
				continue;

			String str = tuples[i].split(",")[0].substring(1);
			list.add(str);
		}

		return list;
	}

	public static ArrayList<String> extractListOfScores(String userIDRatingString) {
		ArrayList<String> list = new ArrayList<>();
		String[] tuples = userIDRatingString.split(";");
		for (int i = 0; i < tuples.length; i++) {
			if (tuples[i].length() == 0)
				continue;

			String str = tuples[i].split(",")[1];
			str = str.substring(0, str.length() - 1);
			list.add(str);
		}

		return list;
	}

	public static int getCommontUsrCount(ArrayList<String> list1, ArrayList<String> list2) {
		int count = 0;

		for (String e1 : list1) {
			for (String e2 : list2) {
				if (e1.trim().compareToIgnoreCase(e2.trim()) == 0)
					count++;
			}
		}

		return count;
	}

	public static HashMap<String, String> readFileSeq(String filename) throws IOException {
		BufferedReader br = openFile(filename);
		
		HashMap<String, String> retMap = new HashMap<>();
		String line;
		line = br.readLine();
		while (line != null) {
			String[] line_array = line.trim().split("[ \t\n]");

			retMap.put(line_array[0], line_array[1]);

			line = br.readLine();
		}

		return retMap;
	}
	
	public static ArrayList<HashMap<String, Double>> readCenterFileSeq(String filename) throws IOException {
		BufferedReader br = openFile(filename);
		
		ArrayList<HashMap<String, Double>> retArr = new ArrayList<HashMap<String,Double>>();
		String line;
		line = br.readLine();
		while (line != null) {
			String[] line_array = line.trim().split("[ \t\n]");

			HashMap<String, Double> m = new HashMap<>();
			String[] userIDRatingList = line_array[1].split(";");
			for(int i=0;i<userIDRatingList.length;i++) {
				if(userIDRatingList[i].length() == 0)
					continue;
				
				String userID = userIDRatingList[i].split(",")[0].substring(1);
				String r = userIDRatingList[i].split(",")[1];
				double rating = Double.valueOf(r.substring(0, r.length() - 1));
				
				retArr.add(m);
				
				m.put(userID, rating);
			}

			line = br.readLine();
		}
		
		return retArr;

	}

	private static BufferedReader openFile(String filename) throws UnknownHostException, IOException {
		String hostName = java.net.InetAddress.getLocalHost().getHostName();
		if(hostName.toLowerCase().indexOf("whale") >= 0)
			filename = "hdfs:/cloudc16/" + filename;
		Path pt = new Path(filename);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		return br;
	}
	
	public static double similarity(HashMap<String, Double> m1, HashMap<String, Double> m2) {
		double num, denum;
		
		num = denum = 0;
		
		for(String k1 : m1.keySet()) {
			Double v1 = m1.get(k1);
			for(String k2 : m2.keySet()) {
				if (k1.compareToIgnoreCase(k2) == 0) {
					Double v2 = m2.get(k2);
					num += v1 * v2;
				}
			}
		}
		
		double d1 = 0;
		for(Double v : m1.values()) 
			d1 += v * v;

		double d2 = 0;
		for(Double v : m2.values()) 
			d2 += v * v;
		
		denum = Math.sqrt(d1) * Math.sqrt(d2);
		
		return num / denum;
	}

	public static HashMap<String, Double> getUserIdRating(String userIDRatingString) {
		String[] userIDRatingList = userIDRatingString.split(";");
		HashMap<String, Double> retMap = new HashMap<>();
		for(int i=0;i<userIDRatingList.length;i++) {
			if(userIDRatingList[i].compareTo("") == 0)
				continue;
			
			String[] q = userIDRatingList[i].split(",");
			String userID = q[0].substring(1);
			String rating = q[1].substring(0, q[1].length()-1);
			
			retMap.put(userID, Double.valueOf(rating));
		}
		return retMap;
	}

}
