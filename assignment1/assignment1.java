//Author: Chang Wang
//I test for the first 1000 rows data as input and got 54341 rows output.
//One row of output is "(2582846,2872718)	[(57,9,9),(56,9,8),(45,4,8)]". The format is correct and could find different users' rate.
//In this assignment I finished it by 2 steps which are job1 and job2. For job1, I deal with the original data and knows every users'rate for movies.
//Class MyMapper1 and MyReducer1 split "::", regards user as output key and put movie and rate in a MapWritable as output value.
//For job2, I got the final output.
//Class MyMapper2 used job1's output as input. After sorting the list of users I used for loop to get movies id and hashMap to find rates.
//Class UserAndRate is the output value of Class MyReducer2 and these could get the correct output.
//Class MovieID not only get pairs of movies id but also compared them to guarantee the key of output correct.
//Class RateByUser could get user id and rates for pairs of movies, and format them as correct output value.
//At last I deleted the output file of job1, the output of job2 which in the output folder is the final output.
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class AssigOne {
	
	public static class MyMapper1 extends Mapper<LongWritable, Text, Text, MapWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			String[] splitStrings = value.toString().split("::");
			MapWritable movieAndrate = new MapWritable();
			Text movie_1 = new Text(splitStrings[1]);
			IntWritable rate_1 = new IntWritable(Integer.valueOf(splitStrings[2]));
			Text user_1 = new Text(splitStrings[0]);
			movieAndrate.put(movie_1, rate_1);
			context.write(user_1, movieAndrate);
		}
		
	}
	
	public static class MyReducer1 extends Reducer<Text, MapWritable, Text, MapWritable> {

		@Override
		protected void reduce(Text user_1, Iterable<MapWritable> movieAndrate,
				Reducer<Text, MapWritable, Text, MapWritable>.Context context) throws IOException, InterruptedException {
			MapWritable idAndrate = new MapWritable();
			for(MapWritable v: movieAndrate) {
				idAndrate.putAll(v);
			}
			context.write(user_1, idAndrate);
		}
	}
	
	
	public static class MovieID implements WritableComparable {
		private Text m1;
		private Text m2;
		public MovieID() {
			m1 = new Text("");
			m2 = new Text("");
			
		}

		public Text getM1() {
			return m1;
		}

		public void setM1(Text m1) {
			this.m1 = m1;
		}

		public Text getM2() {
			return m2;
		}

		public void setM2(Text m2) {
			this.m2 = m2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.m1.readFields(data);
			this.m2.readFields(data);		
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.m1.write(data);
			this.m2.write(data);
		}

		@Override
		public int compareTo(Object o) {
			if(this.m1.compareTo(((MovieID)o).m1) == 0) {
				return this.m2.compareTo(((MovieID)o).m2);
			}else {
				return this.m1.compareTo(((MovieID)o).m1);
			}
		}
		
		@Override
		public String toString() {
			return "(" + this.m1.toString() + "," + this.m2.toString() + ")";
			
		}
	}
	
	public static class RateByUser implements Writable {
		private Text user;
		private IntWritable r1;
		private IntWritable r2;
		
		public RateByUser() {
			user = new Text("");
			r1 = new IntWritable(-1);
			r2 = new IntWritable(-1);
		}

		public RateByUser(RateByUser v) {
			user = new Text(v.getUser().toString());
			r1 = new IntWritable(v.getR1().get());
			r2 = new IntWritable(v.getR2().get());
		}

		public Text getUser() {
			return user;
		}

		public void setUser(Text user) {
			this.user = user;
		}

		public IntWritable getR1() {
			return r1;
		}

		public void setR1(IntWritable r1) {
			this.r1 = r1;
		}

		public IntWritable getR2() {
			return r2;
		}

		public void setR2(IntWritable r2) {
			this.r2 = r2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.r1.readFields(data);
			this.r2.readFields(data);
			
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.r1.write(data);
			this.r2.write(data);
			
		}
		
		@Override
		public String toString() {
			return "(" + this.user.toString() + "," + String.valueOf(this.r1.get()) + "," + String.valueOf(this.r2.get()) + ")";
			
		}
		
	}
	
	public static class MyMapper2 extends Mapper<Text, MapWritable, MovieID, RateByUser> {

		@Override
		protected void map(Text user_1, MapWritable idAndrate, Mapper<Text, MapWritable, MovieID, RateByUser>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrayList = new ArrayList<String>();
			HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
			for(Entry<Writable, Writable> entry: idAndrate.entrySet()) {
				arrayList.add(((Text)entry.getKey()).toString());
				hashMap.put(((Text)entry.getKey()).toString(), ((IntWritable)entry.getValue()).get());
			}
			Collections.sort(arrayList);
			for(int i = 0; i < arrayList.size(); i++) {
				for(int j = i + 1; j < arrayList.size(); j++) {
					Text movie1 = new Text(arrayList.get(i));
					Text movie2 = new Text(arrayList.get(j));
					MovieID movieID = new MovieID();
					movieID.setM1(movie1);
					movieID.setM2(movie2);
					RateByUser rateByUser = new RateByUser();
					rateByUser.setUser(user_1);
					IntWritable rate1 = new IntWritable(hashMap.get(arrayList.get(i)));
					rateByUser.setR1(rate1);
					IntWritable rate2 = new IntWritable(hashMap.get(arrayList.get(j)));
					rateByUser.setR2(rate2);
					context.write(movieID, rateByUser);
				}
			}
		}
		
	}
	
	public static class UserAndRate extends ArrayWritable {

		public UserAndRate(Class<? extends Writable> valueClass) {
			super(valueClass);
			// TODO Auto-generated constructor stub
		}
		@Override
		public String toString() {
			String[] results = this.toStrings();
			return "[" + String.join(",", results) + "]";
		}
	
	}
	
	public static class MyReducer2 extends Reducer<MovieID, RateByUser, MovieID, UserAndRate> {

		@Override
		protected void reduce(MovieID movieID, Iterable<RateByUser> rateByUser,
				Reducer<MovieID, RateByUser, MovieID, UserAndRate>.Context context) throws IOException, InterruptedException {
			UserAndRate userAndrate = new UserAndRate(RateByUser.class);
			ArrayList<RateByUser> arrayList = new ArrayList<RateByUser>();
			for(RateByUser v: rateByUser) {
				RateByUser user_rate = new RateByUser(v);
				arrayList.add(user_rate);
			}
			RateByUser[] user_rates = arrayList.toArray(new RateByUser[arrayList.size()]);
			userAndrate.set(user_rates);
			context.write(movieID, userAndrate);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		Job job1 = Job.getInstance(conf, "job1");
		job1.setJarByClass(AssigOnez5196324.class);
		job1.setReducerClass(MyReducer1.class);
		job1.setMapperClass(MyMapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MapWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MapWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1], "job1_output"));
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		Job job2 = Job.getInstance(conf, "job2");
		job2.setJarByClass(AssigOnez5196324.class);
		job2.setReducerClass(MyReducer2.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(MovieID.class);
		job2.setMapOutputValueClass(RateByUser.class);
		job2.setOutputKeyClass(MovieID.class);
		job2.setOutputValueClass(UserAndRate.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[1], "job1_output"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1], "output"));
		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		fs.delete(new Path(args[1], "job1_output"), true);
	}
}
