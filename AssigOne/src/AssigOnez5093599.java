import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/*
 * Structure description
 * There are two MapReduce used in the project
 * the chaining mapreduces are connected with SequenceFile.
 * ========================================================================================
 * 
 * (1) Map: This will split the longwritable, and set
 * 			key: userID              ---------Text
 * 			value: (movieID, rate)   ---------MovieRateWritable
 * ========================================================================================
 * 
 * (1) Reduce: This will aggregate by key and produce all the combination of movies.
 * 				In each movie pair, the order should be sorted. 
 * 				An ArrayWritable will be returned.
 * 				eg: there will be (M1,M2) rather than (M2,M1)
 * 			key: userID                                  ---------Text
 * 			value: [(movie_1,movie_2,rate_1,rate_2)]     ---------PairMoviesRatesArrayWritable
 * ========================================================================================
 * 
 * (2) Map: This map will extract the movie pairs from the value, 
 * 			set them as comparable key and combine userID with two corresponding rates.
 * 			key: (movie_1,movie_2)                       ---------MoviePairComparator
 * 			value: (userID,rate_1,rate_2)                ---------UserRatesPairWritable
 * ========================================================================================
 * 
 * (2) Reduce: This will simply aggregate by key and output as ArrayWritable
 * 			key: (movie_1,movie_2)                       ---------MoviePairComparator
 * 			value: [(userID,rate_1,rate_2)]              ---------UserRatesPairArrayWritable
 * ========================================================================================
 * 
 */



public class AssigOnez5093599 {
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	used in first map
	 *	produce a (movie,rate) pair
	 *
	 */
	public static class MovieRateWritable implements Writable {

		private Text movieID;
		private IntWritable rate;

		
		public MovieRateWritable() {
			super();
			this.movieID = new Text("");
			this.rate = new IntWritable(-1);
		}
		
		public MovieRateWritable(Text movieID, IntWritable rate) {
			super();
			this.movieID = movieID;
			this.rate = rate;
		}

		public Text getMovieID() {
			return movieID;
		}

		public void setMovieID(Text movieID) {
			this.movieID = movieID;
		}

		public IntWritable getRate() {
			return rate;
		}

		public void setRate(IntWritable rate) {
			this.rate = rate;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movieID.readFields(data);
			this.rate.readFields(data);			
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movieID.write(data);
			this.rate.write(data);				
		}

		@Override
	    public String toString() {
	    	String strings = "";
	        strings += "(" + this.movieID.toString() + "," + this.rate.toString() + ")";
	      return strings;
		}
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	do the combination over all the movies and corresponding rates
	 *	produce a (movie_1,movie_2,rate_1,rate_2)
	 *
	 */
	public static class PairMoviesRatesWritable implements Writable {
		
		private Text movie_1;
		private Text movie_2;
		private IntWritable rate_1;
		private IntWritable rate_2;
		
		
		public PairMoviesRatesWritable() {
			super();
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
			this.rate_1 = new IntWritable(-1);
			this.rate_2 = new IntWritable(-1);
		}
		
		public PairMoviesRatesWritable(Text movie_1, Text movie_2, IntWritable rate_1, IntWritable rate_2) {
			super();
			this.movie_1 = movie_1;
			this.movie_2 = movie_2;
			this.rate_1 = rate_1;
			this.rate_2 = rate_2;
		}
		
		public Text getMovie_1() {
			return movie_1;
		}

		public void setMovie_1(Text movie_1) {
			this.movie_1 = movie_1;
		}

		public Text getMovie_2() {
			return movie_2;
		}

		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}

		public IntWritable getRate_1() {
			return rate_1;
		}

		public void setRate_1(IntWritable rate_1) {
			this.rate_1 = rate_1;
		}

		public IntWritable getRate_2() {
			return rate_2;
		}

		public void setRate_2(IntWritable rate_2) {
			this.rate_2 = rate_2;
		}


		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_1.readFields(data);
			this.movie_2.readFields(data);
			this.rate_1.readFields(data);
			this.rate_2.readFields(data);
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_1.write(data);				
			this.movie_2.write(data);				
			this.rate_1.write(data);				
			this.rate_2.write(data);				
			
		}

		@Override
	    public String toString() {
			String strings = "";
	        strings += "(" + this.movie_1.toString() + "," + this.movie_2.toString()+
	        		"," + this.rate_1.toString() + "," + this.rate_2.toString() + ")";
	      return strings;
		}
	
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	put all the MovieRateWritable into a ArrayList
	 *	produce a MovieRateWritable ArrayWritable
	 *
	 */
	public static class MovieRateArrayWritable extends ArrayWritable {
		private ArrayList<MovieRateWritable> pairs;
		
		public ArrayList<MovieRateWritable> getPairs() {
			return pairs;
		}

		public void setPairs(ArrayList<MovieRateWritable> pairs) {
			this.pairs = pairs;
		}

		public MovieRateArrayWritable() {
	        super(MovieRateWritable.class);
	        this.pairs = new ArrayList<MovieRateWritable>();
		}
		
		public MovieRateArrayWritable(ArrayList<MovieRateWritable> val) {
	        super(MovieRateWritable.class);
	        this.pairs = val;
	    }


		public void readFields(DataInput in) throws IOException {
	        this.pairs = new ArrayList<MovieRateWritable>();
	        int size = in.readInt();
		    for (int i = 0; i < size; i++) {
		    	MovieRateWritable value = new MovieRateWritable();
		        value.readFields(in);                       
		        this.pairs.add(value);                          

		    }
		  }

		  public void write(DataOutput out) throws IOException {
		    out.writeInt(this.pairs.size());                 
		    for (int i = 0; i < this.pairs.size(); i++) {
		      pairs.get(i).write(out);
		    }
		  }

		
		
	    @Override
	    public String toString() {
	    	String strings = "[";
	        for (int i = 0; i < this.pairs.size(); i++) {
	         strings = strings + this.pairs.get(i).toString() + ",";
	        }
	        strings = strings.substring(0, strings.length() - 1);
	        strings += "]";
	      return strings;
	    }
	}
	
	/**
	 * 
	 * @author z5093599
	 *	
	 *	put all the PairMoviesRatesArrayWritable into a ArrayList
	 *	produce a PairMoviesRatesArrayWritable ArrayWritable
	 *
	 */
	public static class PairMoviesRatesArrayWritable extends ArrayWritable {
		
		private ArrayList<PairMoviesRatesWritable> pairs = new ArrayList<PairMoviesRatesWritable>();

		public ArrayList<PairMoviesRatesWritable> getPairs() {
			return pairs;
		}

		public void setPairs(ArrayList<PairMoviesRatesWritable> pairs) {
			this.pairs = pairs;
		}

		public PairMoviesRatesArrayWritable() {
	        super(PairMoviesRatesWritable.class);
	        this.pairs = new ArrayList<PairMoviesRatesWritable>();
		}
		
		public PairMoviesRatesArrayWritable(ArrayList<PairMoviesRatesWritable> val) {
	        super(PairMoviesRatesWritable.class);
	        this.pairs = val;
	    }

		@Override
		public void readFields(DataInput in) throws IOException {
	        this.pairs = new ArrayList<PairMoviesRatesWritable>();
	        int size = in.readInt();
		    for (int i = 0; i < size; i++) {
		    	PairMoviesRatesWritable value = new PairMoviesRatesWritable();
		        value.readFields(in);                       
		        this.pairs.add(value);                         

		    }
		  }
		@Override
		public void write(DataOutput out) throws IOException {
		    out.writeInt(this.pairs.size());                 
		    for (int i = 0; i < this.pairs.size(); i++) {
		      pairs.get(i).write(out);
		    }
		  }

	    @Override
	    public String toString() {
	    	String strings = "[";
	        for (int i = 0; i < this.pairs.size(); i++) {
	         strings = strings + this.pairs.get(i).toString() + ",";
	        }
	        if(this.pairs.size()> 0) {
	        	strings = strings.substring(0, strings.length() - 1);
	        }
	        strings += "]";
	      return strings;
	    }
	}   
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	produce a (user,rate_1,rate_2)
	 *
	 */
	public static class UserRatesPairWritable implements Writable{
		
		private Text user;
		private IntWritable rate_1;
		private IntWritable rate_2;
		
		public UserRatesPairWritable() {
			super();
			this.user = new Text("");
			this.rate_1 = new IntWritable(-1);
			this.rate_2 = new IntWritable(-1);
		}
		
		public UserRatesPairWritable(Text user, IntWritable rate_1, IntWritable rate_2) {
			super();
			this.user = user;
			this.rate_1 = rate_1;
			this.rate_2 = rate_2;
		}

		public Text getUser() {
			return user;
		}

		public void setUser(Text user) {
			this.user = user;
		}

		public IntWritable getRate_1() {
			return rate_1;
		}

		public void setRate_1(IntWritable rate_1) {
			this.rate_1 = rate_1;
		}

		public IntWritable getRate_2() {
			return rate_2;
		}

		public void setRate_2(IntWritable rate_2) {
			this.rate_2 = rate_2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rate_1.readFields(data);
			this.rate_2.readFields(data);

		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);				
			this.rate_1.write(data);				
			this.rate_2.write(data);				
		}

		@Override
	    public String toString() {
	    	String strings = "";
	        strings += "(" + this.user.toString() + "," + this.rate_1.toString() + "," + this.rate_2.toString()+ ")";
	      return strings;
		}
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	produce a WritableComparable for the movie pairs as they're used as key
	 *	produce a (movie_1,movie_2)
	 *
	 */
	public static class MoviePairComparator implements WritableComparable<MoviePairComparator> {

		private Text movie_1;
		private Text movie_2;
		public Text getMovie_1() {
			return movie_1;
		}

		public void setMovie_1(Text movie_1) {
			this.movie_1 = movie_1;
		}

		public Text getMovie_2() {
			return movie_2;
		}

		public void setMovie_2(Text movie_2) {
			this.movie_2 = movie_2;
		}

		public MoviePairComparator() {
			super();
			this.movie_1 = new Text("");
			this.movie_2 = new Text("");
		}
		
		public MoviePairComparator(Text movie_1, Text movie_2) {
			super();
			this.movie_1 = movie_1;
			this.movie_2 = movie_2;
		}

		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie_1.readFields(data);
			this.movie_2.readFields(data);			
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.movie_1.write(data);				
			this.movie_2.write(data);			
		}

		@Override
		public int compareTo(MoviePairComparator mc) {
			int res;
			
			if(this.movie_1.compareTo(mc.getMovie_1()) > 0) {
				res = 1;
			} else if(this.movie_1.compareTo(mc.getMovie_1()) == 0) {
				if(this.movie_2.compareTo(mc.getMovie_2()) > 0) {
					res = 1;
				} else if(this.movie_2.compareTo(mc.getMovie_2()) == 0) {
					res = 0;
				} else {
					res = -1;
				}
			} else {
				res = -1;
			}
			return res;
		}
		
		
		
		@Override
	    public String toString() {
	    	String strings = "";
	        strings += "(" + this.movie_1.toString() + "," + this.movie_2.toString() + ")";
	      return strings;
		}
		

	
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	put all the UserRatesPairWritable into a ArrayList
	 *	produce a UserRatesPairWritable ArrayWritable
	 *
	 */
	public static class UserRatesPairArrayWritable extends ArrayWritable {
		
		private ArrayList<UserRatesPairWritable> pairs = new ArrayList<UserRatesPairWritable>();

		public UserRatesPairArrayWritable(ArrayList<UserRatesPairWritable> pairs) {
			super(UserRatesPairWritable.class);
			this.pairs = pairs;
		}

		public UserRatesPairArrayWritable() {
			super(UserRatesPairWritable.class);
			this.pairs = new ArrayList<UserRatesPairWritable>();
		}
		public ArrayList<UserRatesPairWritable> getPairs() {
			return pairs;
		}

		public void setPairs(ArrayList<UserRatesPairWritable> pairs) {
			this.pairs = pairs;
		}

		@Override
	    public String toString() {
	    	String strings = "[";
	        for (int i = 0; i < this.pairs.size(); i++) {
	        	strings = strings + this.pairs.get(i).toString() + ",";
	        }
	        if(this.pairs.size()> 0) {
	        	strings = strings.substring(0, strings.length() - 1);
	        }
	        strings += "]";
	      return strings;
	    }
		
	}
	
	public static class UserMovieMapper extends Mapper<LongWritable, Text, Text, MovieRateWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieRateWritable>.Context context)
				throws IOException, InterruptedException {
			
			String [] parts = value.toString().split("::");
			int rate = Integer.parseInt(parts[2]);
			
			MovieRateWritable val = new MovieRateWritable();
			val.setMovieID(new Text(parts[1]));
			val.setRate(new IntWritable(rate));

			context.write(new Text(parts[0]),val);
		}

		
	}
	
	public static class UserMovieReducer extends Reducer<Text, MovieRateWritable, Text, PairMoviesRatesArrayWritable> {

		
		private int compareMovie(Text movie_1, Text movie_2) {
			return movie_1.compareTo(movie_2);
		}
		
		@Override
		protected void reduce(Text key, Iterable<MovieRateWritable> values,
				Reducer<Text, MovieRateWritable, Text, PairMoviesRatesArrayWritable>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<MovieRateWritable> movieRates = new ArrayList<MovieRateWritable>();
			ArrayList<PairMoviesRatesWritable> pairMoviesRates = new ArrayList<PairMoviesRatesWritable>();

			for(MovieRateWritable a: values) {
				movieRates.add(new MovieRateWritable(new Text(a.getMovieID()), new IntWritable(a.getRate().get())));
			}
			if(movieRates.size() == 2) {
				Text movie_1 = new Text(movieRates.get(0).getMovieID());
				Text movie_2 = new Text(movieRates.get(1).getMovieID());
				IntWritable rate_1 = new IntWritable(movieRates.get(0).getRate().get());
				IntWritable rate_2 = new IntWritable(movieRates.get(1).getRate().get());
				
				if(compareMovie(movie_1, movie_2) > 0) {
					pairMoviesRates.add(new PairMoviesRatesWritable(movie_2, movie_1, rate_2, rate_1));
				} else {
					pairMoviesRates.add(new PairMoviesRatesWritable(movie_1, movie_2, rate_1, rate_2));
				}
			} 
			
			if(movieRates.size() > 2) {
				for (int i = 0; i < movieRates.size()-1; i++) {
					for (int j = i+1; j < movieRates.size(); j++) {
						Text movie_1 = new Text(movieRates.get(i).getMovieID());
						Text movie_2 = new Text(movieRates.get(j).getMovieID());
						IntWritable rate_1 = new IntWritable(movieRates.get(i).getRate().get());
						IntWritable rate_2 = new IntWritable(movieRates.get(j).getRate().get());
						
						if(compareMovie(movie_1, movie_2) > 0) {
							pairMoviesRates.add(new PairMoviesRatesWritable(movie_2, movie_1, rate_2, rate_1));
						} else {
							pairMoviesRates.add(new PairMoviesRatesWritable(movie_1, movie_2, rate_1, rate_2));
						}
					}
				}
			}
			PairMoviesRatesArrayWritable value = new PairMoviesRatesArrayWritable(pairMoviesRates);

			context.write(key, value);
		}

	}	
	
	

	public static class MovieKeyMapper extends Mapper<Text, PairMoviesRatesArrayWritable, MoviePairComparator, UserRatesPairWritable> {

		@Override
		protected void map(Text key, PairMoviesRatesArrayWritable value,
				Mapper<Text, PairMoviesRatesArrayWritable, MoviePairComparator, UserRatesPairWritable>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<PairMoviesRatesWritable> pair = value.getPairs();
			
			for (int i = 0; i < pair.size(); i++) {
				Text movie_1 = new Text(pair.get(i).getMovie_1());
				Text movie_2 = new Text(pair.get(i).getMovie_2());
				IntWritable rate_1 = new IntWritable(pair.get(i).getRate_1().get());
				IntWritable rate_2 = new IntWritable(pair.get(i).getRate_2().get());
				UserRatesPairWritable ur = new UserRatesPairWritable(key, rate_1, rate_2);
				MoviePairComparator mc = new MoviePairComparator(movie_1, movie_2);
				
				context.write(mc,ur);

			}
		}
	}
	
	public static class MovieKeyReducer extends Reducer<MoviePairComparator, UserRatesPairWritable, MoviePairComparator, UserRatesPairArrayWritable> {

		@Override
		protected void reduce(MoviePairComparator key, Iterable<UserRatesPairWritable> values,
				Reducer<MoviePairComparator, UserRatesPairWritable, MoviePairComparator, UserRatesPairArrayWritable>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<UserRatesPairWritable> userRates = new ArrayList<UserRatesPairWritable>();
			for(UserRatesPairWritable a: values) {
				userRates.add(new UserRatesPairWritable(
						new Text(a.getUser()), 
						new IntWritable(a.getRate_1().get()), 
						new IntWritable(a.getRate_2().get())));
			}
			
			UserRatesPairArrayWritable value = new UserRatesPairArrayWritable(userRates);
			context.write(key, value);

		}
		
		
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
	    Path out = new Path(args[1]);

	    Job job1 = Job.getInstance(conf, "word count");
	    job1.setMapperClass(UserMovieMapper.class);
	    job1.setReducerClass(UserMovieReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(PairMoviesRatesArrayWritable.class);
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(MovieRateWritable.class);
	    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
	    if (!job1.waitForCompletion(true)) {
	      System.exit(1);
	    }
	    Job job2 = Job.getInstance(conf, "sort by frequency");
	    job2.setMapperClass(MovieKeyMapper.class);
	    job2.setReducerClass(MovieKeyReducer.class);
	    job2.setNumReduceTasks(1);
	    
	    job2.setOutputKeyClass(MoviePairComparator.class);
	    job2.setOutputValueClass(UserRatesPairArrayWritable.class);
	    job2.setMapOutputKeyClass(MoviePairComparator.class);
	    job2.setMapOutputValueClass(UserRatesPairWritable.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    job2.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job2, new Path(out, "out1"));
	    FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
	    if (!job2.waitForCompletion(true)) {
	      System.exit(1);
	    }

	}

}
