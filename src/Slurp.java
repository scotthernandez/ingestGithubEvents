import java.io.BufferedInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

public class Slurp {

	/**
	 * Slurps up some data and ingests it into raw docs and counter (aggregated) docs.
	 * The aggregations are always live, and up to date as data comes in.
	 * @param args
	 * @throws MongoException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MongoException, IOException {
		if (args == null || args.length == 0) {
			System.out.println("Must supply a github json file as the argument to this program");
			return;
		}
		
		String jsonFP = args[0];
		Mongo m = new Mongo();
		DB db = m.getDB("github");
		DBCollection coll = db.getCollection("events");
		//Create the writer for our aggregations
		AggregationWriter aggWriter = new AggregationWriter(db, null);
		System.out.println("Aggregations to apply");
		System.out.println(aggWriter.aggs);
		System.out.println();

		if (jsonFP.length() > 4 && jsonFP.startsWith("http")) {
			for(String s : args) {
				final URL url = new URL(s);
				final InputStream in = new BufferedInputStream(url.openStream());
				processReader(coll, aggWriter, new InputStreamReader(new GZIPInputStream(in)));				
			}
		} else {
			System.out.println("processing " + jsonFP);
			processReader(coll, aggWriter, new FileReader(jsonFP));
		}
	}
	
	public static void processReader(DBCollection coll, AggregationWriter aggWriter, Reader r) throws IOException {
		GithubJSONReader reader = new GithubJSONReader(r);
		try {
			String strLine;
			//loop through each json doc -- record the raw and aggregate data
			while ((strLine = reader.readLine()) != null) {
				BasicDBObject event = (BasicDBObject) JSON.parse(strLine);
				//System.out.println("found line, " + strLine.length() + " chars");
				//save raw data
				coll.insert(event, WriteConcern.NONE);
				//save aggregations for raw event
				aggWriter.write(event);
			}	
		} finally {
			reader.close();
		}
	}
}
