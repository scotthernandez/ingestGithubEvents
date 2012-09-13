import java.io.FileReader;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

public class Slurp {

	/**
	 * Slurps up a single json file
	 * @param args
	 * @throws MongoException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MongoException, IOException {
		if (args == null || args.length != 1) {
			System.out.println("Must supply a github json file as the only argument to this program");
			return;
		}
		
		String jsonFP = args[0];
		Mongo m = new Mongo();
		DB db = m.getDB("github");
		DBCollection coll = db.getCollection("events");
		GithubJSONReader reader = new GithubJSONReader(new FileReader(jsonFP));
		String strLine;
		

		//Create the writer for our aggregations
		AggregationWriter aggWriter = new AggregationWriter(db, null);
		
		System.out.println("Aggregations to apply");
		System.out.println(aggWriter.aggs);
		System.out.println();

		//loop through each json doc -- record the raw and aggregate data
		while ((strLine = reader.readLine()) != null) {
			BasicDBObject event = (BasicDBObject) JSON.parse(strLine);
			System.out.println("found line, " + strLine.length() + " chars");
			//save raw data
			coll.insert(event, WriteConcern.NONE);
			//save aggregations for raw event
			aggWriter.write(event);
		}
		
		reader.close();
	}


}
