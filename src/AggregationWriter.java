import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * This writer is able to write counters based on aggregations defined for documents passed to it.
 * @author scotthernandez
 *
 */
class AggregationWriter{
	public final static String countField = "c";
	public final static String monthsField = "M";
	public final static String daysField = "d";
	public final static String hoursField = "h";
	public final static String minutesField = "m";
	public final static String totalField = "t";

	public final static List<Period> AGG_PERIODS =  Arrays.asList(Period.HOURLY, Period.DAILY, Period.MONTHLY);
	private final static List<Aggregation> DEFAULT_AGGS = Arrays.asList(
			new Aggregation("actors", AGG_PERIODS, Arrays.asList("actor"), null),
			new Aggregation("projects", AGG_PERIODS, Arrays.asList("repository.name"), null),
			new Aggregation("types", AGG_PERIODS, Arrays.asList("type"), null),
			new Aggregation("langs", AGG_PERIODS, Arrays.asList("repository.language"), null)
	);
	
	DB db = null;
	List<Aggregation> aggs = DEFAULT_AGGS;
	
	public AggregationWriter(DB db, List<Aggregation> aggs) {
		this.db = db;
		this.aggs = aggs == null ? DEFAULT_AGGS : aggs;
	}
	
	public void write(BasicDBObject dbObj){
		writeAggregates(dbObj, DEFAULT_AGGS);
	}
	
	@SuppressWarnings("deprecation")
	private void writeAggregates(BasicDBObject srcDoc, List<Aggregation> aggregations) {
		//do each aggregation update
		for(Aggregation agg : aggregations){
			boolean skip = false;

			DBObject updateValue;
			
			if(agg.counterFields == null || agg.counterFields.isEmpty()) {
				updateValue = BasicDBObjectBuilder.start(countField, 1L).get();
			} else {
				//build the update part
				BasicDBObjectBuilder updateBldr = BasicDBObjectBuilder.start();

				//collect counter field values
				for(String aggFieldName : agg.counterFields){
					skip = !copyFieldToAggDoc(srcDoc, aggFieldName, updateBldr);
					if (skip) break;
				}
				updateBldr.add(countField, 1);
				updateValue = updateBldr.get();
			}

			//skip since a field wasn't found for the counters
			if (skip) continue;
			
			//create the group by part which goes in the _id field
			BasicDBObjectBuilder queryBldr = BasicDBObjectBuilder.start();
			for(String fld : agg.groupFields) {
				skip = !copyFieldToAggDoc(srcDoc, fld, queryBldr);
				if (skip) break;
			}
			
			//skip since a field wasn't found for the group
			if (skip) continue;
			
			BasicDBObject dbObjBaseIdValues = (BasicDBObject) queryBldr.get();
			
			//long periodMS = System.currentTimeMillis();
			//TODO parse created_at field.
			long periodMS = System.currentTimeMillis();
			periodMS = Date.parse((String)srcDoc.get("created_at"));
			Calendar periodDate = Period.MINUTELY.getPeriodDate(periodMS);

			//add periods for this aggregation and update on the server
			for(Period p : agg.periods) {
				HashMap<String, Long> updateMatrix;
				DBCollection coll = getCollectionForAggregation(agg,p);

				queryBldr = BasicDBObjectBuilder.start().push("_id");
				
				Calendar period; 
				int pos, sub = 0;
				String prefix;
				switch (p) {
				case MONTHLY:
					period = Period.MONTHLY.getPeriodDate(periodMS);
					updateMatrix = new HashMap<String, Long>(10);
					pos = periodDate.get(Calendar.MONDAY) + 1;
					prefix = monthsField;
					break;
				case DAILY:
					period = Period.MONTHLY.getPeriodDate(periodMS);
					updateMatrix = new HashMap<String, Long>(10);
					pos = periodDate.get(Calendar.DAY_OF_MONTH) - 1;
					prefix = daysField;
					break;
				case HOURLY:
					period = Period.DAILY.getPeriodDate(periodMS);
					updateMatrix = new HashMap<String, Long>(10);
					pos = periodDate.get(Calendar.HOUR_OF_DAY) + 1;
					prefix = hoursField;
					break;
				case FIVEMINUTELY:
					//TODO: add support for 5-minute stats
				case MINUTELY:
				default:
					period = Period.HOURLY.getPeriodDate(periodMS);
					updateMatrix = new HashMap<String, Long>(10);
					pos = periodDate.get(Calendar.MINUTE) + 1;
					prefix = minutesField;
				}
				
				for(Map.Entry<String, Object> e : ((BasicDBObject)updateValue).entrySet()) {
					if (sub > 0)
						updateMatrix.put(prefix + "." + pos + "." + sub + "." + e.getKey(), ((Number)e.getValue()).longValue());
					else
						updateMatrix.put(prefix + "." + pos + "." + e.getKey(), ((Number)e.getValue()).longValue());
					
					updateMatrix.put(totalField + "." + e.getKey(), ((Number)e.getValue()).longValue());
				}
				
				//add period to end for range query support
				queryBldr.add("p", period.getTime());

				for(Map.Entry<String, Object> e : dbObjBaseIdValues.entrySet())
					queryBldr.add(e.getKey(), e.getValue());
				
				DBObject query = queryBldr.get();
				coll.update(query,  new BasicDBObject("$inc", updateMatrix), true, false, WriteConcern.NORMAL);
			}
					
		}
	}

	private DBCollection getCollectionForAggregation(Aggregation agg, Period p) {
		String collName = "stats_" + p.toString();
		
		if(agg.namePostfix != null)
			collName += "." + agg.namePostfix;
		
		return db.getCollection(collName);
	}

	/** adds the field from the source doc, and returns success (doc has field) */
	private boolean copyFieldToAggDoc(BasicDBObject srcDoc, String fld, BasicDBObjectBuilder aggDocBldr) {
		String fieldname = fld;
		//indicator if it should be treated as an array
		int indexOffset = 0;
		if (fld.endsWith("]")) {
			int pos = fld.lastIndexOf("[");
			fieldname = fld.substring(0, pos);
			indexOffset = new Integer(fld.substring(pos+1, fld.length()-1));
		}
		Object o = srcDoc.get(fieldname);
		try{
			if(o == null && fieldname.contains(".")) {
				BasicDBObject current = srcDoc; 
				String[] parts = fieldname.split("\\.");
				for(int i=0; i<parts.length-1;i++){
					current = (BasicDBObject) current.get(parts[i]);
					if(current == null)
						break;
				}
				if(current!=null)
					o = current.get(parts[parts.length-1]);
			}
		} catch (Exception e){
			e.printStackTrace(System.out);
			return false;
		}
		if(o != null) {
			//get offset for array/list and replace o
			if (indexOffset != 0 && o.getClass().isArray()) {
				int len = Array.getLength(o);
				if (indexOffset > len)
					o = Array.get(o, len-1); //get last one 
				else
					o = Array.get(o, (indexOffset < 0) ? (len + indexOffset) : indexOffset);  
			}
			aggDocBldr.add(fieldname.replace(".", "_"), o);
		} else //missing value, skip
			return false;
		
		return true;
	}
	
}