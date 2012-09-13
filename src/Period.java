import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Representation for an aggregation period, with helpers.
 * @author scotthernandez
 *
 */
enum Period {
	MINUTELY, FIVEMINUTELY, HOURLY, DAILY, MONTHLY;
	@Override
	public String toString() {
		return super.toString().toLowerCase();
	}

	public static Period parse(String p){
		return valueOf(p.toUpperCase());
	}
	
	/**
	 * Returns a Calendar for a given the milliseconds since epoch (GMT)
	 * @param msCount milliseconds since epoch (GMT)
	 * @return The calendar representing this period for the given time.
	 */
	Calendar getPeriodDate(long msCount) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		cal.setTimeInMillis(msCount);
		cal.set(Calendar.MILLISECOND, 0);
		Calendar currentDt = new GregorianCalendar(TimeZone.getDefault(), Locale.ENGLISH);
		// Get the Offset from GMT taking DST into account
		int gmtOffset = TimeZone.getDefault().getOffset(
		    currentDt.get(Calendar.ERA), 
		    currentDt.get(Calendar.YEAR), 
		    currentDt.get(Calendar.MONTH), 
		    currentDt.get(Calendar.DAY_OF_MONTH), 
		    currentDt.get(Calendar.DAY_OF_WEEK), 
		    currentDt.get(Calendar.MILLISECOND));
		
		/*TODO: find a helper to do this for us. --probably not good like this*/
		// convert to hours
		gmtOffset = gmtOffset / (60*60*1000);

		switch (this) {
			case MINUTELY:
				cal.set(Calendar.SECOND,0);
				break;
			case FIVEMINUTELY:
				cal.set(Calendar.SECOND,0);
				cal.set(Calendar.MINUTE,cal.get(Calendar.MINUTE) - (cal.get(Calendar.MINUTE) %5));
				break;
			case HOURLY:
				cal.set(Calendar.SECOND,0);
				cal.set(Calendar.MINUTE,0);
				break;
			case DAILY:
				cal.set(Calendar.SECOND,0);
				cal.set(Calendar.MINUTE,0);
				cal.set(Calendar.HOUR_OF_DAY,0);
				break;
			case MONTHLY:
				cal.set(Calendar.SECOND,0);
				cal.set(Calendar.MINUTE,0);
				cal.set(Calendar.HOUR_OF_DAY,0);
				cal.add(Calendar.DAY_OF_MONTH, -1);
				break;

			default:
				break;
		}
		
		return cal;
	}
}