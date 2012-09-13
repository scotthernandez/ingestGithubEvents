import java.util.List;

/**
 * Definition for one transformation and aggregation from one (inserted) document
 * @author scotthernandez
 *
 */
class Aggregation {
	String namePostfix;
	List<String> groupFields;
	List<Period> periods;
	List<String> counterFields;
	Aggregation(String name, List<Period> p, List<String> grp, List<String> cnt){
		namePostfix = name; periods = p; groupFields = grp; counterFields = cnt;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Aggregation[*").append(namePostfix).append("]");
		sb.append(" groupby: ").append(groupFields);
		
		if(counterFields != null)
			sb.append(" sum fields").append(counterFields);
		else
			sb.append(" counting");

		sb.append(" for: ").append(periods);
		
		return sb.toString();
	}
	
	
}