import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * This class will read one json document at a time as if they are each on a line of their own.
 * @author scotthernandez
 *
 */
class GithubJSONReader extends BufferedReader {

	private StringBuilder sb = new StringBuilder();
	public GithubJSONReader(Reader r) {
		super(r);
	}

	@Override
	public String readLine() throws IOException {
		int prev = 0;
		int curr = -1;
		if (sb == null) return null;
		while(true) {
			curr = super.read();
			if(curr == -1) break;
			
			if (prev == '}' && curr == '{'){
				prev = curr;
				String ret = sb.toString();
				sb = new StringBuilder().append("{");
				return ret;
			}
			
			sb.append((char)curr);
			prev = curr;
		}
		String ret = sb.toString();
		sb = null;
		return ret;
	}
	
}