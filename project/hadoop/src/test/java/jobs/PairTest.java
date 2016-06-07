package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PairTest {
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private static final String STAR_SYMBOL = "*";

	public static void main(String[] args) throws IOException,
			InterruptedException {
		String line;
		String[] arr;
		int len;
		int i;
		int j;

		List<String> lst = new ArrayList<String>();
		lst.add("cat mat rat cat");
		lst.add("cat bat cat pat");
		lst.add("cat rat bat rat");

		for (String s : lst) {
			line = s.trim();
			arr = WORD_BOUNDARY.split(line);

			len = arr.length;
			i = 0;
			j = 0;
			for (; i < len - 1; i++) {
				if (arr[i] != null && !arr[i].isEmpty()) {
					for (j = i + 1; j < len; j++) {
						if (arr[j] != null && !arr[j].isEmpty()) {
							if (!arr[i].equals(arr[j])) {
								System.out.println("(" + arr[i] + ", " + arr[j]
										+ "), " + 1);
								System.out.println("(" + arr[i] + ", "
										+ STAR_SYMBOL + "), " + 1);
							} else {
								break;
							}
						}
					}
				}
			}
		}

	}
}