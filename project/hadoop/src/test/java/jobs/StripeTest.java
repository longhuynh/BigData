package jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class StripeTest {
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	public static void main(String[] args) throws IOException,
			InterruptedException {
		String line;
		String[] arr;
		int len;
		int w;
		int u;
		Map<String, Integer> stripeH;

		List<String> lst = new ArrayList<String>();
		lst.add("18 34 56 29 12 34 56 92 29 34 12");
		lst.add("92 29 18 12 34 79 29 56 12 34 18");

		for (String s : lst) {
			line = s.trim();
			arr = WORD_BOUNDARY.split(line);

			len = arr.length;
			w = 0;
			u = 0;
			for (; w < len - 1; w++) {
				if (arr[w] != null && !arr[w].isEmpty()) {
					stripeH = new HashMap<String, Integer>();

					for (u = w + 1; u < len; u++) {
						if (arr[u] != null && !arr[u].isEmpty()) {
							if (!arr[w].equals(arr[u])) {
								// if (stripeH.containsKey(new Text(arr[u]))) {
								// stripeH.put(new Text(arr[u]), new
								// IntWritable(
								// ((IntWritable) stripeH.get(arr[u])).get() +
								// 1));
								// }
								// else {
								// stripeH.put(new Text(arr[u]), ONE);
								// }
								String t = arr[u];
								int tempInt;

								if (stripeH.get(t) == null) {
									tempInt = 0;
									stripeH.put(t, tempInt);
								} else {
									tempInt = stripeH.get(t);
								}

								stripeH.put(t, tempInt + 1);
							} else {
								break;
							}
						}
					}

					System.out.print("(" + arr[w] + ", ");
					Iterator<String> it = stripeH.keySet().iterator();
					while (it.hasNext()) {
						String key = (String) it.next();
						int value = stripeH.get(key);
						System.out.print("(" + key + ", "
								+ String.valueOf(value) + "), ");
					}
					System.out.println("), ");
				}
			}
		}

	}

}
