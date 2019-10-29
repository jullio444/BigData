package bigData.w1d1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreatePairFromText {

	public static final String SOURCE_DIR = System.getProperty("user.dir") + "//src//bigData//w1d1//textfile/testDataForW1D1.txt";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		createListPairs(readFromFile()).forEach(System.out::print);
	}

	public static Set<String> readFromFile() {
		Set<String> wordsList = new HashSet<>();
		List<String> lines = readLineTextInFile(SOURCE_DIR);
		Iterator<String> itr = lines.iterator();
		while (itr.hasNext()) {
			String line = itr.next();
			line = line.replaceAll("\\w*\\d\\w*", "");
			line = line.replaceAll("\\w{1,}\\_\\w{1,}", "");
			line = line.replaceAll("\\w{1,}\\.\\w{1,}", "");
			Pattern p = Pattern.compile("[A-Za-z]+");
			Matcher m = p.matcher(line);
			while (m.find()) {
				wordsList.addAll(Arrays.asList(m.group().toLowerCase()));
			}
		}
		return wordsList;
	}

	public static List<Pair<String, Integer>> createListPairs(Set<String> list) {
		List<Pair<String, Integer>> pairList = new ArrayList<>();
		for (String str : list) {
			pairList.add(new Pair<String, Integer>(str, 1));
		}
		sortCollection(pairList);
		return pairList;
	}

	public static void sortCollection(List<Pair<String, Integer>> list) {
		Collections.sort(list, (p1, p2) -> p1.getK().compareTo(p2.getK()));
	}

	public static List<String> readLineTextInFile(String fileName) {

		List<String> lines = Collections.emptyList();
		try {
			lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
}
/**Sample output
 * 
(a,1)
(and,1)
(are,1)
(as,1)
(cat,1)
(class,1)
(collections,1)
(comparator,1)
(each,1)
(extract,1)
(file,1)
(for,1)
(form,1)
(given,1)
(however,1)
(inserted,1)
(integer,1)
(into,1)
(involve,1)
(is,1)
(key,1)
(list,1)
(may,1)
(not,1)
(note,1)
(one,1)
(output,1)
(pair,1)
(program,1)
(same,1)
(should,1)
(sort,1)
(such,1)
(text,1)
(that,1)
(the,1)
(this,1)
(tokens,1)
(treat,1)
(two,1)
(using,1)
(value,1)
(where,1)
(will,1)
(word,1)
(words,1)
(writing,1)
(your,1)
 *  
 */
