package bigData.w1d3.prob2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {

	String source_dir;

	Mapper(String source) {
		this.source_dir = System.getProperty("user.dir") + source;

	}

	public List<String> readFromFile() {
		List<String> wordsList = new ArrayList<>();
		List<String> lines = readLineTextInFile(source_dir);
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

	public LinkedHashMap<Character, Pair<Integer, Integer>> createMappedListPairs(List<String> list) {
		Set<Character> set = new LinkedHashSet<Character>();
		LinkedHashMap<Character, Pair<Integer, Integer>> hashMap = new LinkedHashMap<>();
		for(String s: list) {
			set.add(s.charAt(0));
		}
		for(Character c: set) {
			Integer length = list.stream().filter(w->w.startsWith(""+c))
						 .map(i->i.length()).reduce(0,(a,b)->a+b);
			Integer nWords = (int) list.stream().filter(w->w.startsWith(""+c)).count();
			hashMap.put(c, new Pair<Integer, Integer>(length, nWords));
		}
		return hashMap;
	}


	public List<String> readLineTextInFile(String fileName) {

		List<String> lines = Collections.emptyList();
		try {
			lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
}