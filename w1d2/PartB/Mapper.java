package bigData.w1d2.PartB;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {

	String source_dir; 
	Mapper(String source){
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

	public List<Pair<String, Integer>> createListPairs(List<String> list) {
		List<Pair<String, Integer>> pairList = new ArrayList<>();
		for (String str : list) {
			pairList.add(new Pair<String, Integer>(str, 1));
		}
		sortCollection(pairList);
		return pairList;
	}

	public void sortCollection(List<Pair<String, Integer>> list) {
		Collections.sort(list, (p1, p2) -> p1.getK().compareTo(p2.getK()));
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