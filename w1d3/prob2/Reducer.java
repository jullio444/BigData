package bigData.w1d3.prob2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Reducer {

	public List<GroupByPair<Character, List<List<Integer>>>> generateReducerInput(
			List<GroupByPair<Character, List<Integer>>> mappedData) {
		LinkedHashMap<Character, GroupByPair<Character, List<List<Integer>>>> hashMap = new LinkedHashMap<>();

		for (GroupByPair<Character, List<Integer>> md : mappedData) {
			List<List<Integer>> values = new ArrayList<>();
			mappedData.stream().filter(k -> k.getK().equals(md.getK())).forEach(k -> addValues(values, k.getV()));
			GroupByPair<Character, List<List<Integer>>> p = new GroupByPair<Character, List<List<Integer>>>(md.getK(),
					values);
			hashMap.put(md.getK(), p);

		}
		List<GroupByPair<Character, List<List<Integer>>>> reduceInput = new ArrayList<>();
		for (Map.Entry<Character, GroupByPair<Character, List<List<Integer>>>> en : hashMap.entrySet()) {
			reduceInput.add(en.getValue());
		}
		sortCollection(reduceInput);
		return reduceInput;
	}

	public List<GroupByPair<Character, Double>> generateReducerOutput(
			List<GroupByPair<Character, List<List<Integer>>>> listInput) {
		List<GroupByPair<Character, Double>> reduceOutput = new ArrayList<>();

		for (GroupByPair<Character, List<List<Integer>>> p : listInput) {
			double totalCharLen = 0.0;
			double totalWordCount = 0.0;
			for(List<Integer> i: p.getV()) {
				totalCharLen += i.get(0);
				totalWordCount += i.get(1);
			}
			double average = totalCharLen/totalWordCount;
			reduceOutput.add(new GroupByPair<Character, Double>(p.getK(),average));
		}
		return reduceOutput;
	}

	public void sortCollection(List<GroupByPair<Character, List<List<Integer>>>> list) {
		Collections.sort(list, (p1, p2) -> p1.getK().compareTo(p2.getK()));
	}

	public void addValues(List<List<Integer>> values, List<Integer> list) {
		values.add(list);
	}

}
