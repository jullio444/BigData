package bigData.w1d3.prob1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Reducer {

	public List<GroupByPair<String, List<Integer>>> generateReducerInput(List<Pair<String, Integer>> mappedData) {
		LinkedHashMap<String, GroupByPair<String, List<Integer>>> hashMap = new LinkedHashMap<>();

		for (Pair<String, Integer> md : mappedData) {
			List<Integer> values = new ArrayList<>();
			mappedData.stream().filter(k -> k.getK().equals(md.getK())).forEach(k -> addValues(values, k.getV()));
			GroupByPair<String, List<Integer>> p = new GroupByPair<String, List<Integer>>(md.getK(), values);
			hashMap.put(md.getK(), p);

		}
		List<GroupByPair<String, List<Integer>>> reduceInput = new ArrayList<>();
		for (Map.Entry<String, GroupByPair<String, List<Integer>>> en : hashMap.entrySet()) {
			reduceInput.add(en.getValue());
		}
		sortCollection(reduceInput);
		return reduceInput;
	}

	public List<GroupByPair<String, Integer>> generateReducerOutput(List<GroupByPair<String, List<Integer>>> listInput) {
		List<GroupByPair<String, Integer>> listOutput = new ArrayList<>();
		for (GroupByPair<String, List<Integer>> p : listInput) {
			Integer value = p.getV().stream().reduce(0, (a, b) -> a + b);
			listOutput.add(new GroupByPair<String, Integer>(p.getK(), value));
		}
		return listOutput;
	}
	
	public void sortCollection(List<GroupByPair<String, List<Integer>>> list) {
		Collections.sort(list, (p1, p2) -> p1.getK().compareTo(p2.getK()));
	}
	
	public void addValues(List<Integer> values, Integer v) {
		values.add(v);
	}
	
}
