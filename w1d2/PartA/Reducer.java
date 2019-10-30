package bigData.w1d2.PartA;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Reducer {
	private static List<Pair<String, Integer>> copy;
	
	Reducer() {
		copy = new Mapper().createListPairs(new Mapper().readFromFile());
	}

	public  List<GroupByPair<String, List<Integer>>> generateReducerInput(List<Pair<String, Integer>> mappedData) {
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
		return reduceInput;
	}

	public  List<GroupByPair<String, Integer>> generateReducerOutput(List<GroupByPair<String, List<Integer>>> listInput){
		List<GroupByPair<String, Integer>> listOutput = new ArrayList<>();
		for(GroupByPair<String, List<Integer>> p: listInput) {
			Integer value = p.getV().stream().reduce(0,(a,b)->a+b);
			listOutput.add(new GroupByPair<String, Integer>(p.getK(),value));	
		}
		return listOutput;
	}
	public  void addValues(List<Integer> values, Integer v) {
		values.add(v);
	}

}
