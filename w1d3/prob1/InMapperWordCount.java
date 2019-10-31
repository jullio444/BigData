package bigData.w1d3.prob1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMapperWordCount {
	private List<Mapper> mappers;
	private List<Reducer> reducers;
	private int m;
	private int r;

	InMapperWordCount() {
		mappers = new ArrayList<>(Arrays.asList(new Mapper("//src//bigData//w1d2//PartB//textfile/testData1.txt"),
				new Mapper("//src//bigData//w1d2//PartB//textfile/testData2.txt"),
				new Mapper("//src//bigData//w1d2//PartB//textfile/testData3.txt")));
		reducers = new ArrayList<>(Arrays.asList(new Reducer(), new Reducer(), new Reducer(), new Reducer()));
		m = mappers.size();
		r = reducers.size();
	}

	public List<LinkedHashMap<String, Integer>> jobTracker() {
		List<LinkedHashMap<String, Integer>> mapperOutputs = new ArrayList<>();
		LinkedHashMap<String, Integer> mapperOutput1 = mappers.get(0)
				.createMappedListPairs(mappers.get(0).readFromFile());
		mapperOutputs.add(mapperOutput1);
		System.out.println("Mapper 0 Output");
		for (Map.Entry<String, Integer> en : mapperOutput1.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();

		LinkedHashMap<String, Integer> mapperOutput2 = mappers.get(1)
				.createMappedListPairs(mappers.get(1).readFromFile());
		mapperOutputs.add(mapperOutput2);
		System.out.println("Mapper 1 Output");
		for (Map.Entry<String, Integer> en : mapperOutput2.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();

		LinkedHashMap<String, Integer> mapperOutput3 = mappers.get(2)
				.createMappedListPairs(mappers.get(2).readFromFile());
		mapperOutputs.add(mapperOutput3);
		System.out.println("Mapper 2 Output");
		for (Map.Entry<String, Integer> en : mapperOutput3.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();
		return mapperOutputs;
	}

	public List<List<Pair<String, Integer>>> pairsSentToReducers() {
		List<List<Pair<String, Integer>>> reducerInputs = new ArrayList<>();
		List<LinkedHashMap<String, Integer>> Mappers = jobTracker();
		List<Pair<String, Integer>> reducerInputs0 = new ArrayList<>();
		List<Pair<String, Integer>> reducerInputs1 = new ArrayList<>();
		List<Pair<String, Integer>> reducerInputs2 = new ArrayList<>();
		List<Pair<String, Integer>> reducerInputs3 = new ArrayList<>();

		for (LinkedHashMap<String, Integer> mapperOutput : Mappers) {
			List<Pair<String, Integer>> reducerInput0 = new ArrayList<>();
			List<Pair<String, Integer>> reducerInput1 = new ArrayList<>();
			List<Pair<String, Integer>> reducerInput2 = new ArrayList<>();
			List<Pair<String, Integer>> reducerInput3 = new ArrayList<>();
			for (Map.Entry<String, Integer> en : mapperOutput.entrySet()) {
				if (getPartition(en.getKey()) == 0) {
					reducerInput0.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
					reducerInputs0.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
				} else if (getPartition(en.getKey()) == 1) {
					reducerInput1.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
					reducerInputs1.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
				} else if (getPartition(en.getKey()) == 2) {
					reducerInput2.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
					reducerInputs2.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
				} else if (getPartition(en.getKey()) == 3) {
					reducerInput3.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
					reducerInputs3.add(new Pair<String, Integer>(en.getKey(), en.getValue()));
				}
			}
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 0");
			reducerInput0.forEach(System.out::print);
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 1");
			reducerInput1.forEach(System.out::print);
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 2");
			reducerInput2.forEach(System.out::print);
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 3");
			reducerInput3.forEach(System.out::print);
		}
		reducerInputs.add(reducerInputs0);
		reducerInputs.add(reducerInputs1);
		reducerInputs.add(reducerInputs2);
		reducerInputs.add(reducerInputs3);
		return reducerInputs;
	}

	public int getPartition(String key) {
		return Math.abs(key.hashCode() % r);
	}

	public void shuffleReduce() {
		List<List<Pair<String, Integer>>> listOfPairsSentToReducers = pairsSentToReducers();
		List<List<GroupByPair<String, List<Integer>>>> reducerInputs = new ArrayList<>();

		for (List<Pair<String, Integer>> lp : listOfPairsSentToReducers) {
			List<GroupByPair<String, List<Integer>>> reducerInput = new ArrayList<>(
					reducers.get(listOfPairsSentToReducers.indexOf(lp)).generateReducerInput(lp));
			reducerInputs.add(reducerInput);
		}
		for (List<GroupByPair<String, List<Integer>>> r : reducerInputs) {
			System.out.println("Reducer " + reducerInputs.indexOf(r) + " input");
			r.forEach(System.out::print);
		}

		List<List<GroupByPair<String, Integer>>> reducerOutputs = new ArrayList<>();
		for (List<GroupByPair<String, List<Integer>>> gp : reducerInputs) {
			List<GroupByPair<String, Integer>> reducerOutput = new ArrayList<>(
					reducers.get(reducerInputs.indexOf(gp)).generateReducerOutput(gp));
			reducerOutputs.add(reducerOutput);
		}
		for (List<GroupByPair<String, Integer>> r : reducerOutputs) {
			System.out.println("Reducer " + reducerOutputs.indexOf(r) + " output");
			r.forEach(System.out::print);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InMapperWordCount w = new InMapperWordCount();
		w.shuffleReduce();
	}

}
/**Sample Output
 * 
Mapper 0 Output
< bat 1 >
< cat 2 >
< eat 1 >
< fat 1 >
< mat 2 >
< pat 1 >
< rat 1 >
< sat 1 >

Mapper 1 Output
< bat 1 >
< eat 1 >
< hat 2 >
< mat 1 >
< oat 2 >
< pat 2 >
< rat 1 >

Mapper 2 Output
< cat 1 >
< hat 1 >
< jat 1 >
< kat 1 >
< lat 1 >
< pat 1 >
< rat 1 >
< sat 1 >
< wat 1 >
< zat 1 >

Pairs sent from Mapper 0 Reducer 0
< eat, 1 >
< mat, 2 >
Pairs sent from Mapper 0 Reducer 1
< bat, 1 >
< fat, 1 >
< rat, 1 >
Pairs sent from Mapper 0 Reducer 2
< cat, 2 >
< sat, 1 >
Pairs sent from Mapper 0 Reducer 3
< pat, 1 >
Pairs sent from Mapper 1 Reducer 0
< eat, 1 >
< mat, 1 >
Pairs sent from Mapper 1 Reducer 1
< bat, 1 >
< rat, 1 >
Pairs sent from Mapper 1 Reducer 2
< oat, 2 >
Pairs sent from Mapper 1 Reducer 3
< hat, 2 >
< pat, 2 >
Pairs sent from Mapper 2 Reducer 0
Pairs sent from Mapper 2 Reducer 1
< jat, 1 >
< rat, 1 >
< zat, 1 >
Pairs sent from Mapper 2 Reducer 2
< cat, 1 >
< kat, 1 >
< sat, 1 >
< wat, 1 >
Pairs sent from Mapper 2 Reducer 3
< hat, 1 >
< lat, 1 >
< pat, 1 >
Reducer 0 input
< eat, [1, 1] >
< mat, [2, 1] >
Reducer 1 input
< bat, [1, 1] >
< fat, [1] >
< jat, [1] >
< rat, [1, 1, 1] >
< zat, [1] >
Reducer 2 input
< cat, [2, 1] >
< kat, [1] >
< oat, [2] >
< sat, [1, 1] >
< wat, [1] >
Reducer 3 input
< hat, [2, 1] >
< lat, [1] >
< pat, [1, 2, 1] >
Reducer 0 output
< eat, 2 >
< mat, 3 >
Reducer 1 output
< bat, 2 >
< fat, 1 >
< jat, 1 >
< rat, 3 >
< zat, 1 >
Reducer 2 output
< cat, 3 >
< kat, 1 >
< oat, 2 >
< sat, 2 >
< wat, 1 >
Reducer 3 output
< hat, 3 >
< lat, 1 >
< pat, 4 >
 */
