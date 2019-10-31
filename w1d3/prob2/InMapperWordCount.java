package bigData.w1d3.prob2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class InMapperWordCount {
	private List<Mapper> mappers;
	private List<Reducer> reducers;
	private int m;
	private int r;

	InMapperWordCount() {
		mappers = new ArrayList<>(Arrays.asList(new Mapper("//src//bigData//w1d3//prob2//textfile/testData1.txt"),
				new Mapper("//src//bigData//w1d3//prob2//textfile/testData2.txt"),
				new Mapper("//src//bigData//w1d3//prob2//textfile/testData3.txt"),
				new Mapper("//src//bigData//w1d3//prob2//textfile/testData4.txt")));
		reducers = new ArrayList<>(Arrays.asList(new Reducer(), new Reducer(), new Reducer()));
		m = mappers.size();
		r = reducers.size();
	}

	public List<LinkedHashMap<Character, Pair<Integer, Integer>>> jobTracker() {
		List<LinkedHashMap<Character, Pair<Integer, Integer>>> mapperOutputs = new ArrayList<>();
		LinkedHashMap<Character, Pair<Integer, Integer>> mapperOutput1 = mappers.get(0)
				.createMappedListPairs(mappers.get(0).readFromFile());
		mapperOutputs.add(sortCollection(mapperOutput1));
		System.out.println("Mapper 0 Output");
		for (Map.Entry<Character, Pair<Integer, Integer>> en : mapperOutput1.entrySet())
			System.out.println("< " + en.getKey() + " , " + en.getValue() + " >");
		System.out.println();

		LinkedHashMap<Character, Pair<Integer, Integer>> mapperOutput2 = mappers.get(1)
				.createMappedListPairs(mappers.get(1).readFromFile());
		mapperOutputs.add(sortCollection(mapperOutput2));
		System.out.println("Mapper 1 Output");
		for (Map.Entry<Character, Pair<Integer, Integer>> en : mapperOutput2.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();

		LinkedHashMap<Character, Pair<Integer, Integer>> mapperOutput3 = mappers.get(2)
				.createMappedListPairs(mappers.get(2).readFromFile());
		mapperOutputs.add(sortCollection(mapperOutput3));
		System.out.println("Mapper 2 Output");
		for (Map.Entry<Character, Pair<Integer, Integer>> en : mapperOutput3.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();

		LinkedHashMap<Character, Pair<Integer, Integer>> mapperOutput4 = mappers.get(3)
				.createMappedListPairs(mappers.get(3).readFromFile());
		mapperOutputs.add(sortCollection(mapperOutput4));
		System.out.println("Mapper 3 Output");
		for (Map.Entry<Character, Pair<Integer, Integer>> en : mapperOutput4.entrySet())
			System.out.println("< " + en.getKey() + " " + en.getValue() + " >");
		System.out.println();


		return mapperOutputs;
	}

	public LinkedHashMap<Character, Pair<Integer, Integer>> sortCollection(LinkedHashMap<Character, Pair<Integer, Integer>> map) {
		return map.entrySet().stream()
				.sorted(Map.Entry.comparingByKey()) 			
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				(oldValue, newValue) -> oldValue, LinkedHashMap::new));
	}

	public List<List<GroupByPair<Character, List<Integer>>>> pairsSentToReducers() {
		List<List<GroupByPair<Character, List<Integer>>>> reducerInputs = new ArrayList<>();
		List<LinkedHashMap<Character, Pair<Integer, Integer>>> Mappers = jobTracker();
		List<GroupByPair<Character, List<Integer>>> reducerInputs0 = new ArrayList<>();
		List<GroupByPair<Character, List<Integer>>> reducerInputs1 = new ArrayList<>();
		List<GroupByPair<Character, List<Integer>>> reducerInputs2 = new ArrayList<>();

		for (LinkedHashMap<Character, Pair<Integer, Integer>> mapperOutput : Mappers) {
			List<GroupByPair<Character, List<Integer>>> reducerInput0 = new ArrayList<>();
			List<GroupByPair<Character, List<Integer>>> reducerInput1 = new ArrayList<>();
			List<GroupByPair<Character, List<Integer>>> reducerInput2 = new ArrayList<>();

			for (Map.Entry<Character, Pair<Integer, Integer>> en : mapperOutput.entrySet()) {
				if (getPartition(en.getKey()) == 0) {
					reducerInput0.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
					reducerInputs0.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
				} else if (getPartition(en.getKey()) == 1) {
					reducerInput1.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
					reducerInputs1.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
				} else if (getPartition(en.getKey()) == 2) {
					reducerInput2.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
					reducerInputs2.add(new GroupByPair<Character, List<Integer>>(en.getKey(),
							Arrays.asList(en.getValue().getK(), en.getValue().getV())));
				}
			}

			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 0");
			reducerInput0.forEach(System.out::println);
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 1");
			reducerInput1.forEach(System.out::println);
			System.out.println("Pairs sent from Mapper " + Mappers.indexOf(mapperOutput) + " Reducer 2");
			reducerInput2.forEach(System.out::println);
		}
		reducerInputs.add(reducerInputs0);
		reducerInputs.add(reducerInputs1);
		reducerInputs.add(reducerInputs2);
		return reducerInputs;
	}

	public int getPartition(Character key) {
		return Math.abs(key.hashCode() % r);
	}

	public void shuffleReduce() {
		List<List<GroupByPair<Character, List<Integer>>>> listOfPairsSentToReducers = pairsSentToReducers();
		List<List<GroupByPair<Character, List<List<Integer>>>>> reducerInputs = new ArrayList<>();
		for (List<GroupByPair<Character, List<Integer>>> lp : listOfPairsSentToReducers) {
			List<GroupByPair<Character, List<List<Integer>>>> reducerInput = new ArrayList<>(reducers.
					get(0).generateReducerInput(lp));
			reducerInputs.add(reducerInput);
		}
		for (List<GroupByPair<Character, List<List<Integer>>>> r : reducerInputs) {
			System.out.println("Reducer " + reducerInputs.indexOf(r) + " input");
			r.forEach(System.out::println);
		}

		List<List<GroupByPair<Character, Double>>> reducerOutputs = new ArrayList<>();
		for (List<GroupByPair<Character, List<List<Integer>>>> gp : reducerInputs) {
			List<GroupByPair<Character, Double>> reducerOutput = new ArrayList<>(
					reducers.get(reducerInputs.indexOf(gp)).generateReducerOutput(gp));
			reducerOutputs.add(reducerOutput);
		}
		for (List<GroupByPair<Character, Double>> r : reducerOutputs) {
			System.out.println("Reducer " + reducerOutputs.indexOf(r) + " output");
			r.forEach(System.out::println);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InMapperWordCount w = new InMapperWordCount();

		w.shuffleReduce();
	}

}
/**Sample output
 *
 Mapper 0 Output
< a , [14,4] >
< i , [4,2] >
< b , [9,1] >
< l , [14,3] >
< e , [9,1] >
< h , [15,3] >
< p , [4,1] >
< v , [4,1] >
< m , [4,1] >

Mapper 1 Output
< s [5,2] >
< i [2,1] >
< t [8,2] >
< e [15,3] >
< d [12,4] >
< m [4,1] >
< c [5,1] >
< l [7,2] >
< u [2,1] >
< a [2,1] >
< b [4,1] >
< f [3,1] >

Mapper 2 Output
< m [18,2] >
< i [11,2] >
< v [4,1] >
< f [3,1] >
< t [3,1] >
< d [14,2] >
< o [2,1] >
< c [13,1] >
< s [2,1] >
< l [3,1] >
< u [2,1] >
< e [5,1] >

Mapper 3 Output
< e [5,1] >
< i [10,5] >
< b [4,1] >
< y [9,2] >
< l [4,1] >
< f [4,1] >
< o [5,1] >
< s [8,2] >
< m [8,2] >
< r [3,1] >
< w [23,4] >
< a [1,1] >

Pairs sent from Mapper 0 Reducer 0
< i , [4, 2] >
< l , [14, 3] >
Pairs sent from Mapper 0 Reducer 1
< a , [14, 4] >
< m , [4, 1] >
< p , [4, 1] >
< v , [4, 1] >
Pairs sent from Mapper 0 Reducer 2
< b , [9, 1] >
< e , [9, 1] >
< h , [15, 3] >
Pairs sent from Mapper 1 Reducer 0
< c , [5, 1] >
< f , [3, 1] >
< i , [2, 1] >
< l , [7, 2] >
< u , [2, 1] >
Pairs sent from Mapper 1 Reducer 1
< a , [2, 1] >
< d , [12, 4] >
< m , [4, 1] >
< s , [5, 2] >
Pairs sent from Mapper 1 Reducer 2
< b , [4, 1] >
< e , [15, 3] >
< t , [8, 2] >
Pairs sent from Mapper 2 Reducer 0
< c , [13, 1] >
< f , [3, 1] >
< i , [11, 2] >
< l , [3, 1] >
< o , [2, 1] >
< u , [2, 1] >
Pairs sent from Mapper 2 Reducer 1
< d , [14, 2] >
< m , [18, 2] >
< s , [2, 1] >
< v , [4, 1] >
Pairs sent from Mapper 2 Reducer 2
< e , [5, 1] >
< t , [3, 1] >
Pairs sent from Mapper 3 Reducer 0
< f , [4, 1] >
< i , [10, 5] >
< l , [4, 1] >
< o , [5, 1] >
< r , [3, 1] >
Pairs sent from Mapper 3 Reducer 1
< a , [1, 1] >
< m , [8, 2] >
< s , [8, 2] >
< y , [9, 2] >
Pairs sent from Mapper 3 Reducer 2
< b , [4, 1] >
< e , [5, 1] >
< w , [23, 4] >
Reducer 0 input
< c , [[5, 1], [13, 1]] >
< f , [[3, 1], [3, 1], [4, 1]] >
< i , [[4, 2], [2, 1], [11, 2], [10, 5]] >
< l , [[14, 3], [7, 2], [3, 1], [4, 1]] >
< o , [[2, 1], [5, 1]] >
< r , [[3, 1]] >
< u , [[2, 1], [2, 1]] >
Reducer 1 input
< a , [[14, 4], [2, 1], [1, 1]] >
< d , [[12, 4], [14, 2]] >
< m , [[4, 1], [4, 1], [18, 2], [8, 2]] >
< p , [[4, 1]] >
< s , [[5, 2], [2, 1], [8, 2]] >
< v , [[4, 1], [4, 1]] >
< y , [[9, 2]] >
Reducer 2 input
< b , [[9, 1], [4, 1], [4, 1]] >
< e , [[9, 1], [15, 3], [5, 1], [5, 1]] >
< h , [[15, 3]] >
< t , [[8, 2], [3, 1]] >
< w , [[23, 4]] >
Reducer 0 output
< c , 9.0 >
< f , 3.3333333333333335 >
< i , 2.7 >
< l , 4.0 >
< o , 3.5 >
< r , 3.0 >
< u , 2.0 >
Reducer 1 output
< a , 2.8333333333333335 >
< d , 4.333333333333333 >
< m , 5.666666666666667 >
< p , 4.0 >
< s , 3.0 >
< v , 4.0 >
< y , 4.5 >
Reducer 2 output
< b , 5.666666666666667 >
< e , 5.666666666666667 >
< h , 5.0 >
< t , 3.6666666666666665 >
< w , 5.75 >
*/