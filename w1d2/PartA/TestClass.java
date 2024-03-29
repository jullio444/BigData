package bigData.w1d2.PartA;

import java.util.List;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Mapper m = new Mapper();
		List<Pair<String, Integer>> mapperOutput = m.createListPairs(m.readFromFile());
		System.out.println("Mapper Output\n");
		mapperOutput.forEach(System.out::print);
		System.out.println();

		Reducer r = new Reducer();
		List<GroupByPair<String, List<Integer>>> GRI = r.generateReducerInput(mapperOutput);
		System.out.println("Reducer Input\n");
		GRI.forEach(System.out::print);
		System.out.println();

		List<GroupByPair<String, Integer>> GRO = r.generateReducerOutput(GRI);
		System.out.println("Reducer Output\n");
		GRO.forEach(System.out::print);
	}

}

/***Sample Output
 * 
Mapper Output

< a, 1 >
< a, 1 >
< a, 1 >
< a, 1 >
< and, 1 >
< and, 1 >
< and, 1 >
< and, 1 >
< are, 1 >
< as, 1 >
< as, 1 >
< cat, 1 >
< cat, 1 >
< class, 1 >
< collections, 1 >
< comparator, 1 >
< each, 1 >
< each, 1 >
< extract, 1 >
< file, 1 >
< for, 1 >
< form, 1 >
< given, 1 >
< however, 1 >
< inserted, 1 >
< integer, 1 >
< into, 1 >
< involve, 1 >
< is, 1 >
< is, 1 >
< is, 1 >
< is, 1 >
< key, 1 >
< key, 1 >
< key, 1 >
< list, 1 >
< list, 1 >
< list, 1 >
< may, 1 >
< not, 1 >
< note, 1 >
< note, 1 >
< one, 1 >
< output, 1 >
< pair, 1 >
< pair, 1 >
< pair, 1 >
< program, 1 >
< program, 1 >
< same, 1 >
< should, 1 >
< sort, 1 >
< such, 1 >
< text, 1 >
< that, 1 >
< that, 1 >
< the, 1 >
< the, 1 >
< the, 1 >
< the, 1 >
< the, 1 >
< the, 1 >
< the, 1 >
< this, 1 >
< tokens, 1 >
< treat, 1 >
< two, 1 >
< using, 1 >
< value, 1 >
< value, 1 >
< value, 1 >
< where, 1 >
< will, 1 >
< word, 1 >
< word, 1 >
< word, 1 >
< words, 1 >
< words, 1 >
< writing, 1 >
< your, 1 >

Reducer Input

< a, [1, 1, 1, 1] >
< and, [1, 1, 1, 1] >
< are, [1] >
< as, [1, 1] >
< cat, [1, 1] >
< class, [1] >
< collections, [1] >
< comparator, [1] >
< each, [1, 1] >
< extract, [1] >
< file, [1] >
< for, [1] >
< form, [1] >
< given, [1] >
< however, [1] >
< inserted, [1] >
< integer, [1] >
< into, [1] >
< involve, [1] >
< is, [1, 1, 1, 1] >
< key, [1, 1, 1] >
< list, [1, 1, 1] >
< may, [1] >
< not, [1] >
< note, [1, 1] >
< one, [1] >
< output, [1] >
< pair, [1, 1, 1] >
< program, [1, 1] >
< same, [1] >
< should, [1] >
< sort, [1] >
< such, [1] >
< text, [1] >
< that, [1, 1] >
< the, [1, 1, 1, 1, 1, 1, 1] >
< this, [1] >
< tokens, [1] >
< treat, [1] >
< two, [1] >
< using, [1] >
< value, [1, 1, 1] >
< where, [1] >
< will, [1] >
< word, [1, 1, 1] >
< words, [1, 1] >
< writing, [1] >
< your, [1] >

Reducer Output

< a, 4 >
< and, 4 >
< are, 1 >
< as, 2 >
< cat, 2 >
< class, 1 >
< collections, 1 >
< comparator, 1 >
< each, 2 >
< extract, 1 >
< file, 1 >
< for, 1 >
< form, 1 >
< given, 1 >
< however, 1 >
< inserted, 1 >
< integer, 1 >
< into, 1 >
< involve, 1 >
< is, 4 >
< key, 3 >
< list, 3 >
< may, 1 >
< not, 1 >
< note, 2 >
< one, 1 >
< output, 1 >
< pair, 3 >
< program, 2 >
< same, 1 >
< should, 1 >
< sort, 1 >
< such, 1 >
< text, 1 >
< that, 2 >
< the, 7 >
< this, 1 >
< tokens, 1 >
< treat, 1 >
< two, 1 >
< using, 1 >
< value, 3 >
< where, 1 >
< will, 1 >
< word, 3 >
< words, 2 >
< writing, 1 >
< your, 1 >
 * 
 * */
 