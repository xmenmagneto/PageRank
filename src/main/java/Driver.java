
public class Driver {

    public static void main(String[] args) throws Exception {
        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        String transitionMatrix = args[0]; //directory of transition.txt
        String prMatrix = args[1]; // /input/pr0  /input/pr1  /input/pr0
        String subPR = args[2]; //  /output/subpr
        int count = Integer.parseInt(args[3]);  //想要迭代多少次

        for(int i = 0;  i < count;  i++) {
            //transitionMatrix始终不变,可以写死

            String[] args1 = {transitionMatrix, prMatrix + i, subPR + i};
            multiplication.main(args1);

            String[] args2 = {subPR + i, prMatrix + (i + 1)};
            sum.main(args2);
        }
    }
}
