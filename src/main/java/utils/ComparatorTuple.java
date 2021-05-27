package utils;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

public class ComparatorTuple {

    private ComparatorTuple() {
    }

    /**
     *
     */
    public static class Tuple2ComparatorString implements java.util.Comparator<Tuple2<String, String>>, Serializable {
        @Override
        public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
            int compareOne = o1._1().compareTo(o2._1());
            int compareTwo = o1._2().compareTo(o2._2());

            if (compareOne == 0) {
                return compareTwo;
            }
            return compareOne;
        }
    }

    /**
     *
     */
    public static class Tuple3ComparatorString implements java.util.Comparator<Tuple3<String, String, String>>, Serializable {
        @Override
        public int compare(Tuple3<String, String, String> o1, Tuple3<String, String, String> o2) {
            int compareOne = o1._1().compareTo(o2._1());
            int compareTwo = o1._2().compareTo(o2._2());
            int compareThree = o1._3().compareTo(o2._3());

            if (compareOne == 0) {
                if (compareTwo == 0) {
                    return compareThree;
                } else {
                    return compareTwo;
                }
            }
            return compareOne;
        }
    }

    /**
     *
     */
    public static class Tuple2ComparatorStringDouble implements java.util.Comparator<Tuple2<Double, String>>, Serializable {
        @Override
        public int compare(Tuple2<Double, String> tuple1, Tuple2<Double, String> tuple2) {
            return tuple1._1.compareTo(tuple2._1);
        }
    }
}
