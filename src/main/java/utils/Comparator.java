package utils;

import scala.Serializable;
import scala.Tuple2;

public class Comparator {

    public static class TupleComparator implements java.util.Comparator<Tuple2<String, String>>, Serializable {
        @Override
        public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
          /*  if (Integer.parseInt(o1._2()) == Integer.parseInt(o2._2()))
                return o1._1().compareTo(o2._1());*/
            return Integer.parseInt(o1._2()) - Integer.parseInt(o2._2());
        }
    }

}
