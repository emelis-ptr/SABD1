package utils;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;

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
    public static class Tuple2ComparatorDateString implements java.util.Comparator<Tuple2<Date, String>>, Serializable {
        @Override
        public int compare(Tuple2<Date, String> o1, Tuple2<Date, String> o2) {
            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
            String date1 = sdformat.format(o1._1());
            String date2 = sdformat.format(o2._1());
            int uno2 = date1.compareTo(date2);
            int due = o1._2().compareTo(o2._2());

            if (uno2 == 0) {
                return due;

            }
            return uno2;
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
    public static class Tuple3ComparatorDateStringDouble implements java.util.Comparator<Tuple3<Date, String, Double>>, Serializable {
        @Override
        public int compare(Tuple3<Date, String, Double> o1, Tuple3<Date, String, Double> o2) {
            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
            String date1 = sdformat.format(o1._1());
            String date2 = sdformat.format(o2._1());
            int uno = date1.compareTo(date2);
            int due = o1._2().compareTo(o2._2());
            int tre = o1._3().compareTo(o2._3());

            if (uno == 0) {
                if (due == 0) {
                    return tre;
                } else {
                    return due;
                }
            }
            return uno;
        }
    }
}
