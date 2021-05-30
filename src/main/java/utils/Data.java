package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static utils.Constants.YEAR_MONTH_FORMAT;

public class Data {

    private Data() {
    }

    /**
     * Metodo che prende il primo giorno del mese successivo
     *
     * @param date:
     * @return :
     * @throws ParseException :
     */
    public static Calendar getNextMonth(String date) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(YEAR_MONTH_FORMAT);
        Date dateNextMonth = dateFormat.parse(date + "-01");

        Calendar cal = Calendar.getInstance();
        cal.setTime(dateNextMonth);
        cal.add(Calendar.MONTH, 1);
        return cal;
    }
}
