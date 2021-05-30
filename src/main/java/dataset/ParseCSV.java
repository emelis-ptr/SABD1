package dataset;

import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniLatest;
import entity.SomministrazioniVacciniSummaryLatest;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static utils.Constants.DATE_WITH_DASHES;
import static utils.Constants.SPLIT_COMMA;

public class ParseCSV {

    private ParseCSV() {
    }

    /**
     * SomministrazioniVacciniSummaryLatest
     *
     * @param csvLine:
     * @return :
     */
    public static SomministrazioniVacciniSummaryLatest parseCsvSVSL(String csvLine) {

        SomministrazioniVacciniSummaryLatest somministrazioniVacciniSummaryLatest;
        String[] csvValues = csvLine.split(SPLIT_COMMA);

        somministrazioniVacciniSummaryLatest = new SomministrazioniVacciniSummaryLatest(
                csvValues[0],
                csvValues[1],
                Integer.valueOf(csvValues[2]),
                Integer.valueOf(csvValues[3]),
                Integer.valueOf(csvValues[4]),
                Integer.valueOf(csvValues[5]),
                Integer.valueOf(csvValues[6]),
                Integer.valueOf(csvValues[7]),
                Integer.valueOf(csvValues[8]),
                Integer.valueOf(csvValues[9]),
                Integer.valueOf(csvValues[10]),
                Integer.valueOf(csvValues[11]),
                Integer.valueOf(csvValues[12]),
                Integer.valueOf(csvValues[13]),
                Integer.valueOf(csvValues[14]),
                Integer.valueOf(csvValues[15]),
                Integer.valueOf(csvValues[16]),
                csvValues[17],
                csvValues[18],
                Integer.valueOf(csvValues[19]),
                csvValues[20]
        );

        return somministrazioniVacciniSummaryLatest;
    }

    /**
     * PuntiSomministrazioneTipologia
     *
     * @param csvLine:
     * @return :
     */
    public static PuntiSomministrazioneTipologia parseCSVPST(String csvLine) {

        PuntiSomministrazioneTipologia somministrazioneTipologia;
        String[] csvValues = csvLine.split(SPLIT_COMMA);

        // ABR,  AVEZZANO-SULMONA-L'AQUILA,  Ospedaliero,    ITF,    ITF1,   13, Abruzzo
        somministrazioneTipologia = new PuntiSomministrazioneTipologia(
                csvValues[0],
                csvValues[1],
                csvValues[2],
                csvValues[3],
                csvValues[4],
                Integer.valueOf(csvValues[5]),
                csvValues[6]
        );

        return somministrazioneTipologia;
    }

    /**
     * SomministrazioniVacciniLatest
     *
     * @param csvLine:
     * @return :
     */
    public static SomministrazioniVacciniLatest parseCsvSVL(String csvLine) throws ParseException {

        SomministrazioniVacciniLatest somministrazioniVacciniLatest;
        String[] csvValues = csvLine.split(SPLIT_COMMA);

        somministrazioniVacciniLatest = new SomministrazioniVacciniLatest(
                csvValues[0],
                csvValues[1],
                csvValues[2],
                csvValues[3],
                Integer.valueOf(csvValues[4]),
                Integer.valueOf(csvValues[5]),
                Integer.valueOf(csvValues[6]),
                Integer.valueOf(csvValues[7]),
                Integer.valueOf(csvValues[8]),
                Integer.valueOf(csvValues[9]),
                Integer.valueOf(csvValues[10]),
                Integer.valueOf(csvValues[11]),
                Integer.valueOf(csvValues[12]),
                Integer.valueOf(csvValues[13]),
                Integer.valueOf(csvValues[14]),
                Integer.valueOf(csvValues[15]),
                Integer.valueOf(csvValues[16]),
                Integer.valueOf(csvValues[17]),
                csvValues[18],
                csvValues[19],
                Integer.valueOf(csvValues[20]),
                csvValues[21]
        );

        return somministrazioniVacciniLatest;
    }
}
