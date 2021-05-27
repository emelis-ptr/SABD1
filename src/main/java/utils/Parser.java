package utils;

import entity.PuntiSomministrazioneTipologia;
import entity.SomministrazioniVacciniSummaryLatest;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class Parser {

    public static SomministrazioniVacciniSummaryLatest parseCsvSVSL(String csvLine) {

        SomministrazioniVacciniSummaryLatest somministrazioniVacciniSummaryLatest;
        String[] csvValues = csvLine.split(",");

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


    public static PuntiSomministrazioneTipologia parseCSVPST(String csvLine) {

        PuntiSomministrazioneTipologia somministrazioneTipologia;
        String[] csvValues = csvLine.split(",");

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
}
