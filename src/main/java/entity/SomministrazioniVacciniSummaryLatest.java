package entity;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

import static utils.Constants.DATE_WITH_DASHES;

public class SomministrazioniVacciniSummaryLatest implements Serializable {

    private final String dataSomministrazione;     //Data di somministrazione
    private final String area;                    //Sigla della regione di consegna
    private final Integer totale;                 //Numero totale di dosi di vaccino somministrate
    private final Integer sessoMaschile;         //Totale dei soggetti di sesso maschile a cui è stato somministrato il vaccino
    private final Integer sessoFemminile;        //Totale dei soggetti di sesso femminile a cui è stato somministrato il vaccino/* private final Integer categoriaOperatoriSanitariSociosanitari;     //Numero di somministrazioni effettuate agli operatori sanitari e sociosanitari
    private final Integer primaDose;                         //Numero prime somministrazioni
    private final Integer secondaDose;                       //Numero seconde somministrazioni
    private final String codiceNUTS1;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private final String codiceNUTS2;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private final Integer codiceRegioneISTAT;               //Codice ISTAT della Regione
    private final String nomeArea;                           //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public SomministrazioniVacciniSummaryLatest(String dataSomministrazione, String area, Integer totale, Integer sessoMaschile, Integer sessoFemminile, Integer primaDose, Integer secondaDose, String codiceNUTS1, String codiceNUTS2, Integer codiceRegioneISTAT, String nomeArea) {
        this.dataSomministrazione = dataSomministrazione;
        this.area = area;
        this.totale = totale;
        this.sessoMaschile = sessoMaschile;
        this.sessoFemminile = sessoFemminile;
        this.primaDose = primaDose;
        this.secondaDose = secondaDose;
        this.codiceNUTS1 = codiceNUTS1;
        this.codiceNUTS2 = codiceNUTS2;
        this.codiceRegioneISTAT = codiceRegioneISTAT;
        this.nomeArea = nomeArea;
    }

    public String getDataSomministrazione() {
        return dataSomministrazione;
    }

    public String getMonth() {
        String month = "";
        try {
            Date date1 = new SimpleDateFormat(DATE_WITH_DASHES).parse(dataSomministrazione);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date1);
            int m = cal.get(Calendar.MONTH) + 1;
            month = cal.get(Calendar.YEAR) + "-" + m;

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return month;
    }

    public Integer getTotale() {
        return totale;
    }

    public String getNomeArea() {
        return nomeArea;
    }

    public static JavaRDD<SomministrazioniVacciniSummaryLatest> getInstance(JavaRDD<String> dataset) {
        return
                dataset.map(
                        line -> {
                            String[] split = line.split(",");
                            return new SomministrazioniVacciniSummaryLatest(
                                    split[0],
                                    split[1],
                                    Integer.valueOf(split[2]),
                                    Integer.valueOf(split[3]),
                                    Integer.valueOf(split[4]),
                                    Integer.valueOf(split[5]),
                                    Integer.valueOf(split[6]),
                                    split[7],
                                    split[8],
                                    Integer.valueOf(split[9]),
                                    split[10]

                            );
                        }).filter(Objects::nonNull);
    }
}
