package entity;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

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
 /*   private final Integer categoriaPersonaleNonSanitario;              //Numero di somministrazioni effettuate al personale non sanitario impiegato in strutture sanitarie e in attività lavorativa a rischio
    private final Integer categoriaOspitiRsa;   //Numero di somministrazioni effettuate ai soggetti ospiti di comunitàresidenziali indicate per giorno, regione e fascia d’età
    private final Integer categoriaPersonaleScolastico;     //Numero di somministrazioni effettuate al personale scolastico
    private final Integer categoria6069;        //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 60 e 69 anni, non appartenenti ad altre categorie prioritarie
    private final Integer categoria7079;         //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 70 e 79 anni, non appartenenti ad altre categorie prioritarie
    private final Integer categoriaOver80;       //Numero somministrazioni effettuate ai soggetti con età anagrafica maggiore o uguale a 80 anni, non appartenenti ad altre categorie prioritarie
    private final Integer categoriaSoggettiFragili;         //Numero di somministrazioni effettuate ai soggetti fragili e loro caregiver
    private final Integer categoriaForzeArmate; //Numero di somministrazioni effettuate al personale del comparto difesa e sicurezza
    private final Integer categoriaAltro;                  //Numero di somministrazioni effettuate ai soggetti non riconducibili alle precedenti categorie  */
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
            month =cal.get(Calendar.YEAR) + "-" + m;

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

    public static JavaRDD<SomministrazioniVacciniSummaryLatest> getInstance(JavaRDD<Row> dataset) {
        return
                dataset.map(
                        line -> new SomministrazioniVacciniSummaryLatest(
                                line.getString(0),
                                line.getString(1),
                                Integer.valueOf(line.getString(2)),
                                Integer.valueOf(line.getString(3)),
                                Integer.valueOf(line.getString(4)),
                                Integer.valueOf(line.getString(5)),
                                Integer.valueOf(line.getString(6)),
                                /*Integer.valueOf(line.getString(7)),
                                Integer.valueOf(line.getString(8)),
                                Integer.valueOf(line.getString(9)),
                                Integer.valueOf(line.getString(10)),
                                Integer.valueOf(line.getString(11)),
                                Integer.valueOf(line.getString(12)),
                                Integer.valueOf(line.getString(13)),
                                Integer.valueOf(line.getString(14)),
                                Integer.valueOf(line.getString(15)),
                                Integer.valueOf(line.getString(16)),*/
                                line.getString(7),
                                line.getString(8),
                                Integer.valueOf(line.getString(9)),
                                line.getString(10)
                        )).filter(Objects::nonNull);
    }

    @Override
    public String toString() {
        return "SomministrazioniVacciniSummaryLatest{" +
                "data_somministrazione=" + dataSomministrazione +
                ", area='" + area + '\'' +
                ", totale=" + totale +
                ", sesso_maschile=" + sessoMaschile +
                ", sesso_femminile=" + sessoFemminile +
               /* ", categoria_operatori_sanitari_sociosanitari=" + categoriaOperatoriSanitariSociosanitari +
                ", categoria_personale_non_sanitario=" + categoriaPersonaleNonSanitario +
                ", categoria_ospiti_rsa=" + categoriaOspitiRsa +
                ", categoria_personale_scolastico=" + categoriaPersonaleScolastico +
                ", categoria_60_69=" + categoria6069 +
                ", categoria70_79=" + categoria7079 +
                ", categoria_over80=" + categoriaOver80 +
                ", categoria_soggetti_fragili=" + categoriaSoggettiFragili +
                ", categoria_forze_armate=" + categoriaForzeArmate +
                ", categoria_altro=" + categoriaAltro +*/
                ", prima_dose=" + primaDose +
                ", seconda_dose=" + secondaDose +
                ", codice_NUTS1='" + codiceNUTS1 + '\'' +
                ", codice_NUTS2='" + codiceNUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codiceRegioneISTAT +
                ", nome_area='" + nomeArea + '\'' +
                '}';
    }
}
