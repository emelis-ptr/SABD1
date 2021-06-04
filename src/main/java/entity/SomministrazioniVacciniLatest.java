package entity;

import org.apache.spark.api.java.JavaRDD;

import java.util.Objects;

public class SomministrazioniVacciniLatest {

    private final String dataSomministrazione;     //Data di somministrazione
    private final String fornitore;               //Nome completo del fornitore del vaccino
    private final String area;                    //Sigla della regione di consegna
    private final String fasciaAnagrafica;       //Fascia anagrafica a cui appartengono i soggetti a cui è stato somministrato il vaccino
    private final Integer sessoMaschile;         //Totale dei soggetti di sesso maschile a cui è stato somministrato il vaccino
    private final Integer sessoFemminile;
    private final Integer primaDose;                         //Numero prime somministrazioni
    private final Integer secondaDose;                       //Numero seconde somministrazioni
    private final String codiceNUTS1;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private final String codiceNUTS2;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private final Integer codiceRegioneISTAT;               //Codice ISTAT della Regione
    private final String nomeArea;                           //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public SomministrazioniVacciniLatest(String dataSomministrazione, String fornitore, String area, String fasciaAnagrafica, Integer sessoMaschile, Integer sessoFemminile, Integer primaDose, Integer secondaDose, String codiceNUTS1, String codiceNUTS2, Integer codiceRegioneISTAT, String nomeArea) {
        this.dataSomministrazione = dataSomministrazione;
        this.fornitore = fornitore;
        this.area = area;
        this.fasciaAnagrafica = fasciaAnagrafica;
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

    public String getFasciaAnagrafica() {
        return fasciaAnagrafica;
    }

    public Integer getSessoFemminile() {
        return sessoFemminile;
    }

    public String getNomeArea() {
        return nomeArea;
    }

    public static JavaRDD<SomministrazioniVacciniLatest> getInstance(JavaRDD<String> dataset) {
        //Data(anno-mese), area, fascia_anagrafica -- data , num_vacc_femminile
        return
                dataset.map(
                        line ->
                        {
                            String[] split = line.split(",");
                            return new SomministrazioniVacciniLatest(
                                    split[0],
                                    split[1],
                                    split[2],
                                    split[3],
                                    Integer.valueOf(split[4]),
                                    Integer.valueOf(split[5]),
                                    Integer.valueOf(split[6]),
                                    Integer.valueOf(split[7]),
                                    split[8],
                                    split[9],
                                    Integer.valueOf(split[10]),
                                    split[11]);
                        })
                        .filter(Objects::nonNull);
    }

    @Override
    public String toString() {
        return "SomministazioniVacciniLatest{" +
                ", data_somministrazione=" + dataSomministrazione +
                ", fornitore='" + fornitore + '\'' +
                ", area='" + area + '\'' +
                ", fascia_anagrafica='" + fasciaAnagrafica + '\'' +
                ", sesso_maschile=" + sessoMaschile +
                ", sesso_femminile=" + sessoFemminile +
                ", prima_dose=" + primaDose +
                ", seconda_dose=" + secondaDose +
                ", codice_NUTS1='" + codiceNUTS1 + '\'' +
                ", codice_NUTS2='" + codiceNUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codiceRegioneISTAT +
                ", nome_area='" + nomeArea + '\'' +
                '}';
    }
}
