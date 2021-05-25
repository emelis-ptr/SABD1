package entity;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class SomministrazioniVacciniSummaryLatest implements Serializable {

    private String data_somministrazione;     //Data di somministrazione
    private String area;                    //Sigla della regione di consegna
    private Integer totale;                 //Numero totale di dosi di vaccino somministrate
    private Integer sesso_maschile;         //Totale dei soggetti di sesso maschile a cui è stato somministrato il vaccino
    private Integer sesso_femminile;        //Totale dei soggetti di sesso femminile a cui è stato somministrato il vaccino
    private Integer categoria_operatori_sanitari_sociosanitari;     //Numero di somministrazioni effettuate agli operatori sanitari e sociosanitari
    private Integer categoria_personale_non_sanitario;              //Numero di somministrazioni effettuate al personale non sanitario impiegato in strutture sanitarie e in attività lavorativa a rischio
    private Integer categoria_ospiti_rsa;   //Numero di somministrazioni effettuate ai soggetti ospiti di comunitàresidenziali indicate per giorno, regione e fascia d’età
    private Integer categoria_personale_scolastico;     //Numero di somministrazioni effettuate al personale scolastico
    private Integer categoria_60_69;        //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 60 e 69 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria70_79;         //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 70 e 79 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria_over80;       //Numero somministrazioni effettuate ai soggetti con età anagrafica maggiore o uguale a 80 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria_soggetti_fragili;         //Numero di somministrazioni effettuate ai soggetti fragili e loro caregiver
    private Integer categoria_forze_armate; //Numero di somministrazioni effettuate al personale del comparto difesa e sicurezza
    private Integer categoria_altro;                    //Numero di somministrazioni effettuate ai soggetti non riconducibili alle precedenti categorie
    private Integer prima_dose;                         //Numero prime somministrazioni
    private Integer seconda_dose;                       //Numero seconde somministrazioni
    private String codice_NUTS1;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private String codice_NUTS2;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private Integer codice_regione_ISTAT;               //Codice ISTAT della Regione
    private String nome_area;                           //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public SomministrazioniVacciniSummaryLatest(String data_somministrazione, String area, Integer totale, Integer sesso_maschile, Integer sesso_femminile, Integer categoria_operatori_sanitari_sociosanitari, Integer categoria_personale_non_sanitario, Integer categoria_ospiti_rsa, Integer categoria_personale_scolastico, Integer categoria_60_69, Integer categoria70_79, Integer categoria_over80, Integer categoria_soggetti_fragili, Integer categoria_forze_armate, Integer categoria_altro, Integer prima_dose, Integer seconda_dose, String codice_NUTS1, String codice_NUTS2, Integer codice_regione_ISTAT, String nome_area) {
        this.data_somministrazione = data_somministrazione;
        this.area = area;
        this.totale = totale;
        this.sesso_maschile = sesso_maschile;
        this.sesso_femminile = sesso_femminile;
        this.categoria_operatori_sanitari_sociosanitari = categoria_operatori_sanitari_sociosanitari;
        this.categoria_personale_non_sanitario = categoria_personale_non_sanitario;
        this.categoria_ospiti_rsa = categoria_ospiti_rsa;
        this.categoria_personale_scolastico = categoria_personale_scolastico;
        this.categoria_60_69 = categoria_60_69;
        this.categoria70_79 = categoria70_79;
        this.categoria_over80 = categoria_over80;
        this.categoria_soggetti_fragili = categoria_soggetti_fragili;
        this.categoria_forze_armate = categoria_forze_armate;
        this.categoria_altro = categoria_altro;
        this.prima_dose = prima_dose;
        this.seconda_dose = seconda_dose;
        this.codice_NUTS1 = codice_NUTS1;
        this.codice_NUTS2 = codice_NUTS2;
        this.codice_regione_ISTAT = codice_regione_ISTAT;
        this.nome_area = nome_area;
    }

    public String getData_somministrazione() {
        return data_somministrazione;
    }

    public String getMonth() {
        String month = "";
        try {
            Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse(data_somministrazione);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date1);
            month = String.valueOf(cal.get(Calendar.MONTH) + 1);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return month;
    }

    public String getArea() {
        return area;
    }

    public Integer getTotale() {
        return totale;
    }

    public Integer getSesso_maschile() {
        return sesso_maschile;
    }

    public Integer getSesso_femminile() {
        return sesso_femminile;
    }

    public Integer getCategoria_operatori_sanitari_sociosanitari() {
        return categoria_operatori_sanitari_sociosanitari;
    }

    public Integer getCategoria_personale_non_sanitario() {
        return categoria_personale_non_sanitario;
    }

    public Integer getCategoria_ospiti_rsa() {
        return categoria_ospiti_rsa;
    }

    public Integer getCategoria_personale_scolastico() {
        return categoria_personale_scolastico;
    }

    public Integer getCategoria_60_69() {
        return categoria_60_69;
    }

    public Integer getCategoria70_79() {
        return categoria70_79;
    }

    public Integer getCategoria_over80() {
        return categoria_over80;
    }

    public Integer getCategoria_soggetti_fragili() {
        return categoria_soggetti_fragili;
    }

    public Integer getCategoria_forze_armate() {
        return categoria_forze_armate;
    }

    public Integer getCategoria_altro() {
        return categoria_altro;
    }

    public Integer getPrima_dose() {
        return prima_dose;
    }

    public Integer getSeconda_dose() {
        return seconda_dose;
    }

    public String getCodice_NUTS1() {
        return codice_NUTS1;
    }

    public String getCodice_NUTS2() {
        return codice_NUTS2;
    }

    public Integer getCodice_regione_ISTAT() {
        return codice_regione_ISTAT;
    }

    public String getNome_area() {
        return nome_area;
    }

    @Override
    public String toString() {
        return "SomministrazioniVacciniSummaryLatest{" +
                "data_somministrazione=" + data_somministrazione +
                ", area='" + area + '\'' +
                ", totale=" + totale +
                ", sesso_maschile=" + sesso_maschile +
                ", sesso_femminile=" + sesso_femminile +
                ", categoria_operatori_sanitari_sociosanitari=" + categoria_operatori_sanitari_sociosanitari +
                ", categoria_personale_non_sanitario=" + categoria_personale_non_sanitario +
                ", categoria_ospiti_rsa=" + categoria_ospiti_rsa +
                ", categoria_personale_scolastico=" + categoria_personale_scolastico +
                ", categoria_60_69=" + categoria_60_69 +
                ", categoria70_79=" + categoria70_79 +
                ", categoria_over80=" + categoria_over80 +
                ", categoria_soggetti_fragili=" + categoria_soggetti_fragili +
                ", categoria_forze_armate=" + categoria_forze_armate +
                ", categoria_altro=" + categoria_altro +
                ", prima_dose=" + prima_dose +
                ", seconda_dose=" + seconda_dose +
                ", codice_NUTS1='" + codice_NUTS1 + '\'' +
                ", codice_NUTS2='" + codice_NUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codice_regione_ISTAT +
                ", nome_area='" + nome_area + '\'' +
                '}';
    }
}
