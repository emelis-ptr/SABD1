package entity;

import java.util.Date;

public class SomministrazioniVacciniSummaryLatest {

    private String id;
    private Date data_somministrazione;     //Data di somministrazione
    private String fornitore;               //Nome completo del fornitore del vaccino
    private String area;                    //Sigla della regione di consegna
    private String fascia_anagrafica;       //Fascia anagrafica a cui appartengono i soggetti a cui è stato somministrato il vaccino
    private Integer sesso_maschile;         //Totale dei soggetti di sesso maschile a cui è stato somministrato il vaccino
    private Integer sesso_femminile;        //Totale dei soggetti di sesso femminile a cui è stato somministrato il vaccino
    private Integer categoria_operatori_sanitari_sociosanitari;     //Numero di somministrazioni effettuate agli operatori sanitari e sociosanitari
    private Integer categoria_personale_non_sanitario;              //Numero di somministrazioni effettuate al personale non sanitario impiegato in strutture sanitarie e in attività lavorativa a rischio
    private Integer categoria_ospiti_rsa;   //Numero di somministrazioni effettuate ai soggetti ospiti di comunitàresidenziali indicate per giorno, regione e fascia d’età
    private Integer categoria_60_69;        //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 60 e 69 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria70_79;         //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 70 e 79 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria_over80;       //Numero somministrazioni effettuate ai soggetti con età anagrafica maggiore o uguale a 80 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria_forze_armate; //Numero di somministrazioni effettuate al personale del comparto difesa e sicurezza
    private Integer categoria_personale_scolastico;     //Numero di somministrazioni effettuate al personale scolastico
    private Integer categoria_soggetti_fragili;         //Numero di somministrazioni effettuate ai soggetti fragili e loro caregiver
    private Integer categoria_altro;                    //Numero di somministrazioni effettuate ai soggetti non riconducibili alle precedenti categorie
    private Integer prima_dose;                         //Numero prime somministrazioni
    private Integer seconda_dose;                       //Numero seconde somministrazioni
    private String codice_NUTS1;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private String codice_NUTS2;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private Integer codice_regione_ISTAT;               //Codice ISTAT della Regione
    private String nome_area;                           //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public SomministrazioniVacciniSummaryLatest(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Date getData_somministrazione() {
        return data_somministrazione;
    }

    public String getFornitore() {
        return fornitore;
    }

    public String getArea() {
        return area;
    }

    public String getFascia_anagrafica() {
        return fascia_anagrafica;
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

    public Integer getCategoria_60_69() {
        return categoria_60_69;
    }

    public Integer getCategoria70_79() {
        return categoria70_79;
    }

    public Integer getCategoria_over80() {
        return categoria_over80;
    }

    public Integer getCategoria_forze_armate() {
        return categoria_forze_armate;
    }

    public Integer getCategoria_personale_scolastico() {
        return categoria_personale_scolastico;
    }

    public Integer getCategoria_soggetti_fragili() {
        return categoria_soggetti_fragili;
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
                "id='" + id + '\'' +
                ", data_somministrazione=" + data_somministrazione +
                ", fornitore='" + fornitore + '\'' +
                ", area='" + area + '\'' +
                ", fascia_anagrafica='" + fascia_anagrafica + '\'' +
                ", sesso_maschile=" + sesso_maschile +
                ", sesso_femminile=" + sesso_femminile +
                ", categoria_operatori_sanitari_sociosanitari=" + categoria_operatori_sanitari_sociosanitari +
                ", categoria_personale_non_sanitario=" + categoria_personale_non_sanitario +
                ", categoria_ospiti_rsa=" + categoria_ospiti_rsa +
                ", categoria_60_69=" + categoria_60_69 +
                ", categoria70_79=" + categoria70_79 +
                ", categoria_over80=" + categoria_over80 +
                ", categoria_forze_armate=" + categoria_forze_armate +
                ", categoria_personale_scolastico=" + categoria_personale_scolastico +
                ", categoria_soggetti_fragili=" + categoria_soggetti_fragili +
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
