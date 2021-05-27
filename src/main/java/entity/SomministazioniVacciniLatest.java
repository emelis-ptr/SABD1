package entity;

import java.time.LocalDate;

public class SomministazioniVacciniLatest {

    private LocalDate dataSomministrazione;     //Data di somministrazione
    private String fornitore;               //Nome completo del fornitore del vaccino
    private String area;                    //Sigla della regione di consegna
    private String fasciaAnagrafica;       //Fascia anagrafica a cui appartengono i soggetti a cui è stato somministrato il vaccino
    private Integer sessoMaschile;         //Totale dei soggetti di sesso maschile a cui è stato somministrato il vaccino
    private Integer sessoFemminile;        //Totale dei soggetti di sesso femminile a cui è stato somministrato il vaccino
    private Integer categoriaOperatoriSanitariSociosanitari;     //Numero di somministrazioni effettuate agli operatori sanitari e sociosanitari
    private Integer categoriaPersonaleNonSanitario;              //Numero di somministrazioni effettuate al personale non sanitario impiegato in strutture sanitarie e in attività lavorativa a rischio
    private Integer categoriaOspitiRsa;   //Numero di somministrazioni effettuate ai soggetti ospiti di comunitàresidenziali indicate per giorno, regione e fascia d’età
    private Integer categoria6069;        //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 60 e 69 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoria7079;         //Numero somministrazioni effettuate ai soggetti con età anagrafica compresa tra 70 e 79 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoriaOver80;       //Numero somministrazioni effettuate ai soggetti con età anagrafica maggiore o uguale a 80 anni, non appartenenti ad altre categorie prioritarie
    private Integer categoriaForzeArmate; //Numero di somministrazioni effettuate al personale del comparto difesa e sicurezza
    private Integer categoriaPersonaleScolastico;     //Numero di somministrazioni effettuate al personale scolastico
    private Integer categoriaSoggettiFragili;         //Numero di somministrazioni effettuate ai soggetti fragili e loro caregiver
    private Integer categoriaAltro;                    //Numero di somministrazioni effettuate ai soggetti non riconducibili alle precedenti categorie
    private Integer primaDose;                         //Numero prime somministrazioni
    private Integer secondaDose;                       //Numero seconde somministrazioni
    private String codiceNUTS1;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private String codiceNUTS2;                        //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private Integer codiceRegioneISTAT;               //Codice ISTAT della Regione
    private String nomeArea;                           //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public LocalDate getDataSomministrazione() {
        return dataSomministrazione;
    }

    public String getFornitore() {
        return fornitore;
    }

    public String getArea() {
        return area;
    }

    public String getFasciaAnagrafica() {
        return fasciaAnagrafica;
    }

    public Integer getSessoMaschile() {
        return sessoMaschile;
    }

    public Integer getSessoFemminile() {
        return sessoFemminile;
    }

    public Integer getCategoriaOperatoriSanitariSociosanitari() {
        return categoriaOperatoriSanitariSociosanitari;
    }

    public Integer getCategoriaPersonaleNonSanitario() {
        return categoriaPersonaleNonSanitario;
    }

    public Integer getCategoriaOspitiRsa() {
        return categoriaOspitiRsa;
    }

    public Integer getCategoria6069() {
        return categoria6069;
    }

    public Integer getCategoria7079() {
        return categoria7079;
    }

    public Integer getCategoriaOver80() {
        return categoriaOver80;
    }

    public Integer getCategoriaForzeArmate() {
        return categoriaForzeArmate;
    }

    public Integer getCategoriaPersonaleScolastico() {
        return categoriaPersonaleScolastico;
    }

    public Integer getCategoriaSoggettiFragili() {
        return categoriaSoggettiFragili;
    }

    public Integer getCategoriaAltro() {
        return categoriaAltro;
    }

    public Integer getPrimaDose() {
        return primaDose;
    }

    public Integer getSecondaDose() {
        return secondaDose;
    }

    public String getCodiceNUTS1() {
        return codiceNUTS1;
    }

    public String getCodiceNUTS2() {
        return codiceNUTS2;
    }

    public Integer getCodiceRegioneISTAT() {
        return codiceRegioneISTAT;
    }

    public String getNomeArea() {
        return nomeArea;
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
                ", categoria_operatori_sanitari_sociosanitari=" + categoriaOperatoriSanitariSociosanitari +
                ", categoria_personale_non_sanitario=" + categoriaPersonaleNonSanitario +
                ", categoria_ospiti_rsa=" + categoriaOspitiRsa +
                ", categoria_60_69=" + categoria6069 +
                ", categoria70_79=" + categoria7079 +
                ", categoria_over80=" + categoriaOver80 +
                ", categoria_forze_armate=" + categoriaForzeArmate +
                ", categoria_personale_scolastico=" + categoriaPersonaleScolastico +
                ", categoria_soggetti_fragili=" + categoriaSoggettiFragili +
                ", categoria_altro=" + categoriaAltro +
                ", prima_dose=" + primaDose +
                ", seconda_dose=" + secondaDose +
                ", codice_NUTS1='" + codiceNUTS1 + '\'' +
                ", codice_NUTS2='" + codiceNUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codiceRegioneISTAT +
                ", nome_area='" + nomeArea + '\'' +
                '}';
    }
}
