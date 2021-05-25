package entity;

import java.io.Serializable;

public class PuntiSomministrazioneTipologia implements Serializable {

    private String area;                        //regione
    private String denominazioneStruttura;     //presidio di somministrazione
    private String tipologia;                   //Tipologia del presidio di somministrazione: ospedaliero o territoriale
    private String codiceNUTS1;                //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private String codiceNUTS2;             //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private Integer codiceRegioneISTAT;       //Codice ISTAT della Regione
    private String nomeRegione;                //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public PuntiSomministrazioneTipologia(String area, String denominazioneStruttura, String tipologia, String codiceNUTS1, String codiceNuts2, Integer codiceRegioneISTAT, String nomeRegione) {
        this.area = area;
        this.denominazioneStruttura = denominazioneStruttura;
        this.tipologia = tipologia;
        this.codiceNUTS1 = codiceNUTS1;
        this.codiceNUTS2 = codiceNuts2;
        this.codiceRegioneISTAT = codiceRegioneISTAT;
        this.nomeRegione = nomeRegione;
    }

    public String getArea() {
        return area;
    }

    public String getDenominazioneStruttura() {
        return denominazioneStruttura;
    }

    public String getTipologia() {
        return tipologia;
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

    public String getNomeRegione() {
        return nomeRegione;
    }

    @Override
    public String toString() {
        return "PuntiSomministrazioneTipologia{" +
                ", area='" + area + '\'' +
                ", denominazione_struttura='" + denominazioneStruttura + '\'' +
                ", tipologia='" + tipologia + '\'' +
                ", codice_NUTS1='" + codiceNUTS1 + '\'' +
                ", getCodice_NUTS2='" + codiceNUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codiceRegioneISTAT +
                ", nome_regione='" + nomeRegione + '\'' +
                '}';
    }
}
