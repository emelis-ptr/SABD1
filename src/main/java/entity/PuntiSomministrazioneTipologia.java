package entity;

public class PuntiSomministrazioneTipologia {

    private String id;
    private String area;                        //regione
    private String denominazione_struttura;     //presidio di somministrazione
    private String tipologia;                   //Tipologia del presidio di somministrazione: ospedaliero o territoriale
    private String codice_NUTS1;                //Classificazione europea delle unità territoriali NUTS: livello NUTS 1
    private String getCodice_NUTS2;             //Classificazione europea delle unità territoriali NUTS: livello NUTS 2
    private Integer codice_regione_ISTAT;       //Codice ISTAT della Regione
    private String nome_regione;                //Denominazione standard dell’area (dove necessario denominazione bilingue)

    public PuntiSomministrazioneTipologia(String id, String area, String denominazione_struttura, String tipologia, String codice_NUTS1, String getCodice_NUTS2, Integer codice_regione_ISTAT, String nome_regione) {
        this.id = id;
        this.area = area;
        this.denominazione_struttura = denominazione_struttura;
        this.tipologia = tipologia;
        this.codice_NUTS1 = codice_NUTS1;
        this.getCodice_NUTS2 = getCodice_NUTS2;
        this.codice_regione_ISTAT = codice_regione_ISTAT;
        this.nome_regione = nome_regione;
    }

    public String getId() {
        return id;
    }

    public String getArea() {
        return area;
    }

    public String getDenominazione_struttura() {
        return denominazione_struttura;
    }

    public String getTipologia() {
        return tipologia;
    }

    public String getCodice_NUTS1() {
        return codice_NUTS1;
    }

    public String getGetCodice_NUTS2() {
        return getCodice_NUTS2;
    }

    public Integer getCodice_regione_ISTAT() {
        return codice_regione_ISTAT;
    }

    public String getNome_regione() {
        return nome_regione;
    }

    @Override
    public String toString() {
        return "PuntiSomministrazioneTipologia{" +
                "id='" + id + '\'' +
                ", area='" + area + '\'' +
                ", denominazione_struttura='" + denominazione_struttura + '\'' +
                ", tipologia='" + tipologia + '\'' +
                ", codice_NUTS1='" + codice_NUTS1 + '\'' +
                ", getCodice_NUTS2='" + getCodice_NUTS2 + '\'' +
                ", codice_regione_ISTAT=" + codice_regione_ISTAT +
                ", nome_regione='" + nome_regione + '\'' +
                '}';
    }
}
