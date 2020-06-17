package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

/*
* This class represents the disservice score earned by a school bus company during a specific time window
* */
public class WindowedCompanyDisserviceScore {

    private String company;
    private Double disserviceScore;

    public WindowedCompanyDisserviceScore() { }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public Double getDisserviceScore() {
        return disserviceScore;
    }

    public void setDisserviceScore(Double disserviceScore) {
        this.disserviceScore = disserviceScore;
    }
}
