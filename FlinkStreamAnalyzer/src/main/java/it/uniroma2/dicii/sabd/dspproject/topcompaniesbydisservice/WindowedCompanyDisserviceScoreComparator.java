package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import java.util.Comparator;

public class WindowedCompanyDisserviceScoreComparator implements Comparator<WindowedCompanyDisserviceScore> {
    @Override
    public int compare(WindowedCompanyDisserviceScore o1, WindowedCompanyDisserviceScore o2) {
        return o1.getDisserviceScore().compareTo(o2.getDisserviceScore());
    }
}
