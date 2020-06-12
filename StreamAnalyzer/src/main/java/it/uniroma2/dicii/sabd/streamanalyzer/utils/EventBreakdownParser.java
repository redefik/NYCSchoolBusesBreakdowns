package it.uniroma2.dicii.sabd.streamanalyzer.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventBreakdownParser {

    private static final String[] DELAY_PATTERNS = {"(\\d+)[-/](\\d+)\\s*([mMHh])*.*", "(\\d+)\\s*([mMhH]).*", "\\d+"};
    private static final String MINUTES_GRANULARITY = "m";
    private static final String HOURS_GRANULARITY = "h";
    private static final Double MINUTES_HOURS_THRESHOLD = 5.0;

    // todo return delay in minutes
    public static Double parseDelay(String delayString) {
        for (int i = 0; i < DELAY_PATTERNS.length; i++) {
            Pattern pattern = Pattern.compile(DELAY_PATTERNS[i]);
            Matcher matcher = pattern.matcher(delayString);
            double value;
            if (matcher.find()) {
                switch(i) {
                    case 0:
                        Double lower = Double.parseDouble(matcher.group(1));
                        Double upper = Double.parseDouble(matcher.group(2));
                        double avg = (lower + upper) / 2;
                        if (matcher.group(3) != null) {
                            String granularity = matcher.group(3).toLowerCase();
                            if (granularity.equals(MINUTES_GRANULARITY)) {
                                return avg;
                            } else if (granularity.equals(HOURS_GRANULARITY)){
                                return avg * 60;
                            }
                        } else {
                            if (avg <= MINUTES_HOURS_THRESHOLD) {
                                return avg * 60;
                            } else {
                                return avg;
                            }
                        }
                    case 1:
                        value = Double.parseDouble(matcher.group(1));
                        String granularity = matcher.group(2).toLowerCase();
                        if (granularity.equals(MINUTES_GRANULARITY)) {
                            return value;
                        } else if (granularity.equals(HOURS_GRANULARITY)) {
                            return value * 60;
                        }
                    case 2:
                        value = Double.parseDouble(matcher.group());
                        if (value <= MINUTES_HOURS_THRESHOLD) {
                            return value * 60;
                        } else {
                            return value;
                        }
                }
            }
        }

        return null;
    }

}
