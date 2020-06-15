package it.uniroma2.dicii.sabd.dspproject.utils;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BreakdownParser {

    private static final String[] DELAY_PATTERNS = {"(\\d+)[-/](\\d+)\\s*([mMHh])*.*", "(\\d+)\\s*([mMhH]).*", "\\d+"};
    private static final String MINUTES_GRANULARITY = "m";
    private static final String HOURS_GRANULARITY = "h";
    private static final Double MINUTES_HOURS_THRESHOLD = 5.0;
    public static final String EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.sss";
    public static final int COUNTY_FIELD = 10;
    public static final int DELAY_FIELD = 12;

    public static class BreakdownParserException extends Exception {

    }

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

    /* Returns the fields of a CSV string as an array of String */
    public static String[] getFieldsFromCsvString(String s) throws BreakdownParserException {
        try {
            CSVParserBuilder csvParserBuilder = new CSVParserBuilder().withSeparator(';');
            CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new StringReader(s)).withCSVParser(csvParserBuilder.build());
            CSVReader csvReader = csvReaderBuilder.build();
            String[] fields = csvReader.readNext();
            csvReader.close();
            return fields;
        } catch (CsvValidationException | IOException e) {
            throw new BreakdownParserException();
        }
    }

}