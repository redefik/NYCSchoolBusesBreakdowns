package it.uniroma2.dicii.sabd.dspproject.topcompaniesbydisservice;

import it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.List;

import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.*;

/*
 * Parses a breakdown event extracting the delay caused by the breakdown, the reason of the delay and the company
 * of the broken school bus
 * */
public class DelayReasonCompanyExtractor implements FlatMapFunction<String, Tuple3<Double, String, String>> {

    private List<Tuple2<String, String>> schoolBusCompaniesPatterns;

    public DelayReasonCompanyExtractor(List<Tuple2<String, String>> schoolBusCompaniesPatterns) {
        this.schoolBusCompaniesPatterns = schoolBusCompaniesPatterns;
    }

    @Override
    public void flatMap(String breakdownEvent, Collector<Tuple3<Double, String, String>> collector) {
        try {
            String[] breakdownEventFields = BreakdownParser.getFieldsFromCsvString(breakdownEvent);
            /* Parsing delay */
            String delayString = breakdownEventFields[DELAY_FIELD];
            Double delay = BreakdownParser.parseDelay(delayString);
            if (delay == null) {
                return;
            }
            /* Parsing breakdown reason */
            String reason = breakdownEventFields[REASON_FIELD];
            if (reason.equals("")) {
                return;
            }
            /* Parsing school bus company */
            String companyRaw = breakdownEventFields[COMPANY_FIELD];
            String company = BreakdownParser.parseCompany(companyRaw, schoolBusCompaniesPatterns);
            if (company == null) {
                return;
            }
            collector.collect(new Tuple3<>(delay, reason, company));

        } catch (BreakdownParser.BreakdownParserException e) {
            e.printStackTrace();
        }
    }
}
