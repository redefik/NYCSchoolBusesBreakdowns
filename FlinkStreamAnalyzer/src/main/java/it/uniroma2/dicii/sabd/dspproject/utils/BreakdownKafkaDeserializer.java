package it.uniroma2.dicii.sabd.dspproject.utils;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import static it.uniroma2.dicii.sabd.dspproject.utils.BreakdownParser.EVENT_TIME_FORMAT;

public class BreakdownKafkaDeserializer extends AbstractDeserializationSchema<String> {

    private static final int EVENT_TIMESTAMP_FIELD = 7;

    @Override
    public String deserialize(byte[] bytes)  {
        try {
            String breakdownEventString = new String(bytes, StandardCharsets.UTF_8);
            String[] breakdownEventFields = BreakdownParser.getFieldsFromCsvString(breakdownEventString);
            String eventTimestampString = breakdownEventFields[EVENT_TIMESTAMP_FIELD];
            /* Parse event timestamp for future watermark generation */
            SimpleDateFormat sdf = new SimpleDateFormat(EVENT_TIME_FORMAT, Locale.US);
            Long eventTimestamp = sdf.parse(eventTimestampString).getTime();
            return eventTimestamp + ";" + breakdownEventString;

        } catch (BreakdownParser.BreakdownParserException | ParseException e) {
            e.printStackTrace();
            return null; /* Skip corrupted events */
        }
    }
}
