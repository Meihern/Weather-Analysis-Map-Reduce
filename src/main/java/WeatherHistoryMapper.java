import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class WeatherHistoryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static boolean isHeaderRow(String[] row){
        for (String cell:row) {
            String new_cell = cell.replace("\"", "");
            switch(new_cell){
                case "STATION":
                case "DATE":
                case "SOURCE":
                case "LATITUDE":
                case "LONGITUDE":
                case "ELEVATION":
                case "NAME":
                case "REPORT_TYPE":
                case "CALL_SIGN":
                case "QUALITY_CONTROL":
                case "WND":
                case "CIG":
                case "VIS":
                case "TMP":
                case "DEW":
                case "SLP":
                case "GF1":
                case "MW1":
                    return true;

            }
        }
        return false;
    }


    public static boolean isValidTempData(int qualityControl){
        if (qualityControl != 1){
            return false;
        }
        else{
            return true;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        String[] valueData = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        if(isHeaderRow(valueData)){
            return;
        }
        else{
            String[] temperatureData = valueData[13].replace("\"", "").split(",");
            int temperatureQualityControl = Integer.parseInt(temperatureData[1]);
            if (isValidTempData(temperatureQualityControl)){
                String dateTime = valueData[1].replace("\"", "");
                String date = dateTime.substring(0, dateTime.lastIndexOf('-'));
                context.write(new Text(date), new DoubleWritable(Double.parseDouble(temperatureData[0])/10));
            }
        }

    }
}
