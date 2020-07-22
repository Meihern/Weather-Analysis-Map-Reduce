import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WeatherHistoryReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private static Double getMaxTemperature(List<Double> temperatureList){
        double max_temperature = temperatureList.get(0);
        for(int i=1; i<temperatureList.size(); i++){
            if (temperatureList.get(i) >= max_temperature){
                max_temperature = temperatureList.get(i);
            }
        }
        return max_temperature;
    }

    private static Double getMinTemperature(List<Double> temperatureList){
        double min_temperature = temperatureList.get(0);
        for(int i=1; i<temperatureList.size(); i++){
            if (temperatureList.get(i) <= min_temperature){
                min_temperature = temperatureList.get(i);
            }
        }
        return min_temperature;
    }

    private static Double getAvgTemperature(List<Double> temperatureList){
        double sum_temperature = 0;
        for(Double temperature:temperatureList){
            sum_temperature += temperature;
        }
        return sum_temperature/temperatureList.size();
    }

    private List<Double> convertDoubleWritableIteratorToList(Iterator<DoubleWritable> doubleWritableIterator){
        List<Double> doubleList = new ArrayList<>();
        while(doubleWritableIterator.hasNext()){
            doubleList.add(doubleWritableIterator.next().get());
        }
        return doubleList;
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        List<Double> temperatureList = convertDoubleWritableIteratorToList(values.iterator());
        Double max_temperature = getMaxTemperature(temperatureList);
        Double min_temperature = getMinTemperature(temperatureList);
        Double avg_temperature = getAvgTemperature(temperatureList);
        context.write(key, new Text("Min_Temp = "+String.format("%.2f",min_temperature)+", Max_Temp = "+String.format("%.2f", max_temperature)+ ", Avg_Temp = "+String.format("%.2f",avg_temperature)));
    }
}
