import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;
import java.util.*;

public class WeatherHistoryFileManagement {
    final static String BASE_WEATHER_URL = "https://www.ncei.noaa.gov/data/global-hourly/archive/csv/";
    final static String FILE_EXTENSION = ".tar.gz";
    final static String INPUT_DIR = "Input";


    public static List<File> extractFile(File inputFile) throws IOException {
        InputStream inputStream = new FileInputStream(inputFile);
        final int BUFFER_SIZE = 1024;
        List<File> extractedFiles = new ArrayList<>();
        GzipCompressorInputStream gzipCompressorInputStream = new GzipCompressorInputStream(inputStream);
        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipCompressorInputStream)){
            TarArchiveEntry tarArchiveEntry;
            while ((tarArchiveEntry = (TarArchiveEntry) tarArchiveInputStream.getNextEntry()) != null){
                if(tarArchiveEntry.isDirectory()){
                    File file = new File(tarArchiveEntry.getName());
                    boolean created = file.mkdir();
                    if(!created){
                        System.out.println("Unable to create directroy "+file.getAbsolutePath());
                    }
                }
                else{
                    int count;
                    byte[] data = new byte[BUFFER_SIZE];
                    FileOutputStream fileOutputStream = new FileOutputStream(INPUT_DIR+'/'+tarArchiveEntry.getName(), false);
                    try (BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream, BUFFER_SIZE)) {
                        extractedFiles.add(new File(INPUT_DIR+'/'+tarArchiveEntry.getName()));
                        while ((count = tarArchiveInputStream.read(data, 0, BUFFER_SIZE)) != -1){
                            outputStream.write(data, 0, count);
                        }
                    }
                }
            }
            System.out.println("Extraction completed successfully !");
        }
        return extractedFiles;
    }

    public static void getFilesFromYearBounds(int start_year, int end_year) throws IOException {
        if (start_year <= 1900 || end_year > 2020) {
            System.out.println("Invalid Input, choose a valid year");
        }
        else {
            List<File> yearDataFiles = new ArrayList<>();
            for (int i=start_year; i<=end_year; i++){
                String file_name = i+FILE_EXTENSION;
                File targetFile = new File(INPUT_DIR+'/'+file_name);
                FileUtils.copyURLToFile(new URL(BASE_WEATHER_URL+"/"+file_name), targetFile);
                List<File> extractedFiles = extractFile(targetFile);
                targetFile.delete();
                yearDataFiles.add(mergePartFiles(extractedFiles, i));
            }
            mergeYearFiles(yearDataFiles);
        }

    }

    public static File mergePartFiles(List<File> files_to_merge, int year) throws IOException {
        File firstFile = files_to_merge.get(0);
        files_to_merge.remove(firstFile);
        File newFileName = null;
        mergeFiles(files_to_merge, firstFile);
        try{
            newFileName = new File(firstFile.getParent() + '/' + year + ".csv");
            firstFile.renameTo(newFileName);
            return newFileName;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return newFileName;
    }

    public static void mergeYearFiles(List<File> files_to_merge) throws IOException {
        File firstFile = files_to_merge.get(0);
        files_to_merge.remove(firstFile);

        mergeFiles(files_to_merge, firstFile);
        try{
            File newFileName = new File(firstFile.getParent() + "/weather_data.csv");
            firstFile.renameTo(newFileName);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void mergeFiles(List<File> files_to_merge, File firstFile) throws IOException {
        Iterator<File> fileIterator = files_to_merge.iterator();
        BufferedWriter writer = new BufferedWriter(new FileWriter(firstFile, true));

        while (fileIterator.hasNext()){
            File nextFile = fileIterator.next();
            BufferedReader reader = new BufferedReader(new FileReader(nextFile));

            String line;
            String[] firstLine = null;
            if ((line = reader.readLine()) != null)
                firstLine = line.split(",");
            while ((line = reader.readLine()) != null){
                writer.write(line);
                writer.newLine();
            }
            reader.close();
            try{
                nextFile.delete();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        writer.close();
    }


}
