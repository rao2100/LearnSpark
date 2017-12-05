package com.rao2100.basicspark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WordUtils {

    public static void writeToCsv(String path, List<String> data) throws IOException {
        FileWriter writer = new FileWriter(path);

        String collect = data.stream().collect(Collectors.joining(","));
        System.out.println(collect);

        writer.write(collect);
        writer.close();
    }

}
