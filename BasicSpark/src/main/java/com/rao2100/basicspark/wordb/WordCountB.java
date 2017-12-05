package com.rao2100.basicspark.wordb;

import com.rao2100.basicspark.*;
import com.rao2100.basicspark.worda.WordCountA;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.col;

public class WordCountB implements WordCount {

    private transient SQLContext sqlContext;

    @Autowired
    public void setSQLContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public List<Count> count() {
        String input = "rao,hello,bird,lion,monkey,sheep,rao";
        String[] _words = input.split(",");
        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sqlContext.createDataFrame(words, Word.class);
        dataFrame.show();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();
        List<Count> countList = rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());

        List<String> data = new ArrayList<>();

        for (Count count : countList) {
            data.add(count.toString());
        }
        try {
            WordUtils.writeToCsv("/tmp/WordB.csv", data);
        } catch (IOException ex) {
            Logger.getLogger(WordCountA.class.getName()).log(Level.SEVERE, null, ex);
            Logger.getLogger(ex.getMessage());
        }

        return countList;

    }
}
