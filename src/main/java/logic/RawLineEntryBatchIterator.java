//package logic;
//
//import com.google.common.collect.Lists;
//import domain.RawLineEntry;
//import org.apache.commons.csv.CSVRecord;
//
//import java.io.BufferedReader;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.util.Iterator;
//import java.util.List;
//
//public class RawLineEntryBatchIterator implements Iterator<List<RawLineEntry>> {
//
//    private static final int BATCH_SIZE = 100;
//
//    private Iterator<CSVRecord> csvRecordIterator;
//
//    public static RawLineEntryBatchIterator from(InputStream inputStream) {
//        BufferedReader csvReader = new BufferedReader(new InputStreamReader(inputStream));
//        return null;
//    }
//
//
//    @Override
//    public boolean hasNext() {
//        return csvRecordIterator.hasNext();
//    }
//
//    @Override
//    public List<RawLineEntry> next() {
//        List<RawLineEntry> nextBatch = Lists.newArrayList();
//        int counter = 0;
//        while(csvRecordIterator.hasNext() && counter++ < BATCH_SIZE) {
//            CSVRecord record = csvRecordIterator.next();
//            nextBatch.add(convert(record));
//        }
//        return nextBatch;
//    }
//
//
//    private RawLineEntry convert(CSVRecord csvRecord) {
//        return null;
//    }
//
//    @Override
//    public void remove() {
//        throw new UnsupportedOperationException("Do not support remove function in RawLineEntryBatchIterator");
//    }
//}
