package logic;

import domain.CalculatedLineEntry;
import domain.RawLineEntry;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class RawLineEntriesTransformer implements Callable<Integer> {

    private final List<RawLineEntry> rawLineEntries;

    private final Writer calculatedFileWriter;

    private RawLineEntriesTransformer(Builder builder) {
        this.rawLineEntries = builder.rawLineEntries;
        this.calculatedFileWriter = builder.calculatedFileWriter;
    }

    public static class Builder {
        private List<RawLineEntry> rawLineEntries;

        private Writer calculatedFileWriter;

        public void setRawLineEntries(List<RawLineEntry> rawLineEntries) {
            this.rawLineEntries = rawLineEntries;
        }

        public void setCalculatedFileWriter(Writer calculatedFileWriter) {
            this.calculatedFileWriter = calculatedFileWriter;
        }

        public RawLineEntriesTransformer build() {
            return new RawLineEntriesTransformer(this);
        }
    }


    @Override
    public Integer call() throws Exception {
        List<CalculatedLineEntry> calculatedLineEntries =
                rawLineEntries.stream().map(this::transform).collect(Collectors.toList());
        calculatedFileWriter.write("");
        return null;
    }



    private CalculatedLineEntry transform(RawLineEntry rawLineEntry) {
        return null;
    }

}
