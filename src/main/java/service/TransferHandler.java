package service;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import domain.OrderPriority;
import logic.OrderManager;
import logic.S3Operator;

import java.io.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.Period;
import java.util.Arrays;

public class TransferHandler implements RequestHandler<TransferRequest, TransferResponse> {


    public TransferResponse handleRequest(TransferRequest transferRequest, Context context) {
        S3Operator s3Operator = new S3Operator();

        TransferResponse response = new TransferResponse();

        String bucketName = transferRequest.getBucketName();
        String objectKey = transferRequest.getObjectKey();
        InputStream fileStream = s3Operator.readFile(bucketName, objectKey);
        BufferedReader csvReader = new BufferedReader(new InputStreamReader(fileStream));
        try {
            File calculatedCsvFIle = File.createTempFile(bucketName, objectKey);
            FileWriter fw = new FileWriter(calculatedCsvFIle);
            BufferedWriter bw = new BufferedWriter(fw);
            processTitle(csvReader.readLine(), bw);
            String entry;
            while((entry = csvReader.readLine()) != null) {
                processDataLine(entry, bw);
            }
            bw.flush();
            s3Operator.uploadFile(bucketName, newObjectKey(objectKey), calculatedCsvFIle);
            calculatedCsvFIle.delete();
        } catch (IOException ioe) {
            response.setError(ioe.toString());
            throw new RuntimeException("Cannot read file");
        }
        response.setValue("1");
        return response;
    }

    private String newObjectKey(String objectKey) {
        int separator = objectKey.lastIndexOf(".");
        return objectKey.substring(0, separator)
                + "_calculated."
                + objectKey.substring(separator);
    }

    private void processTitle(String title, BufferedWriter bw) throws IOException {
        // Add two new column
        bw.write(title + ",Order Processing Time,Gross Margin");
        bw.newLine();
    }

    private void processDataLine(String entry, BufferedWriter bw) throws IOException {
        OrderManager orderManager = new OrderManager();
        String[] data = entry.split(",");
        long orderId = Long.parseLong(data[6]);
        if(orderManager.isAlreadyProcessed(orderId)) {
            return;
        }
        setOderPriorityDiscription(data);
        StringBuilder sb = new StringBuilder(Arrays.toString(data));
        sb.append(",").append(getProcessingDays(data));
        sb.append(",").append(margin(data));
        String calculatedEntry = sb.toString();
        bw.write(calculatedEntry);
        bw.newLine();
        orderManager.markProcessed(orderId);
    }

    private void setOderPriorityDiscription(String[] data) {
        data[4] = getOrderPriorityDescription(data[4]);
    }

    private String getOrderPriorityDescription(String abs) {
        return OrderPriority.valueOf(abs).getDescription();
    }

    private int getProcessingDays(String[] data) {
        LocalDate oderDate = parseLocalDate(data[5]);
        LocalDate shipDate = parseLocalDate(data[7]);
        return Period.between(oderDate, shipDate).getDays();
    }

    private LocalDate parseLocalDate(String localDate) {
        String[] dmy = localDate.split("/");
        return LocalDate.of(Integer.parseInt(dmy[2]),
                Integer.parseInt(dmy[0]), Integer.parseInt(dmy[1]));
    }

    private String margin(String[] data) {
        DecimalFormat df = new DecimalFormat("#.00");
        double revenue = Double.parseDouble(data[11]);
        double profit = Double.parseDouble(data[13]);
        return df.format(profit / revenue);
    }


}
