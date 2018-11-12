package logic;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

public class OrderManager {

    private final AmazonDynamoDB ddb;

    public OrderManager() {
        this.ddb = AmazonDynamoDBClientBuilder.standard().build();
    }

    public boolean isAlreadyProcessed(long id) {
        GetItemRequest getItemRequest = new GetItemRequest()
                .withKey(generateOrderIdItem(id))
                .withTableName("ProcessedOrder");
        Map<String, AttributeValue> idItem = ddb.getItem(getItemRequest).getItem();
        return idItem != null;
    }

    public void markProcessed(long id) {
        ddb.putItem("ProcessedOrder", generateOrderIdItem(id));
    }

    private HashMap<String, AttributeValue> generateOrderIdItem(long id) {
        HashMap<String, AttributeValue> values = Maps.newHashMap();
        values.put("OrderId", new AttributeValue().withN(id + ""));
        return values;
    }

}
