package domain;

public enum OrderPriority {
    L("Low"),
    M("Medium"),
    H("High"),
    C("Critical"),
    ;

    private final String description;

    private OrderPriority(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
