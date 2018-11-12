package service;

public class TransferResponse {

    private String error;

    // Return value
    private String value;

    public String getError()
    {
        return error;
    }

    public void setError(String err)
    {
        this.error = err;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }
}
