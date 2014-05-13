package test.model;

import java.io.Serializable;
import java.util.Date;

public class Property implements Serializable {
    public Object actualValue;
    public Date actualReceived;
    public Date actualChanged;

    public Object requestedValue;
    public String requestId;
    public Date requestTime;
    public Date requestExpiry;
}
