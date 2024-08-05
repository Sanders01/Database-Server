package edu.uob.Parsing;

public class NameValuePair {

    private final String attributeName;

    public String getAttributeName() {
        return attributeName;
    }

    private final String value;

    public String getValue() {
        return value;
    }


    public NameValuePair(String attributeName, String value) {
        this.attributeName = attributeName;
        this.value = value;
    }


}
