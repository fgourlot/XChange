package info.bitrich.xchangestream.bittrex.dto;

public class BittrexOrderToCancel {
    private String type;
    private String id;

    public BittrexOrderToCancel() {
    }

    public BittrexOrderToCancel(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }
}
