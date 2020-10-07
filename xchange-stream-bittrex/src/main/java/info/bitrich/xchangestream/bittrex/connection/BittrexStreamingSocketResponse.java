package info.bitrich.xchangestream.bittrex.connection;

public class BittrexStreamingSocketResponse {

  private final Boolean Success;
  private final String ErrorCode;

  public BittrexStreamingSocketResponse(Boolean success, String error) {
    Success = success;
    ErrorCode = error;
  }

  @Override
  public String toString() {
    return "SocketResponse{" + "Success=" + Success + ", ErrorCode='" + ErrorCode + '\'' + '}';
  }
}
