package info.bitrich.xchangestream.bittrex.connection;

public class BittrexStreamingSocketResponse {

  private final Boolean Success;
  private final String ErrorCode;

  public BittrexStreamingSocketResponse(Boolean success, String error) {
    Success = success;
    ErrorCode = error;
  }

  public Boolean getSuccess() {
    return Success;
  }

  public String getErrorCode() {
    return ErrorCode;
  }

  @Override
  public String toString() {
    return "SocketResponse{" + "Success=" + Success + ", ErrorCode='" + ErrorCode + '\'' + '}';
  }
}
