package info.bitrich.xchangestream.bittrex.services.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

/** Utility class with tools for websocket message conversion. */
public final class BittrexStreamingEncryptionUtils {

  private BittrexStreamingEncryptionUtils() {
    // Utility class
  }

  public static String calculateHash(String secret, String data, String algorithm)
      throws InvalidKeyException, NoSuchAlgorithmException {
    Mac shaHmac = Mac.getInstance(algorithm);
    SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(), algorithm);
    shaHmac.init(secretKey);
    byte[] hash = shaHmac.doFinal(data.getBytes());
    return DatatypeConverter.printHexBinary(hash);
  }

  /**
   * Creates a nonce.
   *
   * @return the created nonce
   * @throws NoSuchAlgorithmException in case the algorithm is not found
   */
  public static String generateNonce() throws NoSuchAlgorithmException {
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    random.setSeed(System.currentTimeMillis());
    byte[] nonceBytes = new byte[16];
    random.nextBytes(nonceBytes);
    return new String(Base64.getEncoder().encode(nonceBytes), StandardCharsets.UTF_8);
  }

  /**
   * Decode and decompress a Base64 String.
   *
   * @param encodedMessage the message to decode and decompress
   * @return the decompressed and decoded message
   */
  public static byte[] decompress(String encodedMessage) throws IOException {
    byte[] decodedData = Base64.getDecoder().decode(encodedMessage);
    return deflate(decodedData);
  }

  /**
   * Decompress Deflate data.
   *
   * @param decodedData the decoded data to decompress
   * @return the decompressed data
   * @throws IOException in case the data could not be decompressed
   */
  public static byte[] deflate(byte[] decodedData) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (InflaterOutputStream zos = new InflaterOutputStream(bos, new Inflater(true))) {
      zos.write(decodedData);
      return bos.toByteArray();
    }
  }
}
