package info.bitrich.xchangestream.bittrex;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

class EncryptionUtils {

  public static String calculateHash(String secret, String data, String algorithm)
      throws InvalidKeyException, NoSuchAlgorithmException {
    Mac shaHmac = Mac.getInstance(algorithm);
    SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(), algorithm);
    shaHmac.init(secretKey);
    byte[] hash = shaHmac.doFinal(data.getBytes());
    return DatatypeConverter.printHexBinary(hash);
  }

  public static String generateNonce()
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    random.setSeed(System.currentTimeMillis());
    byte[] nonceBytes = new byte[16];
    random.nextBytes(nonceBytes);
    String nonce = new String(Base64.getEncoder().encode(nonceBytes), "UTF-8");
    return nonce;
  }

  /**
   * Decode and decompress a Base64 String
   *
   * @param encodedMessage
   * @return
   */
  public static String decompress(String encodedMessage) throws IOException {
    byte[] decodedData = Base64.getDecoder().decode(encodedMessage);
    return new String(deflate(decodedData));
  }

  /**
   * Decompress Deflate data
   *
   * @param decodedData
   * @return
   * @throws Exception
   */
  public static byte[] deflate(byte[] decodedData) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Inflater decompressor = new Inflater(true);
    InflaterOutputStream zos = new InflaterOutputStream(bos, decompressor);
    zos.write(decodedData);
    zos.close();
    return bos.toByteArray();
  }
}
