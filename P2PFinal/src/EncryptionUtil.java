import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class EncryptionUtil {

	static Cipher cipher;

	
	/*public static String decrypt(String encryptedText, SecretKey secretKey)
			throws Exception {
		Base64.Decoder decoder = Base64.getDecoder();
		byte[] encryptedTextByte = decoder.decode(encryptedText);
		cipher.init(Cipher.DECRYPT_MODE, secretKey);
		byte[] decryptedByte = cipher.doFinal(encryptedTextByte);
		String decryptedText = new String(decryptedByte);
		return decryptedText;
	}*/
	public static String decrypt(String valueToDecrypt,  String password) throws Exception {		

		int len = valueToDecrypt.length();     
		byte[] data = new byte[len / 2];    
		for (int i = 0; i < len; i += 2) {        
			data[i / 2] = (byte) ((Character.digit(valueToDecrypt.charAt(i), 16) << 4) 
					+ Character.digit(valueToDecrypt.charAt(i+1), 16));    
		} 

		byte[] keyBytes = password.getBytes("UTF-8");
		byte[] ivBytes = keyBytes;
		IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);
		SecretKeySpec spec = new SecretKeySpec(keyBytes, "AES");
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");		
		cipher.init(Cipher.DECRYPT_MODE, spec, ivSpec);
		data = cipher.doFinal(data);
		return(new String(data));

	}
	

		
}
