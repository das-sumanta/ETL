import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Hex;

public class EncryptionUtil {

	public static void main(String[] args) throws Exception {
		//String passKey = "EncryptingKeySto";
		String passKey = "ScholasticsETL12";
		String builder = "SecureThis";
		
		String encryptedString = encryptAES(builder, passKey);
		System.out.println("encryptedString="+encryptedString);
		
		String decryptedString = decryptAES(encryptedString, passKey);
		System.out.println("decryptedString="+decryptedString);
	}

	public static String encryptAES(String plainText, String password) throws Exception {
		byte[] preSharedKey = password.getBytes();  
		byte[] data = plainText.getBytes("UTF-8");
		SecretKey aesKey = new SecretKeySpec(preSharedKey, "AES");  			
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(preSharedKey)); 
		byte[] output = cipher.doFinal(data);  		
		String encryptedString = new String(Hex.encodeHex(output));
		return encryptedString.toUpperCase();
	}
	
	public static String decryptAES(String valueToDecrypt,  String password) throws Exception {		

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
