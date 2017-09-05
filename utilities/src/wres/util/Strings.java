package wres.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Strings {

	private final static int TRUNCATE_SIZE = 1000;

    private Strings(){}
	
	/**
	 * Static list of string values that might map to the boolean value 'true'
	 */
	public static final List<String> POSSIBLE_TRUE_VALUES = Arrays.asList("true", "True", "TRUE", "T", "t", "y", "yes", "Yes", "YES", "Y", "1");
	
	public static boolean isTrue(String possibleBoolean)
	{
	    return POSSIBLE_TRUE_VALUES.contains(possibleBoolean);
	}

	public static boolean hasValue(String word)
	{
		return word != null && !word.trim().isEmpty();
	}

	/**
	 * Extracts the first grouping of characters in the source string that matches the pattern
	 * @param source The string to extract the word from
	 * @param pattern The pattern to match
	 * @return The first substring to match the pattern
	 */
	public static String extractWord(String source, String pattern) {
		String matched_string = null;
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find()) {
			matched_string = match.group();
		}
		return matched_string;
	}

	public static String truncate(String message)
	{
		return truncate(message, TRUNCATE_SIZE);
	}

	public static String truncate(String message, int length)
	{
		String truncatedMessage = message;
		if (message.length() > length - 3 && length > 3)
		{
			truncatedMessage = message.substring(0, length - 3) + "...";
		}
		return truncatedMessage;
	}
	
	/**
	 * Finds every substring that in the source that matches the pattern
	 * @param source The string to extract the words from
	 * @param pattern The pattern to match
	 * @return A string array containing all matched substrings
	 */
	public static String[] extractWords(String source, String pattern) {
		String[] matches = null;
		
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find()) {
			matches = new String[match.groupCount() + 1];
			for (int match_index = 0; match_index <= match.groupCount(); ++match_index) {
				matches[match_index] = match.group(match_index);
			}
		}
				
		return matches;
	}

	public static boolean contains(String full, String pattern)
	{
		Pattern regex = Pattern.compile(pattern);
		return regex.matcher(full).find();
	}

	public static String removePattern(String string, String pattern)
    {
        return string.replaceAll(pattern, "");
    }
	
	/**
	 * Determines if a string describes some number
	 * @param possibleNumber A string that might be a number
	 * @return True if the possibleNumber really is a number
	 */
	public static boolean isNumeric(String possibleNumber) {
		return hasValue(possibleNumber) && possibleNumber.trim().matches("^[-]?\\d*\\.?\\d+$");
	}
	
	public static String getStackTrace(Exception error)
	{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        error.printStackTrace(ps);
        ps.close();
        return baos.toString();
	}

	public static boolean isOneOf(String possible, String... options)
	{
		boolean isOne = false;

		for (String option : options)
		{
			if (possible.equalsIgnoreCase(option))
			{
				isOne = true;
				break;
			}
		}

		return isOne;
	}

    public static String getAbsolutePath(String filename)
    {
        return Paths.get(filename).toAbsolutePath().toString();
    }

    public static String getFileName(String path)
    {
        return Paths.get(path).getFileName().toString();
    }

    public static String getMD5Checksum(String filename) throws IOException
    {
        byte[] buffer = new byte[1024];
        MessageDigest complete = getMD5Algorithm();
        int numRead;

        try ( InputStream fis = new FileInputStream( filename ) )
        {
            do
            {
                numRead = fis.read( buffer );
                if ( numRead > 0 )
                {
                    complete.update( buffer, 0, numRead );
                }
            } while ( numRead != -1 );
        }

        return getMD5Checksum( complete.digest() );
	}

	public static String getMD5Checksum(byte[] checksum)
    {
		if (checksum == null)
		{
			return null;
		}

		if (checksum.length > 16)
        {
            MessageDigest complete = getMD5Algorithm();
            complete.update( checksum );
            checksum = complete.digest();
        }

		/*final byte[] hexTable = {
				(byte)'0', (byte)'1', (byte)'2', (byte)'3',
				(byte)'4', (byte)'5', (byte)'6', (byte)'7',
				(byte)'8', (byte)'9', (byte)'a', (byte)'b',
				(byte)'c', (byte)'d', (byte)'e', (byte)'f'
		};*/

		final String hexes = "0123456789ABCDEF";

		final StringBuilder hex = new StringBuilder( 2 * checksum.length );
		//int index = 0;
		//byte[] hex = new byte[2 * checksum.length];

		for (byte b : checksum)
		{
			hex.append(hexes.charAt((b & 0xF0) >> 4))
			   .append(hexes.charAt( b & 0x0F ));
			//int v = b & 0xFF;
			//hex[index++] = hexTable[v >>> 4];
			//hex[index++] = hexTable[v & 0xF];
		}

		return hex.toString(); //new String(hex, "ASCII");
	}

	private static MessageDigest getMD5Algorithm()
    {
        MessageDigest algorithm;

        try
        {
            algorithm = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException e )
        {
            e.printStackTrace();
            throw new RuntimeException(
                    "Something went wrong when trying to generate the MD5 algorithm",
                    e );
        }

        return algorithm;
    }
}
