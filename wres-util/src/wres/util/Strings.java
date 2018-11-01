package wres.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Strings
{
	private static final Logger LOGGER = LoggerFactory.getLogger( Strings.class );
	private static final Pattern RTRIM = Pattern.compile("\\s+$");

	private static final int LINE_LENGTH = 120;
	private static final int TRUNCATE_SIZE = 2000;

    private Strings(){}
	
	/**
	 * Static list of string values that might map to the boolean value 'true'
	 */
	private static final List<String> POSSIBLE_TRUE_VALUES =
			Arrays.asList("true",
						  "True",
						  "TRUE",
						  "T",
						  "t",
						  "y",
						  "yes",
						  "Yes",
						  "YES",
						  "Y",
						  "1");
	
	public static boolean isTrue(String possibleBoolean)
	{
	    return Strings.hasValue( possibleBoolean ) && POSSIBLE_TRUE_VALUES.contains(possibleBoolean);
	}

	public static boolean hasValue(String word)
	{
		return word != null && !word.trim().isEmpty();
	}

	public static String formatForLine(final String line)
	{
		String formattedLine = line;
		while (formattedLine.length() < LINE_LENGTH)
		{
			formattedLine += " ";
		}

		if (formattedLine.length() > LINE_LENGTH)
		{
			formattedLine = formattedLine.substring( 0, LINE_LENGTH );
		}

		return "\r" + formattedLine;
	}

	/**
	 * Extracts the first grouping of characters in the source string that matches the pattern
	 * @param source The string to extract the word from
	 * @param pattern The pattern to match
	 * @return The first substring to match the pattern
	 */
	public static String extractWord(final String source, final String pattern)
    {
		return Strings.extractWord( source, pattern, null );
	}

	public static String extractWord(final String source,
                                     final String pattern,
                                     final String defaultString)
	{
		Pattern regex = Pattern.compile(pattern);
		return Strings.extractWord( source, regex, defaultString );
	}

	public static String extractWord(final String source,
                                     final Pattern pattern,
                                     final String defaultString)
	{
		String matchedString = defaultString;
		Matcher match = pattern.matcher(source);

		if (match.find())
		{
			matchedString = match.group();
		}
		return matchedString;
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

	public static boolean contains(String full, String pattern)
	{
		Pattern regex = Pattern.compile(pattern);
		return regex.matcher(full).find();
	}

	public static String removePattern(String string, String pattern)
    {
        return string.replaceAll(pattern, "");
    }

    public static boolean isNaturalNumber(final CharSequence candidate)
    {
        for (int i = 0; i < candidate.length(); ++i)
        {
            if (i == 0 && candidate.charAt( i ) == '-')
            {
                continue;
            }
            if (!Character.isDigit( candidate.charAt( i ) ))
            {
                return false;
            }
        }

        return true;
    }

    public static boolean isFloatingPoint(final CharSequence candidate)
    {
        boolean decimalEncountered = false;

        for (int i = 0; i < candidate.length(); ++i)
        {
            if (i == 0 && candidate.charAt( i ) == '-')
            {
                continue;
            }

            if (candidate.charAt( i ) == '.')
            {
                if (decimalEncountered)
                {
                    return false;
                }

                decimalEncountered = true;
            }
            else if (!Character.isDigit( candidate.charAt( i ) ))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Generates a stack trace from the current thread
     * <br>
     * Useful for debugging and tracing asynchronous processes
     * @return A multiline string representation of the stack trace
     */
	public static String getStackTrace()
	{
        StackTraceElement[] traceElements = Thread.currentThread().getStackTrace();
        traceElements = Arrays.copyOfRange( traceElements, 2, traceElements.length);
        return Collections.toString( traceElements, StackTraceElement::toString, "    " + System.lineSeparator());
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

    public static String getFileName(String path)
    {
        return Paths.get(path).getFileName().toString();
    }

	/**
	 * Generates a hash from a file on the file system to identify a file's
	 * contents using the MD5 algorithm
	 * @param filename The path to the file to load into memory
	 * @return The hash that can be used to identify a file's contents
	 * @throws IOException Thrown if the file could not be read
	 */
    public static String getMD5Checksum(String filename) throws IOException
    {
        byte[] buffer = new byte[1024];
        MessageDigest complete = Strings.getMD5Algorithm();
        int bytesBuffered;
        int passCount = 0;

        try ( InputStream fileStream = new BufferedInputStream( new FileInputStream( filename ) ))
        {
            do
            {
                bytesBuffered = fileStream.read( buffer );

                if ( bytesBuffered > 0 )
                {
                    complete.update( buffer, 0, bytesBuffered );
                }
                passCount++;

            } while ( Strings.continueBufferingForChecksum( passCount, bytesBuffered ));
        }

        return getMD5Checksum( complete.digest() );
	}

	public static String getMD5Checksum(Object object) throws IOException
    {
        byte[] objectBytes;

        try ( ByteArrayOutputStream byteStream = new ByteArrayOutputStream(  );
              ObjectOutputStream stream = new ObjectOutputStream( byteStream ))
        {
            stream.writeObject( object );
            stream.flush();
            objectBytes = byteStream.toByteArray();
        }

        return Strings.getMD5Checksum( objectBytes );
    }

	/**
	 * Determines if buffering should continue for determining a checksum
	 * <p>
	 *     If short buffering is enabled (i.e., HASH_ENTIRE_FILE == false),
	 *     a warning is output to make it abundantly clear that this should not
	 *     be active for a genuine operating environment.
	 * </p>
	 * @param passCount The amount of times that the stream has passed through the data
	 * @param amountLastBuffered The amount of data that was last read through the stream
	 * @return Whether or not the function that loads data to hash should
	 * continue to attempt to read data
	 */
	private static boolean continueBufferingForChecksum(final int passCount, final int amountLastBuffered)
	{
		final int passLimit = 5000;

		boolean continueBuffering = amountLastBuffered != -1;

		if (continueBuffering)
		{
			continueBuffering = passCount < passLimit;
		}


		return continueBuffering;
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

		final String hexes = "0123456789ABCDEF";

		final StringBuilder hex = new StringBuilder( 2 * checksum.length );

		for (byte b : checksum)
		{
			hex.append(hexes.charAt((b & 0xF0) >> 4))
			   .append(hexes.charAt( b & 0x0F ));
		}

		return hex.toString();
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
            throw new RuntimeException(
                    "Something went wrong when trying to generate the MD5 algorithm",
                    e );
        }

        return algorithm;
    }

    public static String rightTrim( String string)
	{
		if( Strings.hasValue( string ))
		{
			return RTRIM.matcher( string ).replaceAll( "" );
		}
	
		return string;
	}

	public static boolean isValidPathFormat(final String path)
	{
		boolean isValid = false;

		try
		{
			Paths.get( path );
			isValid = true;
		}
		catch ( InvalidPathException invalid )
		{
			// If it isn't valid, we want to catch this, but not break
            LOGGER.trace("The path '{}' doesn't exist.", path);
		}

		return isValid;
	}
}
