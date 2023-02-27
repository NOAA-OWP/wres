package wres.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for manipulating strings.
 */
public final class Strings
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Strings.class );
    private static final Pattern RTRIM = Pattern.compile( "\\s+$" );

    private static final int LINE_LENGTH = 120;

    /**
     * Static list of string values that might map to the boolean value 'true'
     */
    private static final List<String> POSSIBLE_TRUE_VALUES =
            Arrays.asList( "true",
                           "True",
                           "TRUE",
                           "T",
                           "t",
                           "y",
                           "yes",
                           "Yes",
                           "YES",
                           "Y",
                           "1" );

    /**
     * @param possibleBoolean a possible representation of a boolean value
     * @return true if the string represents a boolean with "true" state
     */
    public static boolean isTrue( String possibleBoolean )
    {
        return Strings.hasValue( possibleBoolean ) && POSSIBLE_TRUE_VALUES.contains( possibleBoolean );
    }

    /**
     * @param word the word
     * @return true if the word is not null and not empty after trimming, otherwise false
     */
    public static boolean hasValue( String word )
    {
        return word != null && !word.trim().isEmpty();
    }

    /**
     * @param line the line to format
     * @return the formatted line
     */
    public static String formatForLine( final String line )
    {
        String formattedLine = line;
        while ( formattedLine.length() < LINE_LENGTH )
        {
            formattedLine += " ";
        }

        if ( formattedLine.length() > LINE_LENGTH )
        {
            formattedLine = formattedLine.substring( 0, LINE_LENGTH );
        }

        return "\r" + formattedLine;
    }

    /**
     * @param source the source string
     * @param pattern the pattern to match
     * @param defaultString the default string
     * @return the extracted word
     */
    public static String extractWord( final String source,
                                      final Pattern pattern,
                                      final String defaultString )
    {
        String matchedString = defaultString;
        Matcher match = pattern.matcher( source );

        if ( match.find() )
        {
            matchedString = match.group();
        }
        return matchedString;
    }

    /**
     * @param full the string
     * @param pattern the pattern to match
     * @return true if the pattern is matched, false otherwise
     */
    public static boolean contains( String full, String pattern )
    {
        Pattern regex = Pattern.compile( pattern );
        return regex.matcher( full ).find();
    }

    /**
     * @param error the error
     * @return a stack trace string
     */
    public static String getStackTrace( Exception error )
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream( baos );
        error.printStackTrace( ps );
        ps.close();
        return baos.toString();
    }

    /**
     * @param checksum the checksum bytes
     * @return a hex string representation
     */
    public static String getMD5Checksum( byte[] checksum )
    {
        Objects.requireNonNull( checksum, "A hash cannot be generated for a non-existent byte array" );

        if ( checksum.length > 16 )
        {
            MessageDigest complete = getMD5Algorithm();
            complete.update( checksum );
            checksum = complete.digest();
        }

        final String hexes = "0123456789ABCDEF";

        final StringBuilder hex = new StringBuilder( 2 * checksum.length );

        for ( byte b : checksum )
        {
            hex.append( hexes.charAt( ( b & 0xF0 ) >> 4 ) )
               .append( hexes.charAt( b & 0x0F ) );
        }

        return hex.toString();
    }

    /**
     * @param string the string to trim
     * @return the right-trimmed string
     */
    public static String rightTrim( String string )
    {
        if ( Strings.hasValue( string ) )
        {
            return RTRIM.matcher( string ).replaceAll( "" );
        }

        return string;
    }

    /**
     * @param path the path to check
     * @return true if the path format is valid, otherwise false
     */

    public static boolean isValidPathFormat( final String path )
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
            LOGGER.trace( "The path '{}' doesn't exist.", path );
        }

        return isValid;
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
            throw new IllegalStateException( "Something went wrong when trying to generate the MD5 algorithm", e );
        }

        return algorithm;
    }

    private Strings()
    {
    }
}
