package wres.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.datamodel.DataException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * @author Chris Tubbs.
 */
public final class Netcdf
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Netcdf.class );
    private static final DateTimeFormatter STANDARD_DATE_FORMAT =
            DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss z" );

    /**
     * A buffered iterator over Netcdf Variable values
     */
    public static class VectorVariableIterator implements Iterator<Object>
    {
        /**
         * The maximum number of values to load from the variable if none is given upon construction
         */
        private static final int DEFAULT_BUFFER_SIZE = 1000;

        /**
         * Create an instance with the default buffer size
         * @param variable The variable to iterate over
         * @return A new VectorVariableIterator instance
         */
        public static VectorVariableIterator from( final Variable variable )
        {
            return VectorVariableIterator.from( variable, DEFAULT_BUFFER_SIZE );
        }

        /**
         * Create an instance
         * @param variable The variable to iterate over
         * @param bufferSize The number of values to load at a time when iterating
         * @return A new VectorVariableIterator instance
         */
        public static VectorVariableIterator from( final Variable variable, final int bufferSize )
        {
            return new VectorVariableIterator( variable, bufferSize );
        }

        /**
         * The variable that is being iterated over
         */
        private final Variable variable;

        /**
         * The maximum number of values that may be loaded at a time
         */
        private final int bufferSize;

        /**
         * The number of values in the variable
         */
        private final int length;

        /**
         * The name of the variable; used for error messaging
         */
        private final String variableName;

        /**
         * The location of the data containing the variable; used for error messaging
         */
        private final String location;

        /**
         * The index of the current value in the variable
         */
        private int index = 0;

        /**
         * The currently loaded buffer of values from the array
         */
        private Array currentData;

        private VectorVariableIterator( final Variable variable, final int bufferSize )
        {
            this.variable = variable;
            this.bufferSize = bufferSize;
            this.length = ( int ) variable.getSize();
            this.variableName = variable.getShortName();
            this.location = variable.getDatasetLocation();
        }

        @Override
        public boolean hasNext()
        {
            boolean canBufferMoreData = this.index < this.length;

            if ( canBufferMoreData && ( this.currentData == null || !this.currentData.hasNext() ) )
            {
                this.loadBufferData();
            }

            return canBufferMoreData && this.currentData.hasNext();
        }

        @Override
        public Object next()
        {
            if ( this.hasNext() )
            {
                Object value = this.currentData.next();
                this.index++;

                return value;
            }
            else
            {
                throw new NoSuchElementException( "There are no more values to iterate over." );
            }
        }

        /**
         * @return the next double
         */
        public double nextDouble()
        {
            if ( this.hasNext() )
            {
                double value = this.currentData.nextDouble();
                this.index++;

                return value;
            }
            else
            {
                throw new NoSuchElementException( "There are no more values to iterate over." );
            }
        }

        private void loadBufferData()
        {
            // Create a new Array to iterate over

            // We want the origin to be the index we're on; we start at 0, then continue through the
            // buffer size and we'll be at 0 + bufferSize, 0 + bufferSize * 2, etc
            int[] origin = new int[] { this.index };

            // We want to pull in the up to bufferSize values, but we'll suffer an
            // InvalidRangeException if we try to pull to much. As a result, we want to pull through
            // the very end if there's less than bufferSize elements left
            int[] shape = new int[] { Math.min( this.bufferSize, this.length - this.index ) };

            try
            {
                this.currentData = this.variable.read( origin, shape );
            }
            catch ( IOException e )
            {
                String message = "The Netcdf variable %s at %s could not be read.";
                message = String.format( message, this.variableName, this.location );
                throw new DataException( message );
            }
            catch ( InvalidRangeException e )
            {
                String message = "The iterator tried to read past the end of the variable data. ";
                message += "The iterator tried to read %d elements starting at the index %d, ";
                message += "but there were only %d elements available to read.";

                message = String.format( message, shape[0], origin[0], this.length );
                throw new NoSuchElementException( message );
            }
        }

        @Override
        public void forEachRemaining( Consumer<? super Object> action )
        {
            while ( this.hasNext() )
            {
                action.accept( this.next() );
            }
        }

        /**
         * @return The index of the value last read from the iterator
         */
        public int getIndexOfLastValue()
        {
            return this.index - 1;
        }
    }

    private Netcdf()
    {
    }

    /**
     * @return a date formatter
     */
    public static DateTimeFormatter getStandardDateFormat()
    {
        return STANDARD_DATE_FORMAT;
    }

    /**
     * Attempts to retrieve the variable with the given short name from within the passed in Netcdf file
     * @param file The file to search through
     * @param variableName The short name of the variable to retrieve
     * @return If the variable exists, the variable is retrieved. Otherwise, it is null.
     */
    public static Variable getVariable( NetcdfFile file, String variableName )
    {
        return file.findVariable( variableName );
    }

    /**
     * @param file the file
     * @return the lead duration
     * @throws IOException if the lead duration could not be determined
     */
    public static Duration getLeadTime( NetcdfFile file ) throws IOException
    {
        Duration lead = Duration.ZERO;
        Instant initializedTime = Netcdf.getReferenceTime( file );
        Instant validTime = Netcdf.getTime( file );

        if ( initializedTime != null && validTime != null )
        {
            lead = Duration.between( initializedTime, validTime );
        }

        if ( lead.equals( Duration.ZERO ) )
        {
            LOGGER.warn( "A proper lead time could not be determined for the "
                         + "forecast data in '{}'", file.getLocation() );
        }

        return lead;
    }

    /**
     * @param file the file
     * @return the valid time
     * @throws IOException if the file could not be read
     */
    public static Instant getTime( NetcdfFile file )
            throws IOException
    {
        Variable time = Netcdf.getVariable( file, "time" );
        Array timeValues;

        try
        {
            timeValues = time.read( new int[] { 0 }, new int[] { 1 } );
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "A valid time value could not be retrieved from '" + file.getLocation() + "#time'",
                                   e );
        }

        int minutes = timeValues.getInt( 0 );
        return Instant.ofEpochSecond( minutes * 60L );
    }

    /**
     * @param file the file
     * @return the reference time
     * @throws IOException if the file could not be read
     */

    public static Instant getReferenceTime( NetcdfFile file )
            throws IOException
    {
        Variable time = Netcdf.getVariable( file, "reference_time" );

        if ( time == null )
        {
            time = Netcdf.getVariable( file, "analysis_time" );
        }

        if ( time == null )
        {
            return Netcdf.getTime( file );
        }

        Array timeValues;

        try
        {
            timeValues = time.read( new int[] { 0 }, new int[] { 1 } );
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "A valid time value could not be retrieved from '" +
                                   file.getLocation() + "#" + time.getShortName() + "'", e );
        }

        int minutes = timeValues.getInt( 0 );
        return Instant.ofEpochSecond( minutes * 60L );
    }

    /**
     * @param filepath the path to a file
     * @param variableName the variable name
     * @return a unique identifier for the gridded data
     * @throws IOException if the identifier could not be determined
     */
    public static String getGriddedUniqueIdentifier( final URI filepath, String variableName ) throws IOException
    {
        try ( NetcdfFile file = NetcdfFiles.open( filepath.toURL().getFile() ) )
        {
            return Netcdf.getGriddedUniqueIdentifier( file, filepath, variableName );
        }
    }

    /**
     * @param file the file
     * @param target the target
     * @param variableName the variable name
     * @return a unique identifier for the gridded data
     * @throws IOException if the identifier could not be determined
     */
    public static String getGriddedUniqueIdentifier( final NetcdfFile file,
                                                     final URI target,
                                                     final String variableName ) throws IOException
    {
        String uniqueIdentifier;
        StringJoiner identityJoiner = new StringJoiner( "::" );

        identityJoiner.add( InetAddress.getLocalHost().getHostName() );
        identityJoiner.add( InetAddress.getLocalHost().getHostAddress() );
        identityJoiner.add( target.toURL().toString() );

        Netcdf.addNetcdfIdentifiers( file, identityJoiner );

        identityJoiner.add( variableName );

        uniqueIdentifier = identityJoiner.toString();
        uniqueIdentifier = Netcdf.getMD5Checksum( uniqueIdentifier.getBytes() );

        return uniqueIdentifier;
    }

    /**
     * @param file the file
     * @param joiner a joiner
     */
    public static void addNetcdfIdentifiers( final NetcdfFile file, final StringJoiner joiner )
    {
        for ( Variable variable : file.getVariables() )
        {
            joiner.add( variable.getNameAndDimensions() );
        }

        for ( Attribute attr : file.getGlobalAttributes() )
        {
            joiner.add( attr.toString() );
        }
    }


    /**
     * @param checksum the checksum bytes
     * @return a hex string representation
     */
    private static String getMD5Checksum( byte[] checksum )
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

}
