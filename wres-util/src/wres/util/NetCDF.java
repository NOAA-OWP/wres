package wres.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.nio.file.Paths;

/**
 * Created by ctubbs on 7/7/17.
 */
public final class NetCDF {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDF.class);

    // TODO: Maybe change this to support forcing data?
    private static final Pattern NWM_NAME_PATTERN = Pattern.compile(
            "^nwm\\.t\\d\\dz\\.(short|medium|long|analysis)_(range|assim)\\.[a-zA-Z]+(_rt)?(_\\d)?\\.(f\\d\\d\\d|tm\\d\\d)\\.conus\\.nc(\\.gz)?$"
    );

    private static final Pattern NWM_CATEGORY_PATTERN =
            Pattern.compile( "(?<=(assim|range)\\.)[a-zA-Z_\\d]+(?=\\.(tm\\d\\d|f\\d\\d\\d))" );

    private static final Pattern NETCDF_FILENAME_PATTERN = Pattern.compile( ".+\\.nc(\\.gz)?$" );

    private static final Pattern SHORT_DATE_PATTERN = Pattern.compile(".*\\d{8}");

    private static final DateTimeFormatter STANDARD_DATE_FORMAT = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss z" );

    public static class Ensemble
    {
        private final String name;
        private final String qualifier;
        private final Integer member;

        public Ensemble(String name, String qualifier, Integer member)
        {
            this.name = name;
            this.qualifier = qualifier;
            this.member = member;
        }

        public String getName()
        {
            return this.name;
        }

        public String getQualifier()
        {
            return this.qualifier;
        }

        public Integer getMember()
        {
            return this.member;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.name, this.qualifier, this.member);
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof Ensemble && this.hashCode() == obj.hashCode();
        }
    }


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
        public static VectorVariableIterator from(final Variable variable)
        {
            return VectorVariableIterator.from(variable, DEFAULT_BUFFER_SIZE);
        }

        /**
         * Create an instance
         * @param variable The variable to iterate over
         * @param bufferSize The number of values to load at a time when iterating
         * @return A new VectorVariableIterator instance
         */
        public static VectorVariableIterator from(final Variable variable, final int bufferSize)
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

        private VectorVariableIterator(final Variable variable, final int bufferSize)
        {
            this.variable = variable;
            this.bufferSize = bufferSize;
            this.length = (int)variable.getSize();
            this.variableName = variable.getShortName();
            this.location = variable.getDatasetLocation();
        }

        @Override
        public boolean hasNext()
        {
            boolean canBufferMoreData = this.index < this.length;

            if (canBufferMoreData && (this.currentData == null || !this.currentData.hasNext()))
            {
                this.loadBufferData();
            }

            return canBufferMoreData && this.currentData.hasNext();
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public Object next()
        {
            if (this.hasNext())
            {
                Object value = this.currentData.next();
                this.index++;

                return value;
            }
            else
            {
                throw new NoSuchElementException("There are no more values to iterate over.");
            }
        }

        public int nextInt()
        {
            if (this.hasNext())
            {
                int value = this.currentData.nextInt();
                this.index++;

                return value;
            }
            else
            {
                throw new NoSuchElementException("There are no more values to iterate over.");
            }
        }

        public double nextDouble()
        {
            if (this.hasNext())
            {
                double value = this.currentData.nextDouble();
                this.index++;

                return value;
            }
            else
            {
                throw new NoSuchElementException("There are no more values to iterate over.");
            }
        }

        private void loadBufferData()
        {
            // Create a new Array to iterate over

            // We want the origin to be the index we're on; we start at 0, then continue through the
            // buffer size and we'll be at 0 + bufferSize, 0 + bufferSize * 2, etc
            int[] origin = new int[] {this.index};

            // We want to pull in the up to bufferSize values, but we'll suffer an
            // InvalidRangeException if we try to pull to much. As a result, we want to pull through
            // the very end if there's less than bufferSize elements left
            int[] shape = new int[] {Math.min(this.bufferSize, this.length - this.index)};

            try
            {
                this.currentData = this.variable.read( origin, shape );
            }
            catch ( IOException e )
            {
                String message = "The Netcdf variable %s at %s could not be read.";
                message = String.format( message, this.variableName, this.location );
                throw new IterationFailedException( message );
            }
            catch ( InvalidRangeException e )
            {
                String message = "The iterator tried to read past the end of the variable data. ";
                message += "The iterator tried to read %d elements starting at the index %d, ";
                message += "but there were only %d elements available to read.";

                message = String.format(message, shape[0], origin[0], this.length);
                throw new IterationFailedException( message );
            }
        }

        @Override
        public void forEachRemaining( Consumer<? super Object> action )
        {
            while(this.hasNext())
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

    private NetCDF() {}

    public static DateTimeFormatter getStandardDateFormat()
    {
        return STANDARD_DATE_FORMAT;
    }

    /**
     * Determines if a variable with the indicated short name exists within the NetCDF file
     * The provided "findVariable" function searches by full name, rather than short name. Since we only
     * care about the short name, this function had to be implemented.
     *
     * @param file The NetCDF file to look into
     * @param variableName The short name of the variable to find
     * @return A boolean indicating whether or not the file has the given variable name
     */
    public static boolean hasVariable (NetcdfFile file, String variableName)
    {
        return NetCDF.getVariable( file, variableName ) != null;
    }

    /**
     * Attempts to retrieve the variable with the given short name from within the passed in NetCDF file
     * @param file The file to search through
     * @param variableName The short name of the variable to retrieve
     * @return If the variable exists, the variable is retrieved. Otherwise, it is null.
     */
    public static Variable getVariable(NetcdfFile file, String variableName)
    {
        return file.findVariable( variableName );
    }

    /**
     * Finds the coordinate variable within a Netcdf file that is used to index other single parametered variables
     * @param file The source file
     * @return The main coordinate variable
     */
    public static Variable getVectorCoordinateVariable(NetcdfFile file)
    {
        Variable nonCoordinate = Collections.find(
                file.getVariables(),
                variable -> variable.getDimensions().size() == 1 && !variable.isCoordinateVariable()
        );

        String coordinateName = nonCoordinate.getDimensions().get( 0 ).getShortName();

        Variable vectorCoordinate = file.findVariable( coordinateName );

        Objects.requireNonNull( vectorCoordinate, "A vector coordinate variable could not be found."  );

        return vectorCoordinate;
    }

    public static Duration getLeadTime( NetcdfFile file) throws IOException
    {
        Duration lead = Duration.ZERO;
        Instant initializedTime = NetCDF.getReferenceTime( file );
        Instant validTime = NetCDF.getTime( file );

        if (initializedTime != null && validTime != null)
        {
            lead = Duration.between( initializedTime, validTime );
        }

        if (lead.equals( Duration.ZERO ))
        {
            LOGGER.warn( "A proper lead time could not be determined for the "
                         + "forecast data in '{}'", file.getLocation() );
        }

        return lead;
    }

    public static Instant getTime( NetcdfFile file)
            throws IOException
    {
        Variable time = NetCDF.getVariable( file, "time" );
        Array timeValues;

        try
        {
            timeValues = time.read( new int[]{0}, new int[]{1} );
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "A valid time value could not be retrieved from '" + file.getLocation() + "#time'", e );
        }

        int minutes = timeValues.getInt( 0 );
        return Instant.ofEpochSecond( minutes * 60 );
    }

    public static Instant getReferenceTime(NetcdfFile file)
            throws IOException
    {
        Variable time = NetCDF.getVariable( file, "reference_time" );

        if (time == null)
        {
            time = NetCDF.getVariable( file, "analysis_time" );
        }

        if (time == null)
        {
            return NetCDF.getTime(file);
        }

        Array timeValues;

        try
        {
            timeValues = time.read( new int[]{0}, new int[]{1} );
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "A valid time value could not be retrieved from '" +
                                   file.getLocation() + "#" + time.getShortName() + "'", e );
        }

        int minutes = timeValues.getInt( 0 );
        return Instant.ofEpochSecond( minutes * 60 );
    }

    public static Attribute getVariableAttribute(final Variable variable, final String attributeName)
    {
        return Collections.find(variable.getAttributes(), attribute ->
            attribute.getShortName().equalsIgnoreCase(attributeName)
        );
    }

    private static String[] getNWMFilenameParts(NetcdfFile file)
    {
        String name = Strings.getFileName(file.getLocation());
        name = Strings.removePattern(name, "\\.gz");
        name = Strings.removePattern(name, "nwm\\.");
        name = Strings.removePattern(name, "\\.nc");
        return name.split("\\.");
    }

    public static boolean isNetCDFFile(String filename)
    {
        return NetCDF.NETCDF_FILENAME_PATTERN.matcher( filename ).matches();
    }

    private static boolean isNWMData(NetcdfFile file)
    {
        return NetCDF.isNWMData( Strings.getFileName(file.getLocation()) );
    }

    private static boolean isNWMData(String filename)
    {
        return NetCDF.NWM_NAME_PATTERN.matcher( filename ).matches();
    }


    private static String getNWMCategory(NetcdfFile file)
    {
        return Strings.extractWord( Paths.get(file.getLocation()).getFileName().toString(),
                                    NWM_CATEGORY_PATTERN,
                                    "Unknown" );
    }

    public static Ensemble getEnsemble(NetcdfFile file)
    {
        String name = "Unknown";
        String qualifier = "";
        Integer member = 0;

        String memberString = null;

        Attribute modelConfiguration = file.findGlobalAttributeIgnoreCase( "model_configuration" );
        Attribute modelOutputType = file.findGlobalAttributeIgnoreCase( "model_output_type" );
        Attribute ensembleMemberNumber = file.findGlobalAttributeIgnoreCase( "ensemble_member_number" );

        if (modelConfiguration != null)
        {
            name = modelConfiguration.getStringValue();
        }

        if (modelOutputType != null)
        {
            qualifier = modelOutputType.getStringValue();
        }

        if (ensembleMemberNumber != null)
        {
            memberString = ensembleMemberNumber.getStringValue();

            if (Strings.isNaturalNumber( memberString ))
            {
                member = Integer.parseInt( memberString );
            }
        }

        if (NetCDF.isNWMData( file ) && (ensembleMemberNumber == null || modelConfiguration == null))
        {
            String[] parts = NetCDF.getNWMFilenameParts( file );

            if (modelConfiguration == null)
            {
                name = Collections.find( parts, possibility ->
                        Strings.hasValue( possibility ) &&
                        ( possibility.endsWith( "range" ) || possibility.endsWith(
                                "assim" ) ) );
            }

            if (ensembleMemberNumber == null && name.endsWith( "assim" ))
            {
                String minus = Collections.find(parts,
                                                possibility ->
                                                        Strings.hasValue( possibility ) &&
                                                        possibility.startsWith( "tm" )
                );

                if (Strings.hasValue( minus ))
                {
                    memberString = Strings.extractWord( minus, "\\d\\d" );

                    if (Strings.isNaturalNumber( memberString ))
                    {
                        member = Integer.parseInt( memberString );
                    }
                }
            }
        }

        if (modelOutputType == null)
        {
            Path location = Paths.get( file.getLocation() );
            ArrayList<String> partList = new ArrayList<>();

            for ( int part = 0; part < 3; ++part )
            {
                if ( location.getParent() == null )
                {
                    break;
                }

                location = location.getParent();

                // If this portion of the path isn't something like "nwm.20180411", we append it
                // We can't vouch for any other patterns, but anything containing
                // a pattern like that is bound to be hyper unique where it really shouldn't be
                if ( !NetCDF.SHORT_DATE_PATTERN.matcher( location.getFileName().toString() ).matches() )
                {
                    partList.add( 0, location.getFileName().toString() );
                }
            }

            partList.add( NetCDF.getNWMCategory( file ) );

            qualifier = String.join( ":", partList );
        }

        return new Ensemble( name, qualifier, member );
    }

    /**
     * Evaluates a unique identifier for the source of NetCDF data
     * @param filepath The path to the NetCDF data
     * @param variableName The variable of interest for the NetCDF data
     * @return An Unique Identifier for specific data
     * @throws IOException Thrown if the file could not be found
     * @throws IOException Thrown if the file could not be opened
     * @throws IOException Thrown if the file could not be read in order to produce a hash
     */
    public static String getUniqueIdentifier( final URI filepath, final String variableName ) throws IOException
    {
        String uniqueIdentifier;

        // With gridded data, we just care about the file itself; variable info will be dealt with later
        if (NetCDF.isGridded( filepath.toURL().getFile() ))
        {
            uniqueIdentifier = NetCDF.getGriddedUniqueIdentifier( filepath );
        }
        else
        {
            // With vector data, we need to know a) what data it is and b) what variable we're looking to ingest
            uniqueIdentifier = Strings.getMD5Checksum( filepath );
            uniqueIdentifier += "::" + variableName;
        }

        return uniqueIdentifier;
    }

    public static String getGriddedUniqueIdentifier( final URI filepath ) throws IOException
    {
        try (NetcdfFile file = NetcdfFile.open( filepath.toURL().getFile() ))
        {
            return NetCDF.getGriddedUniqueIdentifier( file, filepath );
        }
    }

    public static String getGriddedUniqueIdentifier( final NetcdfFile file, final URI target ) throws IOException
    {
        String uniqueIdentifier;
        StringJoiner identityJoiner = new StringJoiner( "::" );

        identityJoiner.add( InetAddress.getLocalHost().getHostName());
        identityJoiner.add( InetAddress.getLocalHost().getHostAddress() );
        identityJoiner.add( target.toURL().toString() );

        NetCDF.addNetcdfIdentifiers( file, identityJoiner );

        uniqueIdentifier = identityJoiner.toString();
        uniqueIdentifier = Strings.getMD5Checksum( uniqueIdentifier.getBytes() );

        return uniqueIdentifier;
    }

    public static void addNetcdfIdentifiers(final NetcdfFile file, final StringJoiner joiner)
    {
        for ( Variable var : file.getVariables() )
        {
            joiner.add( var.getNameAndDimensions() );
        }

        for ( Attribute attr : file.getGlobalAttributes() )
        {
            joiner.add( attr.toString() );
        }
    }

    public static Integer getYLength(Variable var)
    {
        Integer length = null;

        switch (var.getDimensions().size())
        {
            case 2:
                length = var.getDimension(1).getLength();
                break;
            case 3:
                length = var.getDimension(2).getLength();
                break;
        }

        return length;
    }

    // TODO: We need a way to determine if the first variable is time and the
    // second variable is the coordinate; in that case, this will return true,
    // but the data will be vector, not gridded.
    public static boolean isGridded(Variable var)
    {
        Integer length = NetCDF.getYLength(var);

        return length != null && length > 0;
    }

    public static boolean isGridded(final String filename) throws IOException
    {
        try (NetcdfFile file = NetcdfFile.open( filename ))
        {
            for ( Variable var : file.getVariables() )
            {
                if ( !var.isCoordinateVariable() && var.getDimensions().size() > 2 )
                {
                    return true;
                }
            }
        }

        return false;
    }

    public static double getScaleFactor(Variable var)
    {
        double scaleFactor = 1.0;
        Attribute factor = NetCDF.getVariableAttribute(var, "scale_factor");

        if (factor != null)
        {
            scaleFactor = factor.getNumericValue().doubleValue();
        }

        return scaleFactor;
    }

    public static double getMinimumValue(Variable var)
    {
        double minimum = -Double.MAX_VALUE;

        Attribute range = NetCDF.getVariableAttribute( var, "valid_range" );

        if (range != null)
        {
            minimum = range.getNumericValue( 0 ).doubleValue();
        }

        return minimum;
    }

    public static double getMaximumValue(Variable var)
    {
        double maximum = Double.MAX_VALUE;

        Attribute range = NetCDF.getVariableAttribute( var, "valid_range" );

        if (range != null)
        {
            maximum = range.getNumericValue( 1 ).doubleValue();
        }

        return maximum;
    }

    public static double getAddOffset(Variable var)
    {
        double addOffset = 0.0;

        Attribute offset = NetCDF.getVariableAttribute(var, "add_offset");

        if (offset != null)
        {
            addOffset = offset.getNumericValue().doubleValue();
        }

        return addOffset;
    }

    public static Double getMissingValue(Variable var)
    {
        Double value = null;

        Attribute missingValue = NetCDF.getVariableAttribute(var, "missing_value");
        if (missingValue == null)
        {
            missingValue = NetCDF.getVariableAttribute(var, "_FillValue");
        }

        if (missingValue != null)
        {
            value = missingValue.getNumericValue().doubleValue() * NetCDF.getScaleFactor(var);
        }

        return value;
    }

    public static Double getGlobalMissingValue(NetcdfFile file)
    {
        Double value = null;

        Attribute missingValue = file.findGlobalAttribute("missing_value");

        if (missingValue == null)
        {
            missingValue = file.findGlobalAttribute("_FillValue");
        }

        if (missingValue != null)
        {
            value = missingValue.getNumericValue().doubleValue();
        }

        return value;
    }

    /**
     *
     * @param variable The coordinate variable holding the value
     * @param value The value whose index to find
     * @return The index at which the value may be found in the variable
     * @throws IOException Thrown if values could not be read from the variable
     * @throws IOException Thrown if the variable is not a coordinate variable
     * @throws InvalidRangeException Thrown if the dimensions that are read
     * from the variable are out of range
     */
    public static Integer getCoordinateIndex(final Variable variable, Object value)
            throws IOException, InvalidRangeException
    {
        if (!variable.isCoordinateVariable())
        {
            throw new IOException( "A coordinate index cannot be read from a non-coordinate variable." );
        }

        // This will be the number of indices we traverse and return
        int indexCount = 0;
        Array values;
        try
        {
            try
            {
                values = variable.read();
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                LOGGER.error("Reading array indexes failed.");
                throw e;
            }

            while ( values.hasNext() )
            {
                Object readValue;
                try
                {
                    readValue = values.next();
                }
                catch (ArrayIndexOutOfBoundsException e)
                {
                    LOGGER.error("Reading next array value failed.");
                    throw e;
                }

                if ( readValue == value || readValue.equals( value ) )
                {
                    LOGGER.info( "Coordinate value for {} found at {}.",
                                 value,
                                 indexCount );
                    return indexCount;
                }
                indexCount++;
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            LOGGER.error("Reading array indexes failed.");
            throw e;
        }
        return null;
    }
}
