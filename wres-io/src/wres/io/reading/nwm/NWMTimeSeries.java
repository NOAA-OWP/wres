package wres.io.reading.nwm;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.reading.PreIngestException;

/**
 * Goal: a variable/feature/timeseries combination in a Vector NWM dataset is
 * considered a source.
 *
 * Only the variable and feature selected for an evaluation will be ingested
 * from a vector NWM dataset.
 *
 * All the NWM netCDF blobs for a given timeseries will first be found and
 * opened prior to attempting to identify the timeseries, prior to attempting
 * to ingest any rows of timeseries data.
 *
 * This class opens a set of NWM netCDF blobs as a timeseries, based on a profile.
 */

class NWMTimeSeries implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWMTimeSeries.class );
    private static final DateTimeFormatter NWM_DATE_FORMATTER = DateTimeFormatter.ofPattern( "yyyyMMdd" );
    private static final DateTimeFormatter NWM_HOUR_FORMATTER = DateTimeFormatter.ofPattern( "HH" );

    private final NWMProfile profile;

    /** The reference datetime of this NWM Forecast */
    private final Instant referenceDatetime;

    /** The base URI from where to find the members of this forecast */
    private final URI baseUri;

    private final Set<NetcdfFile> netcdfFiles = new HashSet<>();

    /**
     *
     * @param profile
     * @param referenceDatetime
     * @param baseUri
     * @throws NullPointerException When any argument is null.
     * @throws PreIngestException When any netCDF blob could not be opened.
     */
    NWMTimeSeries( NWMProfile profile,
                   Instant referenceDatetime,
                   URI baseUri )
    {
        Objects.requireNonNull( profile );
        Objects.requireNonNull( referenceDatetime );
        Objects.requireNonNull( baseUri );
        this.profile = profile;
        this.referenceDatetime = referenceDatetime;
        this.baseUri = baseUri;

        // Build the set of URIs based on the profile given.
        Set<URI> netcdfUris = NWMTimeSeries.getNetcdfUris( profile,
                                                           referenceDatetime,
                                                           baseUri );

        // Open all the relevant files during construction, or fail.
        for ( URI netcdfUri : netcdfUris )
        {
            NetcdfFile netcdfFile = NWMTimeSeries.openFile( netcdfUri );
            this.netcdfFiles.add( netcdfFile );
        }
    }


    /**
     * Create the Set of URIs for the whole forecast based on given nwm profile.
     * <br />
     * Assumes:
     * <ol>
     *     <li>NWM emits a regular timeseries using a single timestep.</li>
     *     <li>The first value in a timeseries is one timestep after reference
     *         date.</li>
     * </ol>
     * @param profile The metadata describing the NWM timeseries(es).
     * @param referenceDatetime The reference datetime for the forecast set.
     * @param baseUri The file or network protocol and path prefix.
     * @return The full Set of URIs for a single forecast
     */

    static Set<URI> getNetcdfUris( NWMProfile profile,
                                   Instant referenceDatetime,
                                   URI baseUri )
    {
        LOGGER.debug( "Called getNetcdfUris with {}, {}, {}", profile,
                      referenceDatetime, baseUri );
        Set<URI> uris = new HashSet<>();
        final String NWM_DOT = "nwm.";

        // Formatter cannot handle Instant
        OffsetDateTime referenceOffsetDateTime = OffsetDateTime.ofInstant( referenceDatetime,
                                                                           ZoneId.of( "UTC" ) );
        String nwmDatePath = NWM_DOT
                             + NWM_DATE_FORMATTER.format( referenceOffsetDateTime );

        for ( short i = 1; i <= profile.getMemberCount(); i++ )
        {
            URI uriWithDate = baseUri.resolve( nwmDatePath + "/" );

            String directoryName = profile.getNwmSubdirectoryPrefix();

            if ( profile.getMemberCount() > 1 )
            {
                directoryName += "_mem" + i ;
            }

            URI uriWithDirectory = uriWithDate.resolve( directoryName + "/" );

            for ( short j = 1; j <= profile.getBlobCount(); j++ )
            {
                String ncFilePartOne = NWM_DOT + "t"
                                       + NWM_HOUR_FORMATTER.format( referenceOffsetDateTime )
                                       + "z." + profile.getNwmConfiguration()
                                       + "." + profile.getNwmOutputType();

                // Ensemble number appended if greater than one member present.
                if ( profile.getMemberCount() > 1 )
                {
                    ncFilePartOne += "_" + i;
                }

                String ncFilePartTwo = "." + profile.getTimeLabel();

                long hours = profile.getDurationBetweenValidDatetimes()
                                    .toHours()
                             * j;

                if ( profile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.f ) )
                {
                    String forecastLabel = String.format( "%03d", hours );
                    ncFilePartTwo += forecastLabel;
                }
                else if ( profile.getTimeLabel()
                                 .equals( NWMProfile.TimeLabel.tm ))
                {
                    // Analysis files go back in valid datetime as j increases.
                    String analysisLabel = String.format( "%02d", j - 1 );
                    ncFilePartTwo += analysisLabel;
                }

                String ncFilePartThree = "." + profile .getNwmLocationLabel()
                                         + ".nc";
                String ncFile = ncFilePartOne + ncFilePartTwo + ncFilePartThree;
                LOGGER.trace( "Built a netCDF filename: {}", ncFile );

                URI fullUri = uriWithDirectory.resolve( ncFile );
                uris.add( fullUri );
            }
        }

        LOGGER.debug( "Returning these netCDF URIs: {}", uris );
        return Collections.unmodifiableSet( uris );
    }

    private static NetcdfFile openFile( URI netcdfUri )
    {
        try
        {
            return NetcdfFile.open( netcdfUri.toString() );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to open netCDF file "
                                          + netcdfUri, ioe );
        }
    }

    NWMProfile getProfile()
    {
        return this.profile;
    }

    Instant getReferenceDatetime()
    {
        return this.referenceDatetime;
    }

    URI getBaseUri()
    {
        return this.baseUri;
    }

    private Set<NetcdfFile> getNetcdfFiles()
    {
        return this.netcdfFiles;
    }

    int countOfNetcdfFiles()
    {
        return this.getNetcdfFiles().size();
    }


    TimeSeries<Ensemble> readEnsembleTimeSeries( int featureId, String variableName )
    {
        final int NOT_FOUND = Integer.MIN_VALUE;

        int memberCount = this.getProfile().getMemberCount();
        int validDatetimeCount = this.getProfile().getBlobCount();


        // Map from ensemble number to map of instant to double[]
        Map<Instant,double[]> ensembleValues = new HashMap<>( memberCount );

        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            // Get the ensemble_member_number
            String memberNumberAttributeName = this.getProfile().getMemberAttribute();
            List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();

            int ncEnsembleNumber = NOT_FOUND;

            for ( Attribute globalAttribute : globalAttributes )
            {
                if ( globalAttribute.getShortName().equals( memberNumberAttributeName ) )
                {
                    ncEnsembleNumber = globalAttribute.getNumericValue()
                                                      .intValue();
                    break;
                }
            }

            if ( ncEnsembleNumber == NOT_FOUND )
            {
                throw new PreIngestException( "Could not find ensemble member attribute "
                                              + memberNumberAttributeName +
                                              " in netCDF file "
                                              + netcdfFile );
            }

            if ( ncEnsembleNumber > memberCount )
            {
                throw new PreIngestException( "Ensemble number "
                                              + ncEnsembleNumber
                                              + " unexpectedly exceeds member count "
                                              + memberCount );
            }

            if ( ncEnsembleNumber < 1 )
            {
                throw new PreIngestException( "Ensemble number "
                                              + ncEnsembleNumber
                                              + " is unexpectedly less than 1." );
            }

            Event<Double> event = readDouble( netcdfFile, featureId, variableName );
            double[] ensembleRow = ensembleValues.get( event.getTime() );

            if ( Objects.isNull( ensembleRow ) )
            {
                ensembleRow = new double[memberCount];
                ensembleValues.put( event.getTime(), ensembleRow );
            }

            ensembleRow[ncEnsembleNumber - 1] = event.getValue();
        }

        if ( ensembleValues.size() != validDatetimeCount )
        {
            throw new PreIngestException( "Expected "
                                          + validDatetimeCount
                                          + " different valid datetimes but only found "
                                          + ensembleValues.size()
                                          + " in netCDF files "
                                          + this.getNetcdfFiles() );
        }

        SortedSet<Event<Ensemble>> sortedEvents = new TreeSet<>();

        for ( Map.Entry<Instant,double[]> entry : ensembleValues.entrySet() )
        {
            Ensemble ensemble = Ensemble.of( entry.getValue() );
            Event<Ensemble> ensembleEvent = Event.of( entry.getKey(), ensemble );
            sortedEvents.add( ensembleEvent );
        }

        return TimeSeries.of( this.getReferenceDatetime(),
                              sortedEvents );
    }



    /**
     * Read the first value for a given variable name attribute from the netCDF
     * files.
     * @param variableName The NWM variable name.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    String readAttributeAsString( String variableName, String attributeName )
    {
        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            Variable variableVariable =  netcdfFile.findVariable( variableName );
            return readAttributeAsString( variableVariable, attributeName );
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + variableName + " in netCDF data." );
    }



    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    private String readAttributeAsString( Variable ncVariable, String attributeName )
    {
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                return attribute.getStringValue();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + ncVariable + " in netCDF data." );
    }



    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    private double readAttributeAsDouble( Variable ncVariable, String attributeName )
    {
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                return (double) attribute.getNumericValue();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + ncVariable + " in netCDF data." );
    }


    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    private int readAttributeAsInt( Variable ncVariable, String attributeName )
    {
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                return (int) attribute.getNumericValue();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + ncVariable + " in netCDF data." );
    }



    /**
     * Read a TimeSeries from across several netCDF single-validdatetime files.
     * @param featureId The NWM feature ID.
     * @param variableName The NWM variable name.
     * @return a TimeSeries containing the events.
     */

    TimeSeries<Double> readTimeSeries( int featureId, String variableName )
    {
        SortedSet<Event<Double>> events = new TreeSet<>();

        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            Event<Double> event = readDouble( netcdfFile, featureId, variableName );
            events.add( event );
        }

        return TimeSeries.of( this.getReferenceDatetime(),
                              ReferenceTimeType.T0,
                              events );
    }


    Event<Double> readDouble( NetcdfFile netcdfFile, int featureId, String variableName )
    {
        final int NOT_FOUND = -1;

        // Get the valid datetime
        String validDatetimeVariableName = this.getProfile().getValidDatetimeVariable();
        Instant validDatetime = this.readMinutesFromEpoch( netcdfFile,
                                                           validDatetimeVariableName );

        // Get the reference datetime
        String referenceDatetimeAttributeName = getProfile().getReferenceDatetimeVariable();
        Instant ncReferenceDatetime = this.readMinutesFromEpoch( netcdfFile,
                                                                 referenceDatetimeAttributeName );

        // Validate: this referenceDatetime should match what was set originally.
        // (This doesn't work for analysis_assim)
        if ( !ncReferenceDatetime.equals( this.getReferenceDatetime() ) )
        {
            throw new PreIngestException( "The reference datetime "
                                          + ncReferenceDatetime
                                          + " from netCDF file "
                                          + netcdfFile
                                          + " does not match expected value "
                                          + this.getReferenceDatetime() );
        }

        // Get the value at the variable in question.
        String featureVariableName = getProfile().getFeatureVariable();
        Variable featureVariable = netcdfFile.findVariable( featureVariableName );

        int indexOfFeature = NOT_FOUND;

        // Must find the location of the variable.
        // Might be nice to assume these are sorted to do a binary search,
        // but I think it is an unsafe assumption that the values are sorted
        // and we are looking for the index of the feature id to use for
        // getting a value from the actual variable needed.
        try
        {
            Array allFeatures = featureVariable.read();
            int[] rawFeatures = (int[]) allFeatures.get1DJavaArray( DataType.INT );

            for ( int i = 0; i < rawFeatures.length; i++ )
            {
                if ( rawFeatures[i] == featureId )
                {
                    indexOfFeature = i;
                    break;
                }
            }
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read features from "
                                          + netcdfFile, ioe );
        }

        if ( indexOfFeature == NOT_FOUND )
        {
            throw new PreIngestException( "Could not find feature id "
                                          + featureId + " in netCDF file "
                                          + netcdfFile );
        }

        Variable variableVariable =  netcdfFile.findVariable( variableName );
        int[] origin = { indexOfFeature };
        int[] shape = { 1 };
        int rawVariableValue;

        try
        {
            Array array = variableVariable.read( origin, shape );
            int[] values = (int[]) array.get1DJavaArray( DataType.INT );

            if ( values.length != 1 )
            {
                throw new PreIngestException( "Expected to read exactly one value, instead got "
                                              + values.length );
            }

            rawVariableValue = values[0];
        }
        catch ( IOException | InvalidRangeException e )
        {
            throw new PreIngestException( "Failed to read variable "
                                          + variableVariable
                                          + " at origin "
                                          + Arrays.toString( origin )
                                          + " and shape "
                                          + Arrays.toString( shape )
                                          + " from netCDF file " + netcdfFile,
                                          e );
        }

        double variableValue;

        int missingValue = readAttributeAsInt( variableVariable,
                                               "missing_value" );
        int fillValue = readAttributeAsInt( variableVariable,
                                            "_FillValue");
        double multiplier = readAttributeAsDouble( variableVariable,
                                                   "scale_factor" );

        if ( rawVariableValue == missingValue
             || rawVariableValue == fillValue )
        {
            variableValue = MissingValues.DOUBLE;
        }
        else
        {
            variableValue = rawVariableValue * multiplier;
        }

        return Event.of( validDatetime, variableValue );
    }


    /**
     * Helper to read minutes from epoch into an Instant
     * @param netcdfFile the (open) netCDF file to read from
     * @param variableName the name of the variable to read
     *                     assumes cardinality 1
     *                     assumes the value is an int
     *                     assumes the value is minutes since unix epoch
     * @return the Instant representation of the value
     */

    private Instant readMinutesFromEpoch( NetcdfFile netcdfFile,
                                          String variableName )
    {
        Variable ncVariable = netcdfFile.findVariable( variableName );

        try
        {
            Array allValidDateTimes = ncVariable.read();
            int minutesSinceEpoch = allValidDateTimes.getInt( 0 );
            Duration durationSinceEpoch = Duration.ofMinutes( minutesSinceEpoch );
            return Instant.ofEpochSecond( durationSinceEpoch.toSeconds() );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read Instant for variable "
                                          + ncVariable
                                          + " from netCDF file "
                                          + netcdfFile );
        }
    }

    public void close()
    {
        for ( NetcdfFile netcdfFile : this.netcdfFiles )
        {
            try
            {
                netcdfFile.close();
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Could not close netCDF file {}", netcdfFile, ioe );
            }
        }
    }
}
