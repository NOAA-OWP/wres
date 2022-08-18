package wres.io.reading.nwm;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesTuple;
import wres.io.config.ConfigHelper;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Reads forecasts and simulations/analyses from the National Water Model (NWM) in a NetCDF vector format.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class NwmVectorReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwmVectorReader.class );

    /** Message used repeatedly. */
    private static final String WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE =
            "When reading time-series data from the National Water Model, you must declare";

    /** Number of features per feature block/chunk. */
    private static final int FEATURE_BLOCK_SIZE = 100;

    /** Dates error message used repeatedly. */
    private static final String DATES_ERROR_MESSAGE =
            "One must specify issued datetimes with both earliest and latest "
                                                      + "(e.g. <issuedDates earliest=\"2019-10-25T00:00:00Z\" "
                                                      + "latest=\"2019-11-25T00:00:00Z\" />) when using NWM forecast "
                                                      + "data as a source, which will be interpreted as reference "
                                                      + "datetimes. The earliest datetime specified must be a valid "
                                                      + "reference time for the NWM data specified, or no data will be "
                                                      + "found. One must specify valid datetimes with both earliest "
                                                      + "and latest (e.g. <dates earliest=\"2019-12-11T00:00:00Z\" "
                                                      + "latest=\"2019-12-12T00:00:00Z\" />) when using NWM analysis "
                                                      + "data as a source, which will be used to find data falling "
                                                      + "within those valid datetimes.";

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final PairConfig pairConfig;

    /**
     * @param pairConfig the pair declaration, required
     * @return an instance
     * @throws NullPointerException if the pairConfig is null
     */

    public static NwmVectorReader of( PairConfig pairConfig )
    {
        return new NwmVectorReader( pairConfig );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        // Validate
        this.validateDataSource( dataSource );

        // Get the feature blocks
        List<List<Integer>> featureBlocks = this.getFeatureBlocks( dataSource, this.getPairConfig() );

        // Get the reference times
        Set<Instant> referenceTimes = this.getReferenceTimes( dataSource, this.getPairConfig() );

        LOGGER.debug( "Chunking the time-series for reading by these feature blocks: {} and these reference times: {}.",
                      featureBlocks,
                      referenceTimes );

        // Read the time-series
        List<NWMTimeSeries> resourcesToClose = new ArrayList<>();
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource,
                                                                         referenceTimes,
                                                                         featureBlocks,
                                                                         resourcesToClose );

        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing all underlying resources." );
                         resourcesToClose.forEach( NWMTimeSeries::close );
                     } );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( stream );

        LOGGER.warn( "Streaming of NetCDF vector time-series data is not currently supported. Attempting to read "
                     + "directly from the data source supplied, {}.",
                     dataSource );

        return this.read( dataSource );
    }

    /**
     * Validates the data source for required elements.
     * 
     * @throws NullPointerException if a required element of the source is missing
     * @throws ReadException if the data source is invalid
     */

    private void validateDataSource( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getSource() );
        Objects.requireNonNull( dataSource.getSource().getInterface() );
        Objects.requireNonNull( dataSource.getSource().getValue() );
        Objects.requireNonNull( dataSource.getContext() );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.NETCDF_VECTOR );
        
        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource );
        
        // Could be an NPE, but the data source is not null and the nullity of the variable is an effect, not a cause
        if ( Objects.isNull( dataSource.getVariable() ) )
        {
            LeftOrRightOrBaseline lrb = dataSource.getLeftOrRightOrBaseline();

            throw new ReadException( "A variable must be declared for an NWM source but no "
                                     + "variable was found for the "
                                     + lrb
                                     + " NWM source with URI: "
                                     + dataSource.getUri()
                                     + ". Please declare a variable for all "
                                     + lrb
                                     + " NWM sources." );
        }

        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();

        NWMProfile nwmProfile = NWMProfiles.getProfileFromShortHand( interfaceShortHand );

        if ( nwmProfile.getTimeLabel()
                       .equals( NWMProfile.TimeLabel.f ) )
        {
            Objects.requireNonNull( this.getPairConfig()
                                        .getIssuedDates(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getPairConfig()
                                        .getIssuedDates()
                                        .getEarliest(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getPairConfig()
                                        .getIssuedDates()
                                        .getLatest(),
                                    DATES_ERROR_MESSAGE );
        }
        else if ( nwmProfile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.tm ) )
        {
            Objects.requireNonNull( this.getPairConfig()
                                        .getDates(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getPairConfig()
                                        .getDates()
                                        .getEarliest(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getPairConfig()
                                        .getDates()
                                        .getLatest(),
                                    DATES_ERROR_MESSAGE );
        }
        else
        {
            throw new ReadException( "Unable to read NWM data with label "
                                     + nwmProfile.getTimeLabel() );
        }

        if ( !interfaceShortHand.toString()
                                .toLowerCase()
                                .startsWith( "nwm_" ) )
        {
            throw new ReadException( "The data source passed does "
                                     + "not appear to be a NWM "
                                     + "source because the interface"
                                     + " shorthand "
                                     + interfaceShortHand
                                     + " did not start with 'nwm_'" );
        }

    }

    /**
     * @return the pair declaration
     */
    private PairConfig getPairConfig()
    {
        return this.pairConfig;
    }

    /**
     * Returns a time-series supplier from the inputs.
     * 
     * @param dataSource the data source
     * @param referenceTimes the reference times to read
     * @param featureBlocks the feature blocks to read
     * @param an empty list to populate with resources opened by this method that must be closed on stream close
     * @return a time-series supplier
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             Set<Instant> referenceTimes,
                                                             List<List<Integer>> featureBlocks,
                                                             List<NWMTimeSeries> resourcesToClose )
    {
        SortedSet<Instant> mutableReferenceTimes = new TreeSet<>( referenceTimes );
        List<List<Integer>> mutableFeatureBlocks = new ArrayList<>( featureBlocks );
        List<TimeSeriesTuple> cachedSeries = new ArrayList<>();

        // An underlying resource of NWM time-series that is opened once per reference time
        AtomicReference<NWMTimeSeries> nwmTimeSeries = new AtomicReference<>();

        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();

        NWMProfile nwmProfile = NWMProfiles.getProfileFromShortHand( interfaceShortHand );

        ReferenceTimeType referenceTimeType = ConfigHelper.getReferenceTimeType( dataSource.getContext()
                                                                                           .getType() );

        // Create a supplier that returns a time-series once complete
        // Since many time-series are read at once, they are cached for return, preferentially
        return () -> {

            // Cached series to return?
            if ( !cachedSeries.isEmpty() )
            {
                LOGGER.debug( "Returning a time-series from the cache. There are {} time-series remaining in the cache.",
                              cachedSeries.size() - 1 );

                return cachedSeries.remove( 0 );
            }

            // Clean up before sending the null sentinel, which terminates the stream
            // New rows to increment
            while ( !mutableReferenceTimes.isEmpty() )
            {
                List<Integer> nextFeatures = mutableFeatureBlocks.get( 0 );
                mutableFeatureBlocks.remove( 0 );

                Instant nextReferenceTime = mutableReferenceTimes.first();

                NWMTimeSeries currentTimeSeries = nwmTimeSeries.get();

                // No blobs open? Create a new opener and expose it for future iterations.
                if ( Objects.isNull( currentTimeSeries ) )
                {
                    currentTimeSeries = new NWMTimeSeries( nwmProfile,
                                                           nextReferenceTime,
                                                           referenceTimeType,
                                                           dataSource.getUri() );
                    nwmTimeSeries.set( currentTimeSeries );
                    resourcesToClose.add( currentTimeSeries );
                }

                // Create the next series and add them to the cache
                List<TimeSeriesTuple> nextSeries = this.getTimeSeries( dataSource,
                                                                       nextFeatures,
                                                                       currentTimeSeries,
                                                                       nwmProfile );
                cachedSeries.addAll( nextSeries );

                // We're at the start of a new reference time on the next iteration, so remove this reference time,
                // reset the feature blocks and close the last set of blobs
                if ( mutableFeatureBlocks.isEmpty() )
                {
                    mutableFeatureBlocks.addAll( featureBlocks );
                    mutableReferenceTimes.remove( nextReferenceTime );
                    nwmTimeSeries.set( null );

                    // This is ultimately closed in the stream close, but try to close early
                    currentTimeSeries.close();
                }

                // Is there a time-series to return?
                if ( !cachedSeries.isEmpty() )
                {
                    return cachedSeries.remove( 0 );
                }
            }

            // This is ultimately closed in the stream close, but try to close early
            if ( Objects.nonNull( nwmTimeSeries.get() ) )
            {
                nwmTimeSeries.get()
                             .close();
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * Reads a NWM time-series for one reference time and multiple geospatial chunks from one or more underlying NetCDF 
     * blobs.
     * 
     * @param dataSource the data source
     * @param featureBlocks the feature blocks
     * @param nwmTimeSeries the NWM time-series
     * @param nwmProfile the NWM profile
     * @return the time-series
     */

    private List<TimeSeriesTuple> getTimeSeries( DataSource dataSource,
                                                 List<Integer> featureBlock,
                                                 NWMTimeSeries nwmTimeSeries,
                                                 NWMProfile nwmProfile )
    {
        String variableName = dataSource.getVariable()
                                        .getValue()
                                        .strip();
        String unitName = nwmTimeSeries.readAttributeAsString( variableName,
                                                               "units" );

        int[] featureBlockInts = featureBlock.stream()
                                             .mapToInt( Integer::intValue )
                                             .toArray();

        try
        {
            // Single-valued series
            if ( nwmProfile.getMemberCount() == 1 )
            {
                Map<Integer, TimeSeries<Double>> values =
                        nwmTimeSeries.readSingleValuedTimeSerieses( featureBlockInts,
                                                                    variableName,
                                                                    unitName );

                return values.values()
                             .stream()
                             .map( TimeSeriesTuple::ofSingleValued )
                             .collect( Collectors.toUnmodifiableList() );
            }
            // Ensemble series
            else if ( nwmProfile.getMemberCount() > 1 )
            {
                Map<Integer, TimeSeries<Ensemble>> values =
                        nwmTimeSeries.readEnsembleTimeSerieses( featureBlockInts,
                                                                variableName,
                                                                unitName );
                return values.values()
                             .stream()
                             .map( TimeSeriesTuple::ofEnsemble )
                             .collect( Collectors.toUnmodifiableList() );
            }
            else
            {
                throw new ReadException( "Cannot read a timeseries with "
                                         + nwmProfile.getMemberCount()
                                         + " members." );
            }
        }
        catch ( ExecutionException e )
        {
            throw new ReadException( "Failed to read an NWM time-series from an underlying blob collection.", e );
        }
        catch ( InterruptedException e )
        {
            // Play nice
            Thread.currentThread()
                  .interrupt();

            throw new ReadException( "Failed to read an NWM time-series from an underlying blob collection.", e );
        }
    }

    /**
     * Returns the feature block chunks from the inputs.
     * @param dataSource the data source
     * @param pairConfig the pair declaration
     * @return the feature block chunks
     */

    private List<List<Integer>> getFeatureBlocks( DataSource dataSource, PairConfig pairConfig )
    {
        // Get the feature set
        Set<String> features = ConfigHelper.getFeatureNamesForSource( pairConfig,
                                                                      dataSource.getContext(),
                                                                      dataSource.getLeftOrRightOrBaseline() );

        // A list of featureIds that will be sorted in NWM id order to be used
        // to create blocks of sequential NWM ids.
        List<Integer> featureNwmIds = new ArrayList<>( features.size() );

        for ( String feature : features )
        {
            try
            {
                Integer id = Integer.parseUnsignedInt( feature );
                featureNwmIds.add( id );
            }
            catch ( NumberFormatException nfe )
            {
                LOGGER.warn( "Skipping non-integer NWM feature ID {} due to {}",
                             feature,
                             nfe.getMessage() );
            }
        }

        // Sort in natural order
        featureNwmIds.sort( null );

        LOGGER.debug( "Sorted featureNwmIds: {}", featureNwmIds );

        List<List<Integer>> featureBlocks = ListUtils.partition( featureNwmIds, FEATURE_BLOCK_SIZE );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Feature blocks: {}", featureBlocks );
        }

        return Collections.unmodifiableList( featureBlocks );
    }

    /**
     * Returns the reference times from the inputs.
     * @param dataSource the data source
     * @param pairConfig the pair declaration
     * @return the reference times
     */

    private Set<Instant> getReferenceTimes( DataSource dataSource, PairConfig pairConfig )
    {

        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();

        NWMProfile nwmProfile = NWMProfiles.getProfileFromShortHand( interfaceShortHand );

        // When we have analysis data, extend out the reference datetimes in
        // order to include data by valid datetime, kind of like observations.
        Instant earliest;
        Instant latest;
        if ( nwmProfile.getTimeLabel()
                       .equals( NWMProfile.TimeLabel.f ) )
        {
            earliest = Instant.parse( pairConfig.getIssuedDates()
                                                .getEarliest() );
            latest = Instant.parse( pairConfig.getIssuedDates()
                                              .getLatest() );
        }
        else if ( nwmProfile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.tm ) )
        {
            Instant earliestValidDatetime =
                    Instant.parse( pairConfig.getDates()
                                             .getEarliest() );
            Instant latestValidDatetime =
                    Instant.parse( pairConfig.getDates()
                                             .getLatest() );
            Pair<Instant, Instant> referenceBounds =
                    this.getReferenceBoundsByValidBounds( nwmProfile,
                                                          earliestValidDatetime,
                                                          latestValidDatetime );
            earliest = referenceBounds.getLeft();
            latest = referenceBounds.getRight();
        }
        else
        {
            throw new ReadException( "Unsupported label encountered when determining reference times: "
                                     + nwmProfile.getTimeLabel()
                                     + "." );
        }

        return this.getReferenceTimes( earliest, latest, nwmProfile );
    }

    /**
     * Creates the values needed to call getReferenceDatetimes when wanting to ingest all the possible data with valid 
     * times within given bounds.
     *
     * Useful for reading analysis data based on the valid times rather than on reference time ranges.
     *
     * @param nwmProfile A profile to use to calculate the new ranges.
     * @param earliestValidTime The earliest valid time to include.
     * @param latestValidTime The latest valid time to include.
     * @return The boundaries to be used to filter by reference times.
     */
    private Pair<Instant, Instant> getReferenceBoundsByValidBounds( NWMProfile nwmProfile,
                                                                    Instant earliestValidTime,
                                                                    Instant latestValidTime )
    {
        Instant earliestReferenceDatetime = earliestValidTime;
        Instant latestReferenceDatetime = latestValidTime;
        Duration betweenBlobs = nwmProfile.getDurationBetweenValidDatetimes();
        int maxBlobsAwayFromReference = nwmProfile.getBlobCount();

        // Figure the furthest away from the reference datetime based on count.
        Duration toExtend = betweenBlobs.multipliedBy( maxBlobsAwayFromReference );

        // When the time label is "f", assume the blobs extend into future,
        // therefore we need to include earlier reference datetimes to get
        // all the valid datetimes.
        if ( nwmProfile.getTimeLabel().equals( NWMProfile.TimeLabel.f ) )
        {
            earliestReferenceDatetime = earliestValidTime.minus( toExtend );
        }
        else if ( nwmProfile.getTimeLabel().equals( NWMProfile.TimeLabel.tm ) )
        {
            // When the time label is "tm", assume the blobs extend into past,
            // therefore we need to include later reference datetimes to get
            // all the valid datetimes.
            latestReferenceDatetime = latestValidTime.plus( toExtend );
        }
        else
        {
            // In case another time label arrives, tolerate it by extending
            // in both directions, past and future, ingesting more data but
            // having a better chance of including all the desired data.
            earliestReferenceDatetime = earliestValidTime.minus( toExtend );
            latestReferenceDatetime = latestValidTime.plus( toExtend );
        }

        return Pair.of( earliestReferenceDatetime, latestReferenceDatetime );
    }

    /**
     * Get the reference datetimes for the given boundaries and {@link NWMProfile}.
     *
     * As of 2020-08-04, there are datasets that have reference datetimes one
     * duration after T00Z but another duration between reference datetimes.
     * The profile now distinguishes between these two.
     *
     * @param earliest The earliest reference datetime.
     * @param latest The latest reference datetime.
     * @param nwmProfile The NWMProfile to use.
     * @return The Set of reference datetimes.
     */

    private Set<Instant> getReferenceTimes( Instant earliest,
                                            Instant latest,
                                            NWMProfile nwmProfile )
    {

        Set<Instant> datetimes = new HashSet<>();
        Duration issuedStep = nwmProfile.getDurationBetweenReferenceDatetimes();

        //Simply truncate earliest to be at time 0 for the earliest
        //day, that should be a good starting point for finding the first 
        //NWM forecast reference strictly after the provided earliest Instant.
        // Then add the duration past midnight of the first forecast.
        Instant forecastDatetime = earliest.truncatedTo( ChronoUnit.DAYS )
                                           .plus( nwmProfile.getDurationPastMidnight() );

        while ( !forecastDatetime.isAfter( earliest ) )
        {
            forecastDatetime = forecastDatetime.plus( issuedStep );
        }

        LOGGER.debug( "Resolved earliest {} issued datetime given to first forecast at {}",
                      earliest,
                      forecastDatetime );
        datetimes.add( forecastDatetime );
        Instant additionalForecastDatetime = forecastDatetime.plus( issuedStep );

        while ( !additionalForecastDatetime.isAfter( latest ) )
        {
            datetimes.add( additionalForecastDatetime );
            additionalForecastDatetime = additionalForecastDatetime.plus( issuedStep );
        }

        return Collections.unmodifiableSet( datetimes );
    }

    /**
     * Hidden constructor.
     * @param pairConfig the optional pair declaration, which is used to perform chunking of a data source
     * @throws ProjectConfigException if the project declaration is invalid for this source type
     * @throws NullPointerException if either input is null
     */

    private NwmVectorReader( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig );

        this.pairConfig = pairConfig;

        // Verify as much as possible upfront, i.e., either dates or issued dates and both components defined
        if ( Objects.isNull( pairConfig.getDates() ) && Objects.isNull( pairConfig.getIssuedDates() ) )
        {
            throw new ProjectConfigException( pairConfig,
                                              WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                                          + "either the dates or issuedDates." );
        }

        if ( Objects.nonNull( pairConfig.getDates() ) && ( Objects.isNull( pairConfig.getDates().getEarliest() )
                                                           || Objects.isNull( pairConfig.getDates()
                                                                                        .getLatest() ) ) )
        {
            throw new ProjectConfigException( pairConfig,
                                              WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                                          + "both the earliest and latest dates (e.g. "
                                                          + "<dates earliest=\"2019-08-10T14:30:00Z\" "
                                                          + "latest=\"2019-08-15T18:00:00Z\" />)." );
        }

        if ( Objects.nonNull( pairConfig.getIssuedDates() )
             && ( Objects.isNull( pairConfig.getIssuedDates().getEarliest() )
                  || Objects.isNull( pairConfig.getIssuedDates()
                                               .getLatest() ) ) )
        {
            throw new ProjectConfigException( pairConfig,
                                              WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                                          + "both the earliest and latest issued dates (e.g. "
                                                          + "<issuedDates earliest=\"2019-08-10T14:30:00Z\" "
                                                          + "latest=\"2019-08-15T18:00:00Z\" />)." );
        }

        LOGGER.debug( "When building a reader for NWM time-series data from the WRDS, received a complete pair "
                      + "declaration, which will be used to chunk requests by feature and time range." );
    }

}
