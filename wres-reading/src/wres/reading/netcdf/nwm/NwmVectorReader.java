package wres.reading.netcdf.nwm;

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
import java.util.stream.Stream;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.SourceInterface;
import wres.datamodel.types.Ensemble;
import wres.datamodel.time.TimeSeries;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Reads forecasts and simulations/analyses from the National Water Model (NWM) in a Netcdf vector format.
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
    private static final String DATES_ERROR_MESSAGE = "One must declare 'reference_dates' with both a 'minimum' and a "
                                                      + "'maximum' when using NWM forecast data as a source. The "
                                                      + "'minimum' datetime must be a valid reference time for the NWM "
                                                      + "data specified, or no data will be found. One must declare "
                                                      + "'valid_dates' with both 'minimum' and 'maximum' when using "
                                                      + "NWM analysis data as a source, which will be used to find "
                                                      + "data falling within those valid datetimes.";

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final EvaluationDeclaration declaration;

    /**
     * @param declaration the declaration, required
     * @return an instance
     * @throws NullPointerException if the declaration is null
     */

    public static NwmVectorReader of( EvaluationDeclaration declaration )
    {
        return new NwmVectorReader( declaration );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        // Validate
        this.validateDataSource( dataSource );

        // Get the feature blocks
        List<SortedSet<Long>> featureBlocks = this.getFeatureBlocks( dataSource, this.getDeclaration() );

        // Get the reference times
        Set<Instant> referenceTimes = this.getReferenceTimes( dataSource, this.getDeclaration() );

        LOGGER.debug( "Chunking the time-series for reading by these feature blocks: {} and these reference times: {}.",
                      featureBlocks,
                      referenceTimes );

        // Read the time-series
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource,
                                                                         referenceTimes,
                                                                         featureBlocks );

        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     .onClose( () -> LOGGER.debug( "Detected a stream close event." ) );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( stream );

        LOGGER.warn( "Streaming of Netcdf vector time-series data is not currently supported. Attempting to read "
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
        Objects.requireNonNull( dataSource.getSource().sourceInterface() );
        Objects.requireNonNull( dataSource.getSource().uri() );
        Objects.requireNonNull( dataSource.getContext() );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.NETCDF_VECTOR );

        // Validate that the source contains a readable file or directory, unless it has an http scheme (e.g., d-store)
        if ( !ReaderUtilities.isWebSource( dataSource ) )
        {
            ReaderUtilities.validateFileSource( dataSource, true );
        }

        // Could be an NPE, but the data source is not null and the nullity of the variable is an effect, not a cause
        if ( Objects.isNull( dataSource.getVariable() ) )
        {
            DatasetOrientation lrb = dataSource.getDatasetOrientation();

            throw new ReadException( "A variable must be declared for an NWM source but no "
                                     + "variable was found for the "
                                     + lrb
                                     + " NWM source with URI: "
                                     + dataSource.getUri()
                                     + ". Please declare a variable for all "
                                     + lrb
                                     + " NWM sources." );
        }

        SourceInterface interfaceShortHand = dataSource.getSource()
                                                       .sourceInterface();

        NwmProfile nwmProfile = NwmProfiles.getProfileFromShortHand( interfaceShortHand );

        if ( nwmProfile.getTimeLabel()
                       .equals( NwmProfile.TimeLabel.F ) )
        {
            Objects.requireNonNull( this.getDeclaration()
                                        .referenceDates(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getDeclaration()
                                        .referenceDates()
                                        .minimum(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getDeclaration()
                                        .referenceDates()
                                        .maximum(),
                                    DATES_ERROR_MESSAGE );
        }
        else if ( nwmProfile.getTimeLabel()
                            .equals( NwmProfile.TimeLabel.TM ) )
        {
            Objects.requireNonNull( this.getDeclaration()
                                        .validDates(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getDeclaration()
                                        .validDates()
                                        .minimum(),
                                    DATES_ERROR_MESSAGE );
            Objects.requireNonNull( this.getDeclaration()
                                        .validDates()
                                        .maximum(),
                                    DATES_ERROR_MESSAGE );
        }
        else
        {
            throw new ReadException( "Unable to read NWM data with label "
                                     + nwmProfile.getTimeLabel() );
        }

        if ( !interfaceShortHand.isNwmInterface() )
        {
            throw new ReadException( "The data source passed does not appear to be a NWM source because the "
                                     + "'source_interface' value of '"
                                     + interfaceShortHand
                                     + "' was not recognized as a NWM source interface." );
        }

    }

    /**
     * @return the declaration
     */
    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param dataSource the data source
     * @param referenceTimes the reference times to read
     * @param featureBlocks the feature blocks to read
     * @return a time-series supplier
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             Set<Instant> referenceTimes,
                                                             List<SortedSet<Long>> featureBlocks )
    {
        SourceInterface interfaceShortHand = dataSource.getSource()
                                                       .sourceInterface();

        NwmProfile nwmProfile = NwmProfiles.getProfileFromShortHand( interfaceShortHand );
        Set<DataType> types = interfaceShortHand.getDataTypes();

        ReferenceTimeType referenceTimeType = DeclarationUtilities.getReferenceTimeType( types.iterator()
                                                                                              .next() );

        // Create the smaller suppliers, one per reference time
        List<Supplier<TimeSeriesTuple>> suppliers = this.getTimeSeriesSuppliers( nwmProfile,
                                                                                 dataSource,
                                                                                 referenceTimes,
                                                                                 referenceTimeType,
                                                                                 featureBlocks );

        // The current supplier
        AtomicReference<Supplier<TimeSeriesTuple>> supplier = new AtomicReference<>();

        // Iterate the suppliers
        return () -> {

            // Cached supply?
            if ( Objects.nonNull( supplier.get() ) )
            {
                Supplier<TimeSeriesTuple> nextSupplier = supplier.get();
                TimeSeriesTuple nextSupply = nextSupplier.get();

                if ( Objects.nonNull( nextSupply ) )
                {
                    return nextSupply;
                }
                else
                {
                    supplier.set( null );
                }
            }

            // New supplier available
            while ( !suppliers.isEmpty() )
            {
                Supplier<TimeSeriesTuple> nextSupplier = supplier.get();

                // No supplier cached? Get a new one
                if ( Objects.isNull( nextSupplier ) )
                {
                    Supplier<TimeSeriesTuple> cachedSupplier = suppliers.remove( 0 );
                    supplier.set( cachedSupplier );
                    nextSupplier = cachedSupplier;
                }

                TimeSeriesTuple nextSupply = nextSupplier.get();

                // Is there a time-series to return?
                if ( Objects.nonNull( nextSupply ) )
                {
                    return nextSupply;
                }
                else
                {
                    supplier.set( null );
                }
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * @param nwmProfile the NWM profile or data shape
     * @param dataSource the data source
     * @param referenceTimes the reference times to read
     * @param referenceTimeType the type of reference time
     * @param featureBlocks the feature blocks to read
     * @return the time-series suppliers
     */

    private List<Supplier<TimeSeriesTuple>> getTimeSeriesSuppliers( NwmProfile nwmProfile,
                                                                    DataSource dataSource,
                                                                    Set<Instant> referenceTimes,
                                                                    ReferenceTimeType referenceTimeType,
                                                                    List<SortedSet<Long>> featureBlocks )
    {
        // Create the smaller suppliers, one per reference time
        List<Supplier<TimeSeriesTuple>> suppliers = new ArrayList<>();

        // There is one set of time-series blobs per reference time
        for ( Instant nextReferenceTime : referenceTimes )
        {
            Supplier<TimeSeriesTuple> nextSupplier = this.getTimeSeriesSupplier( nwmProfile,
                                                                                 dataSource,
                                                                                 nextReferenceTime,
                                                                                 referenceTimeType,
                                                                                 featureBlocks );
            suppliers.add( nextSupplier );
        }

        // Mutable list
        return suppliers;
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param nwmProfile the NWM profile or data shape
     * @param dataSource the data source
     * @param referenceTime the reference time to read
     * @param referenceTimeType the type of reference time
     * @param featureBlocks the feature blocks to read
     * @return a time-series supplier
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( NwmProfile nwmProfile,
                                                             DataSource dataSource,
                                                             Instant referenceTime,
                                                             ReferenceTimeType referenceTimeType,
                                                             List<SortedSet<Long>> featureBlocks )
    {
        List<TimeSeriesTuple> cachedSeries = new ArrayList<>();
        List<SortedSet<Long>> mutableFeatureBlocks = new ArrayList<>( featureBlocks );
        AtomicReference<NwmTimeSeries> nwmTimeSeries = new AtomicReference<>();

        // Create a supplier that returns a time-series once complete
        // Since many time-series are read at once, they are cached for return, preferentially
        return () -> {

            LOGGER.debug( "Entered the time-series supplier for reference time {}.", referenceTime );

            // Cached series to return?
            if ( !cachedSeries.isEmpty() )
            {
                LOGGER.debug( "Returning a time-series from the cache. There are {} time-series remaining in the "
                              + "cache.",
                              cachedSeries.size() - 1 );

                TimeSeriesTuple next = cachedSeries.remove( 0 );

                this.closeNwmTimeSeriesIfCacheIsEmpty( cachedSeries, mutableFeatureBlocks, nwmTimeSeries );

                return next;
            }

            while ( !mutableFeatureBlocks.isEmpty() )
            {
                NwmTimeSeries currentTimeSeries = nwmTimeSeries.get();
                SortedSet<Long> nextFeatureBlock = mutableFeatureBlocks.remove( 0 );

                // No blobs open? Create a new opener and expose it for future iterations.
                if ( Objects.isNull( currentTimeSeries ) )
                {
                    currentTimeSeries = new NwmTimeSeries( nwmProfile,
                                                           referenceTime,
                                                           referenceTimeType,
                                                           dataSource.getUri() );

                    LOGGER.debug( "Opened {}.", currentTimeSeries );

                    nwmTimeSeries.set( currentTimeSeries );
                }

                List<TimeSeriesTuple> nextSeries = this.getTimeSeries( dataSource,
                                                                       nextFeatureBlock,
                                                                       currentTimeSeries,
                                                                       nwmProfile );
                cachedSeries.addAll( nextSeries );

                // Is there a time-series to return?
                if ( !cachedSeries.isEmpty() )
                {
                    LOGGER.debug( "Returning a time-series from the cache. There are {} time-series remaining in the "
                                  + "cache.",
                                  cachedSeries.size() - 1 );

                    TimeSeriesTuple next = cachedSeries.remove( 0 );

                    if ( mutableFeatureBlocks.isEmpty() )
                    {
                        this.closeNwmTimeSeriesIfCacheIsEmpty( cachedSeries, mutableFeatureBlocks, nwmTimeSeries );
                    }

                    return next;
                }
            }

            // Close the time-series
            this.closeTimeSeries( nwmTimeSeries );

            LOGGER.debug( "Exiting the time-series supplier for reference time {}.", referenceTime );

            // Null sentinel
            return null;
        };
    }

    /**
     * Closes the supplied time-series if needed.
     * @param nwmTimeSeries the time-series
     */
    private void closeTimeSeries( AtomicReference<NwmTimeSeries> nwmTimeSeries )
    {
        // Close the time-series
        if ( Objects.nonNull( nwmTimeSeries.get() ) )
        {
            nwmTimeSeries.get()
                         .close();

            LOGGER.debug( "Closed {}.", nwmTimeSeries.get() );

            nwmTimeSeries.set( null );
        }
    }

    /**
     * @param cachedSeries the cache to inspect
     * @param featureBlocks the feature blocks
     * @param nwmTimeSeries the time-series to close if the cache is empty and there are no feature blocks remaining
     */

    private void closeNwmTimeSeriesIfCacheIsEmpty( List<TimeSeriesTuple> cachedSeries,
                                                   List<SortedSet<Long>> featureBlocks,
                                                   AtomicReference<NwmTimeSeries> nwmTimeSeries )
    {
        if ( cachedSeries.isEmpty() && featureBlocks.isEmpty() && Objects.nonNull( nwmTimeSeries.get() ) )
        {
            nwmTimeSeries.get()
                         .close();

            LOGGER.debug( "Closed {} because there are no cached time-series remaining and no feature blocks remaining "
                          + "for which to retrieve new time-series.",
                          nwmTimeSeries.get() );
        }
    }

    /**
     * Reads a NWM time-series for one reference time and multiple geospatial chunks from one or more underlying Netcdf
     * blobs.
     *
     * @param dataSource the data source
     * @param featureBlock the feature block
     * @param nwmTimeSeries the NWM time-series
     * @param nwmProfile the NWM profile
     * @return the time-series
     */

    private List<TimeSeriesTuple> getTimeSeries( DataSource dataSource,
                                                 SortedSet<Long> featureBlock,
                                                 NwmTimeSeries nwmTimeSeries,
                                                 NwmProfile nwmProfile )
    {
        if ( nwmTimeSeries.countOfNetcdfFiles() <= 0 )
        {
            LOGGER.debug( "Found an empty NWM time-series. Skipping it. The series is: {}.", nwmTimeSeries );

            return Collections.emptyList();
        }

        String variableName = dataSource.getVariable()
                                        .name()
                                        .strip();
        String unitName = nwmTimeSeries.readAttributeAsString( variableName,
                                                               "units" );

        long[] featureBlockInts = featureBlock.stream()
                                              .mapToLong( Long::longValue )
                                              .toArray();

        try
        {
            // Single-valued series
            if ( nwmProfile.getMemberCount() == 1 )
            {
                Map<Long, TimeSeries<Double>> values =
                        nwmTimeSeries.readSingleValuedTimeSerieses( featureBlockInts,
                                                                    variableName,
                                                                    unitName );

                return values.values()
                             .stream()
                             .map( next -> ReaderUtilities.validateAgainstEmptyTimeSeries( next, dataSource.getUri() ) )
                             .map( next -> TimeSeriesTuple.ofSingleValued( next, dataSource ) )
                             .toList();
            }
            // Ensemble series
            else if ( nwmProfile.getMemberCount() > 1 )
            {
                Map<Long, TimeSeries<Ensemble>> values =
                        nwmTimeSeries.readEnsembleTimeSerieses( featureBlockInts,
                                                                variableName,
                                                                unitName );
                return values.values()
                             .stream()
                             .map( next -> ReaderUtilities.validateAgainstEmptyTimeSeries( next, dataSource.getUri() ) )
                             .map( next -> TimeSeriesTuple.ofEnsemble( next, dataSource ) )
                             .toList();
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
     * @param declaration the declaration
     * @return the feature block chunks
     */

    private List<SortedSet<Long>> getFeatureBlocks( DataSource dataSource, EvaluationDeclaration declaration )
    {
        // Get the feature set
        Set<GeometryTuple> geometries = DeclarationUtilities.getFeatures( declaration );
        Set<String> features = ReaderUtilities.getFeatureNamesFor( geometries, dataSource );

        // A list of featureIds that will be sorted in NWM id order to be used
        // to create blocks of sequential NWM ids.
        List<Long> featureNwmIds = new ArrayList<>( features.size() );

        for ( String feature : features )
        {
            try
            {
                Long id = Long.parseUnsignedLong( feature );
                featureNwmIds.add( id );
            }
            catch ( NumberFormatException nfe )
            {
                LOGGER.warn( "Skipping non-long NWM feature ID '{}' due to: {}",
                             feature,
                             nfe.getMessage() );
            }
        }

        // Sort in natural order
        featureNwmIds.sort( null );

        LOGGER.debug( "Sorted featureNwmIds: {}", featureNwmIds );

        List<List<Long>> tempListOfLists = ListUtils.partition( featureNwmIds, FEATURE_BLOCK_SIZE );
        List<SortedSet<Long>> featureBlocks = tempListOfLists.stream()
                .map( t -> Collections.unmodifiableSortedSet( new TreeSet<>( t ) ) )
                .toList();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Feature blocks: {}", featureBlocks );
        }

        return featureBlocks;
    }

    /**
     * Returns the reference times from the inputs.
     * @param dataSource the data source
     * @param declaration the declaration
     * @return the reference times
     */

    private Set<Instant> getReferenceTimes( DataSource dataSource, EvaluationDeclaration declaration )
    {

        SourceInterface interfaceShortHand = dataSource.getSource()
                                                       .sourceInterface();

        NwmProfile nwmProfile = NwmProfiles.getProfileFromShortHand( interfaceShortHand );

        // When we have analysis data, extend out the reference datetimes in
        // order to include data by valid datetime, kind of like observations.
        Instant earliest;
        Instant latest;
        if ( nwmProfile.getTimeLabel()
                       .equals( NwmProfile.TimeLabel.F ) )
        {
            earliest = declaration.referenceDates()
                                  .minimum();
            latest = declaration.referenceDates()
                                .maximum();
        }
        else if ( nwmProfile.getTimeLabel()
                            .equals( NwmProfile.TimeLabel.TM ) )
        {
            Instant earliestValidDatetime = declaration.validDates()
                                                       .minimum();
            Instant latestValidDatetime = declaration.validDates()
                                                     .maximum();
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
     * times within given bounds. Useful for reading analysis data based on the valid times rather than on reference
     * time ranges.
     *
     * @param nwmProfile A profile to use to calculate the new ranges.
     * @param earliestValidTime The earliest valid time to include.
     * @param latestValidTime The latest valid time to include.
     * @return The boundaries to be used to filter by reference times.
     */
    private Pair<Instant, Instant> getReferenceBoundsByValidBounds( NwmProfile nwmProfile,
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
        if ( nwmProfile.getTimeLabel().equals( NwmProfile.TimeLabel.F ) )
        {
            earliestReferenceDatetime = earliestValidTime.minus( toExtend );
        }
        else if ( nwmProfile.getTimeLabel().equals( NwmProfile.TimeLabel.TM ) )
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
     * Get the reference datetimes for the given boundaries and {@link NwmProfile}. As of 2020-08-04, there are
     * datasets that have reference datetimes one duration after T00Z but another duration between reference datetimes.
     * The profile now distinguishes between these two.
     *
     * @param earliest The earliest reference datetime.
     * @param latest The latest reference datetime.
     * @param nwmProfile The NwmProfile to use.
     * @return The Set of reference datetimes.
     */

    private Set<Instant> getReferenceTimes( Instant earliest,
                                            Instant latest,
                                            NwmProfile nwmProfile )
    {
        // Earliest and latest are the same? #99039
        if ( earliest.equals( latest ) )
        {
            LOGGER.debug( "Discovered that the earliest and latest reference times are the same: {}.", earliest );
            return Set.of( earliest );
        }

        Set<Instant> datetimes = new HashSet<>();
        Duration issuedStep = nwmProfile.getDurationBetweenReferenceDatetimes();

        // Simply truncate earliest to be at time 0 for the earliest
        // day, that should be a good starting point for finding the first 
        // NWM forecast reference strictly after the provided earliest Instant.
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
     * @param declaration the optional pair declaration, which is used to perform chunking of a data source
     * @throws NullPointerException if the declaration is null
     * @throws DeclarationException if the declaration is invalid
     */

    private NwmVectorReader( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        this.declaration = declaration;

        // Verify as much as possible upfront, i.e., either dates or issued dates and both components defined
        if ( Objects.isNull( declaration.validDates() ) && Objects.isNull( declaration.referenceDates() ) )
        {
            throw new DeclarationException( WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                            + "either the 'valid_dates' or 'reference_dates'." );
        }

        if ( Objects.nonNull( declaration.validDates() )
             && ( Objects.isNull( declaration.validDates()
                                             .minimum() )
                  || Objects.isNull( declaration.validDates()
                                                .maximum() ) ) )
        {
            throw new DeclarationException( WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                            + "both the 'minimum' and 'maximum' for the 'valid_dates'." );
        }

        if ( Objects.nonNull( declaration.referenceDates() )
             && ( Objects.isNull( declaration.referenceDates()
                                             .minimum() )
                  || Objects.isNull( declaration.referenceDates()
                                                .maximum() ) ) )
        {
            throw new DeclarationException( WHEN_READING_TIME_SERIES_DATA_FROM_THE_NATIONAL_WATER_MODEL_YOU_MUST_DECLARE
                                            + "both the 'minimum' and 'maximum' for the 'reference_dates'." );
        }

        LOGGER.debug( "When building a reader for NWM time-series data from the WRDS, received a complete project "
                      + "declaration, which will be used to chunk requests by feature and time range." );
    }

}

