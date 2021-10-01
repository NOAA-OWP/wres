package wres.pipeline.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.Pool.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author James Brown
 */
public final class MetricTestDataFactory
{

    /**
     * Streamflow for metadata.
     */
    private static final String STREAMFLOW = "Streamflow";

    /**
     * Units for metadata.
     */

    private static final String MM_DAY = "MM/DAY";

    /**
     * Location for metadata.
     */

    private static final String DRRC2 = "DRRC2";

    /**
     * Sixth time.
     */

    private static final String SIXTH_TIME = "1985-01-02T18:00:00Z";

    /**
     * Fifth time.
     */

    private static final String FIFTH_TIME = "1985-01-02T12:00:00Z";

    /**
     * Fourth time.
     */

    private static final String FOURTH_TIME = "1985-01-02T06:00:00Z";

    /**
     * Third time.
     */

    private static final String THIRD_TIME = "1985-01-02T00:00:00Z";

    /**
     * Second time.
     */

    private static final String SECOND_TIME = "2010-12-31T11:59:59Z";

    /**
     * First time.
     */

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /** Variable name for boilerplate metadata */
    private static final String VARIABLE_NAME = "SQIN";

    /** Feature name for boilerplate metadata */
    private static final String FEATURE_NAME = DRRC2;

    /** Unit name for boilerplate metadata */
    private static final String UNIT = "CMS";

    private static final FeatureKey NWS_FEATURE = new FeatureKey( "DRRC2", null, null, null );
    private static final FeatureKey USGS_FEATURE = new FeatureKey( "09165000",
                                                                   "DOLORES RIVER BELOW RICO, CO.",
                                                                   4326,
                                                                   "POINT ( -108.0603517 37.63888428 )" );
    private static final FeatureKey NWM_FEATURE = new FeatureKey( "18384141", null, null, null );
    private static final FeatureTuple FEATURE_TUPLE = new FeatureTuple( USGS_FEATURE, NWS_FEATURE, NWM_FEATURE );
    private static final FeatureGroup FEATURE_GROUP = FeatureGroup.of( FEATURE_TUPLE );

    /**
     * @return a feature group
     */

    static FeatureGroup getFeatureGroup()
    {
        return FEATURE_GROUP;
    }

    /**
     * @return a feature tuple
     */

    static FeatureTuple getFeatureTuple()
    {
        return FEATURE_TUPLE;
    }

    /**
     * @return some pool metadata
     */

    static PoolMetadata getPoolMetadata()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( MetricTestDataFactory.FEATURE_GROUP,
                                          null,
                                          null,
                                          null,
                                          false );

        return PoolMetadata.of( evaluation, pool );
    }

    /**
     * @param featureId the feature name
     * @param baseline is true to include a baseline
     * @return a singleton feature group containing the named feature
     */

    static FeatureGroup getFeatureGroup( final String featureId, boolean baseline )
    {
        FeatureKey featureKey = new FeatureKey( featureId, null, null, null );

        if ( baseline )
        {
            return FeatureGroup.of( new FeatureTuple( featureKey, featureKey, featureKey ) );
        }

        return FeatureGroup.of( new FeatureTuple( featureKey, featureKey, null ) );
    }

    /**
     * Returns a {@link Pool} with single-valued pairs containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsTwo()
    {
        // Build an immutable regular time-series of single-valued pairs
        Builder<TimeSeries<Pair<Double, Double>>> builder =
                new Builder<>();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( FIRST_TIME );
        SortedSet<Event<Pair<Double, Double>>> firstValues = new TreeSet<>();
        // Add some values
        firstValues.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), Pair.of( 1.0, 1.0 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T12:00:00Z" ), Pair.of( 1.0, 5.0 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T18:00:00Z" ), Pair.of( 5.0, 1.0 ) ) );

        // Create some default metadata for the time-series
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( FIRST_TIME ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata metaData = PoolMetadata.of( evaluation, pool );

        // Build the time-series
        return builder.addData( TimeSeries.of( getBoilerplateMetadataWithT0( firstId ),
                                               firstValues ) )
                      .setMetadata( metaData )
                      .build();
    }

    /**
     * Returns a {@link Pool} with single-valued pairs containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsThree()
    {
        // Build an immutable regular time-series of single-valued pairs
        Builder<TimeSeries<Pair<Double, Double>>> builder =
                new Builder<>();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Add a time-series
        Instant secondId = Instant.parse( THIRD_TIME );
        SortedSet<Event<Pair<Double, Double>>> secondValues = new TreeSet<>();

        // Add some values
        secondValues.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 10.0, 1.0 ) ) );
        secondValues.add( Event.of( Instant.parse( FIFTH_TIME ), Pair.of( 1.0, 1.0 ) ) );
        secondValues.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 1.0, 10.0 ) ) );

        // Create some default metadata for the time-series
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( THIRD_TIME ),
                                                           Instant.parse( THIRD_TIME ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata metaData = PoolMetadata.of( evaluation, pool );

        // Build the time-series
        return builder.addData( TimeSeries.of( getBoilerplateMetadataWithT0( secondId ),
                                               secondValues ) )
                      .setMetadata( metaData )
                      .build();
    }

    /**
     * Returns a {@link Pool} with single-valued pairs containing no data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsFour()
    {
        // Build an immutable regular time-series of single-valued pairs
        Builder<TimeSeries<Pair<Double, Double>>> builder =
                new Builder<>();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Create some default metadata for the time-series
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                           Instant.MAX );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata metaData = PoolMetadata.of( evaluation, pool );

        // Build the time-series
        return builder.setMetadata( metaData ).build();
    }

    /**
     * Returns a moderately-sized test dataset of single-valued pairs without a baseline. The data are partitioned by
     * observed values of {1,2,3,4,5} with 100-pair chunks and corresponding predicted values of {6,7,8,9,10}. The data
     * are returned with a nominal lead time of 1.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsSix()
    {
        //Construct some single-valued pairs
        SortedSet<Event<Pair<Double, Double>>> events = new TreeSet<>();

        Instant start = Instant.parse( "1985-01-01T00:00:00Z" );

        int time = 0;
        for ( int i = 0; i < 5; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                events.add( Event.of( start.plus( Duration.ofHours( time ) ), Pair.of( i + 1.0, i + 6.0 ) ) );

                time++;
            }
        }

        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ),
                                                           Duration.ofHours( 1 ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( MetricTestDataFactory.getFeatureGroup(),
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Builder<TimeSeries<Pair<Double, Double>>> builder = new Builder<>();
        return builder.addData( TimeSeries.of( MetricTestDataFactory.getBoilerplateMetadata(),
                                               events ) )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs with a baseline, both empty.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsSeven()
    {
        //Construct some single-valued pairs
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        PoolMetadata main = PoolMetadata.of( evaluation, Pool.getDefaultInstance() );
        Pool pool = Pool.newBuilder()
                        .setIsBaselinePool( true )
                        .build();

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( "SQIN" )
                                             .setRightDataName( "ESP" )
                                             .setMeasurementUnit( "CMS" )
                                             .build();

        PoolMetadata base = PoolMetadata.of( evaluationTwo, pool );

        Builder<TimeSeries<Pair<Double, Double>>> builder = new Builder<>();
        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               Collections.emptySortedSet() ) )
                      .setMetadata( main )
                      .addDataForBaseline( TimeSeries.of( getBoilerplateMetadata(),
                                                          Collections.emptySortedSet() ) )
                      .setMetadataForBaseline( base )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs without a baseline and with some missing values.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsEight()
    {
        //Construct some single-valued pairs
        SortedSet<Event<Pair<Double, Double>>> events = new TreeSet<>();

        Instant start = Instant.parse( "1985-01-01T00:00:00Z" );

        events.add( Event.of( start.plus( Duration.ofHours( 1 ) ), Pair.of( 22.9, 22.8 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 2 ) ), Pair.of( 75.2, 80.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 3 ) ), Pair.of( 63.2, 65.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 4 ) ), Pair.of( 29.0, 30.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 5 ) ), Pair.of( 5.0, 2.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 6 ) ), Pair.of( 2.1, 3.1 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 7 ) ), Pair.of( 35000.0, 37000.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 8 ) ), Pair.of( 8.0, 7.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 9 ) ), Pair.of( 12.0, 12.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 10 ) ), Pair.of( 93.0, 94.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 11 ) ), Pair.of( Double.NaN, 94.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 12 ) ), Pair.of( 93.0, Double.NaN ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 13 ) ), Pair.of( Double.NaN, Double.NaN ) ) );

        Builder<TimeSeries<Pair<Double, Double>>> builder = new Builder<>();

        TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ZERO );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "AHPS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          timeWindow,
                                          null,
                                          null,
                                          false );
        return builder.addData( TimeSeries.of( MetricTestDataFactory.getBoilerplateMetadata(),
                                               events ) )
                      .setMetadata( PoolMetadata.of( evaluation, pool ) )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs with a single pair and no baseline. This is useful for checking exceptional
     * behavior due to an inadequate sample size.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsTen()
    {
        //Construct some single-valued pairs
        SortedSet<Event<Pair<Double, Double>>> events = new TreeSet<>();

        List<Event<Pair<Double, Double>>> values = new ArrayList<>();
        values.add( Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), Pair.of( 22.9, 22.8 ) ) );
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                     Instant.parse( SECOND_TIME ),
                                                     Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "A", false );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( MM_DAY )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Builder<TimeSeries<Pair<Double, Double>>> builder = new Builder<>();
        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               events ) )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs without a baseline and with some missing values.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsEleven()
    {
        //Construct some single-valued pairs
        SortedSet<Event<Pair<Double, Double>>> events = new TreeSet<>();

        Instant start = Instant.parse( "1985-01-01T00:00:00Z" );

        events.add( Event.of( start.plus( Duration.ofHours( 1 ) ), Pair.of( 22.9, 22.8 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 2 ) ), Pair.of( 75.2, 80.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 3 ) ), Pair.of( 63.2, 65.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 4 ) ), Pair.of( 29.0, 30.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 5 ) ), Pair.of( 5.0, 2.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 6 ) ), Pair.of( 2.1, 3.1 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 7 ) ), Pair.of( 35000.0, 37000.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 8 ) ), Pair.of( 8.0, 7.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 9 ) ), Pair.of( 12.0, 12.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 10 ) ), Pair.of( 93.0, 94.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 11 ) ), Pair.of( Double.NaN, 94.0 ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 12 ) ), Pair.of( 93.0, Double.NaN ) ) );
        events.add( Event.of( start.plus( Duration.ofHours( 13 ) ), Pair.of( Double.NaN, Double.NaN ) ) );

        Builder<TimeSeries<Pair<Double, Double>>> builder = new Builder<>();

        TimeWindowOuter timeWindow = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ZERO );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "AHPS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        FeatureGroup groupOne = MetricTestDataFactory.getFeatureGroup( DRRC2, false );
        FeatureGroup groupTwo = MetricTestDataFactory.getFeatureGroup( "DRRC3", false );
        Set<FeatureTuple> features = new HashSet<>();
        features.addAll( groupOne.getFeatures() );
        features.addAll( groupTwo.getFeatures() );
        FeatureGroup featureGroup = FeatureGroup.of( features );

        Pool pool = MessageFactory.parse( featureGroup,
                                          timeWindow,
                                          null,
                                          null,
                                          false );
        return builder.addData( TimeSeries.of( MetricTestDataFactory.getBoilerplateMetadata(),
                                               events ) )
                      .setMetadata( PoolMetadata.of( evaluation, pool ) )
                      .build();
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs with the same dataset as a baseline. Reads the pairs 
     * from testinput/sharedinput/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getTimeSeriesOfEnsemblePairsOne()
            throws IOException
    {
        //Construct some ensemble pairs
        SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        File file = new File( "testinput/sharedinput/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {

            Instant time = Instant.parse( "1981-12-01T00:00:00Z" );
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( Event.of( time,
                                      Pair.of( doubleValues[0],
                                               Ensemble.of( Arrays.copyOfRange( doubleValues,
                                                                                1,
                                                                                doubleValues.length ) ) ) ) );
                climatology.add( doubleValues[0] );
                time = time.plus( Duration.ofHours( 1 ) );
            }
        }

        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                     Instant.parse( SECOND_TIME ),
                                                     Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Pool poolTwo = MessageFactory.parse( featureGroup,
                                             window,
                                             null,
                                             null,
                                             true );

        PoolMetadata baseMeta = PoolMetadata.of( evaluation, poolTwo );

        VectorOfDoubles clim = VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) );

        Builder<TimeSeries<Pair<Double, Ensemble>>> builder = new Builder<>();

        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               values ) )
                      .setMetadata( meta )
                      .addDataForBaseline( TimeSeries.of( getBoilerplateMetadata(),
                                                          values ) )
                      .setMetadataForBaseline( baseMeta )
                      .setClimatology( clim )
                      .build();
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs without a baseline. Reads the pairs from
     * testinput/sharedinput/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours. Adds some
     * missing values to the dataset, namely {@link Double#NaN}.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>>
            getTimeSeriesOfEnsemblePairsOneWithMissings() throws IOException
    {
        //Construct some ensemble pairs
        final SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        File file = new File( "testinput/sharedinput/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();

        Instant time = Instant.parse( "1981-12-01T00:00:00Z" );

        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( Event.of( time,
                                      Pair.of( doubleValues[0],
                                               Ensemble.of( Arrays.copyOfRange( doubleValues,
                                                                                1,
                                                                                doubleValues.length ) ) ) ) );
                climatology.add( doubleValues[0] );
                time = time.plus( Duration.ofHours( 1 ) );
            }
        }
        //Add some missing values
        climatology.add( Double.NaN );
        values.add( Event.of( time.plus( Duration.ofHours( 1 ) ),
                              Pair.of( Double.NaN,
                                       Ensemble.of( Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN ) ) ) );

        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                     Instant.parse( SECOND_TIME ),
                                                     Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Pool poolTwo = MessageFactory.parse( featureGroup,
                                             window,
                                             null,
                                             null,
                                             true );

        PoolMetadata baseMeta = PoolMetadata.of( evaluation, poolTwo );

        VectorOfDoubles clim = VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) );

        Builder<TimeSeries<Pair<Double, Ensemble>>> builder = new Builder<>();

        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               values ) )
                      .setMetadata( meta )
                      .addDataForBaseline( TimeSeries.of( getBoilerplateMetadata(),
                                                          values ) )
                      .setMetadataForBaseline( baseMeta )
                      .setClimatology( clim )
                      .build();
    }

    /**
     * Returns a small test dataset of ensemble pairs without a baseline. Reads the pairs from
     * testinput/sharedinput/getEnsemblePairsTwo.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getTimeSeriesOfEnsemblePairsTwo()
            throws IOException
    {
        //Construct some ensemble pairs
        final SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        File file = new File( "testinput/sharedinput/getEnsemblePairsTwo.asc" );
        List<Double> climatology = new ArrayList<>();

        Instant time = Instant.parse( "1981-12-01T00:00:00Z" );

        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( Event.of( time,
                                      Pair.of( doubleValues[0],
                                               Ensemble.of( Arrays.copyOfRange( doubleValues,
                                                                                1,
                                                                                doubleValues.length ) ) ) ) );
                climatology.add( doubleValues[0] );
                time = time.plus( Duration.ofHours( 1 ) );
            }
        }

        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ),
                                                           Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        VectorOfDoubles clim = VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) );

        Builder<TimeSeries<Pair<Double, Ensemble>>> builder = new Builder<>();

        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               values ) )
                      .setMetadata( meta )
                      .setClimatology( clim )
                      .build();
    }

    /**
     * Returns a set of ensemble pairs with a single pair and no baseline. 
     * 
     * @return ensemble pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getTimeSeriesOfEnsemblePairsThree()
    {
        SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();
        values.add( Event.of( Instant.parse( "1985-03-12T00:00:00Z" ), Pair.of( 22.9, Ensemble.of( 22.8, 23.9 ) ) ) );

        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                     Instant.parse( SECOND_TIME ),
                                                     Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( DRRC2, false );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( MM_DAY )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Builder<TimeSeries<Pair<Double, Ensemble>>> builder = new Builder<>();

        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               values ) )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * Returns a set of ensemble pairs with no data in the main input or baseline. 
     * 
     * @return ensemble pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getTimeSeriesOfEnsemblePairsFour()
    {
        //Construct some ensemble pairs
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ),
                                                           Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "MAP" )
                                          .setMeasurementUnit( MM_DAY )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Pool basePool = MessageFactory.parse( featureGroup,
                                              window,
                                              null,
                                              null,
                                              true );

        PoolMetadata base = PoolMetadata.of( evaluation, basePool );

        Builder<TimeSeries<Pair<Double, Ensemble>>> builder = new Builder<>();

        return builder.addData( TimeSeries.of( getBoilerplateMetadata(),
                                               Collections.emptySortedSet() ) )
                      .setMetadata( meta )
                      .addDataForBaseline( TimeSeries.of( getBoilerplateMetadata(),
                                                          Collections.emptySortedSet() ) )
                      .setMetadataForBaseline( base )
                      .build();
    }

    /**
     * @param t0 the T0 time
     * @return the metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0( Instant t0 )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FeatureKey.of( FEATURE_NAME ),
                                      UNIT );
    }

    /**
     * @return some metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FeatureKey.of( FEATURE_NAME ),
                                      UNIT );
    }

    /**
     * Hidden constructor
     */
    private MetricTestDataFactory()
    {
    }

}
