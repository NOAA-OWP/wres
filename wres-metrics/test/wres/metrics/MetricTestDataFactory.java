package wres.metrics;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.Probability;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
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
     * Fourteenth time.
     */

    private static final String FOURTEENTH_TIME = "2551-03-19T09:00:00Z";

    /**
     * Thirteenth time.
     */

    private static final String THIRTEENTH_TIME = "2551-03-19T06:00:00Z";

    /**
     * Twelfth time.
     */

    private static final String TWELFTH_TIME = "2551-03-19T03:00:00Z";

    /**
     * Eleventh time.
     */

    private static final String ELEVENTH_TIME = "2551-03-19T00:00:00Z";

    /**
     * Tenth time.
     */

    private static final String TENTH_TIME = "2551-03-18T21:00:00Z";

    /**
     * Ninth time.
     */

    private static final String NINTH_TIME = "2551-03-18T18:00:00Z";

    /**
     * Eight time.
     */

    private static final String EIGHTH_TIME = "2551-03-18T15:00:00Z";

    /**
     * Seventh time.
     */

    private static final String SEVENTH_TIME = "2551-03-18T12:00:00Z";

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

    /**
     * @param t0 the T0 time
     * @return some metadata
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
     * Returns a set of single-valued pairs without a baseline.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsOne()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();

        pairs.add( Pair.of( 22.9, 22.8 ) );
        pairs.add( Pair.of( 75.2, 80.0 ) );
        pairs.add( Pair.of( 63.2, 65.0 ) );
        pairs.add( Pair.of( 29.0, 30.0 ) );
        pairs.add( Pair.of( 5.0, 2.0 ) );
        pairs.add( Pair.of( 2.1, 3.1 ) );
        pairs.add( Pair.of( 35000.0, 37000.0 ) );
        pairs.add( Pair.of( 8.0, 7.0 ) );
        pairs.add( Pair.of( 12.0, 12.0 ) );
        pairs.add( Pair.of( 93.0, 94.0 ) );

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        builder.addData( pairs )
               .setMetadata( PoolMetadata.of() );

        return builder.build();
    }

    /**
     * Returns a set of single-valued pairs with a baseline.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsTwo()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();

        pairs.add( Pair.of( 22.9, 22.8 ) );
        pairs.add( Pair.of( 75.2, 80.0 ) );
        pairs.add( Pair.of( 63.2, 65.0 ) );
        pairs.add( Pair.of( 29.0, 30.0 ) );
        pairs.add( Pair.of( 5.0, 2.0 ) );
        pairs.add( Pair.of( 2.1, 3.1 ) );
        pairs.add( Pair.of( 35000.0, 37000.0 ) );
        pairs.add( Pair.of( 8.0, 7.0 ) );
        pairs.add( Pair.of( 12.0, 12.0 ) );
        pairs.add( Pair.of( 93.0, 94.0 ) );

        List<Pair<Double, Double>> baseline = new ArrayList<>();
        baseline.add( Pair.of( 20.9, 23.8 ) );
        baseline.add( Pair.of( 71.2, 83.2 ) );
        baseline.add( Pair.of( 69.2, 66.0 ) );
        baseline.add( Pair.of( 20.0, 30.5 ) );
        baseline.add( Pair.of( 5.8, 2.1 ) );
        baseline.add( Pair.of( 1.1, 3.4 ) );
        baseline.add( Pair.of( 33020.0, 37500.0 ) );
        baseline.add( Pair.of( 8.8, 7.1 ) );
        baseline.add( Pair.of( 12.1, 13.0 ) );
        baseline.add( Pair.of( 93.2, 94.8 ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                          null,
                                          null,
                                          null,
                                          false );

        PoolMetadata main = PoolMetadata.of( evaluation, pool );

        Pool poolTwo = Pool.newBuilder().setIsBaselinePool( true ).build();

        PoolMetadata base = PoolMetadata.of( evaluation, poolTwo );

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( pairs )
                      .setMetadata( main )
                      .addDataForBaseline( baseline )
                      .setMetadataForBaseline( base )
                      .build();
    }

    /**
     * @param featureId the feature name
     * @param baseline is true to include a baseline
     * @return a singleton feature group containing the named feature
     */

    public static FeatureGroup getFeatureGroup( final String featureId, boolean baseline )
    {
        FeatureKey featureKey = new FeatureKey( featureId, null, null, null );
        
        if( baseline )
        {
            return FeatureGroup.of( new FeatureTuple( featureKey, featureKey, featureKey ) );
        }
        
        return FeatureGroup.of( new FeatureTuple( featureKey, featureKey, null ) );
    }

    /**
     * Returns a moderately-sized (10k) test dataset of single-valued pairs, {5,10}, without a baseline.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsThree()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();
        for ( int i = 0; i < 10000; i++ )
        {
            pairs.add( Pair.of( 5.0, 10.0 ) );
        }

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                          null,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( pairs )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * Returns a moderately-sized test dataset of single-valued pairs without a baseline. The data are partitioned by
     * observed values of {1,2,3,4,5} with 100-pair chunks and corresponding predicted values of {6,7,8,9,10}. The data
     * are returned with a nominal lead time of 1.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsFour()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();

        for ( int i = 0; i < 5; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                pairs.add( Pair.of( i + 1.0, i + 6.0 ) );
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

        Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( pairs )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * <p>Returns a small test dataset with predictions and corresponding observations from location "103.1" from
     * https://github.com/NVE/RunoffTestData:
     * 
     * https://github.com/NVE/RunoffTestData/blob/master/24h/qobs_calib/103.1.txt
     * https://github.com/NVE/RunoffTestData/blob/master/Example/calib_txt/103_1_station.txt
     * </p>
     * 
     * <p>The data are stored in:</p>
     *  
     * <p>testinput/metricTestDataFactory/getSingleValuedPairsFive.asc</p>
     * 
     * <p>The observations are in the first column and the predictions are in the second column.</p>
     * 
     * @return single-valued pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsFive() throws IOException
    {
        //Construct some pairs
        final List<Pair<Double, Double>> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getSingleValuedPairsFive.asc" );
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( Pair.of( doubleValues[0], doubleValues[1] ) );
            }
        }

        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ),
                                                           Duration.ofHours( 24 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "103.1", false );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "QME" )
                                          .setRightDataName( "NVE" )
                                          .setMeasurementUnit( MM_DAY )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        return wres.datamodel.pools.Pool.of( values, meta );
    }

    /**
     * Returns a set of single-valued pairs with a single pair and no baseline. This is useful for checking exceptional
     * behaviour due to an inadequate sample size.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsSix()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();

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

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( pairs )
                      .setMetadata( meta )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs with a baseline, both empty.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsSeven()
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

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( List.of() )
                      .setMetadata( main )
                      .addDataForBaseline( List.of() )
                      .setMetadataForBaseline( base )
                      .build();
    }

    /**
     * Returns a set of single-valued pairs without a baseline and with some missing values.
     * 
     * @return single-valued pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Double>> getSingleValuedPairsEight()
    {
        //Construct some single-valued pairs
        List<Pair<Double, Double>> pairs = new ArrayList<>();

        pairs.add( Pair.of( 22.9, 22.8 ) );
        pairs.add( Pair.of( 75.2, 80.0 ) );
        pairs.add( Pair.of( 63.2, 65.0 ) );
        pairs.add( Pair.of( 29.0, 30.0 ) );
        pairs.add( Pair.of( 5.0, 2.0 ) );
        pairs.add( Pair.of( 2.1, 3.1 ) );
        pairs.add( Pair.of( 35000.0, 37000.0 ) );
        pairs.add( Pair.of( 8.0, 7.0 ) );
        pairs.add( Pair.of( 12.0, 12.0 ) );
        pairs.add( Pair.of( 93.0, 94.0 ) );
        pairs.add( Pair.of( Double.NaN, 94.0 ) );
        pairs.add( Pair.of( 93.0, Double.NaN ) );
        pairs.add( Pair.of( Double.NaN, Double.NaN ) );

        wres.datamodel.pools.Pool.Builder<Pair<Double, Double>> builder = new wres.datamodel.pools.Pool.Builder<>();
        return builder.addData( pairs )
                      .setMetadata( PoolMetadata.of() )
                      .build();
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs with the same dataset as a baseline. Reads the pairs 
     * from testinput/metricTestDataFactory/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<Pair<Double, Ensemble>> getEnsemblePairsOne() throws IOException
    {
        wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> pool =
                MetricTestDataFactory.getTimeSeriesOfEnsemblePairsOne();

        return PoolSlicer.unpack( pool );
    }

    /**
     * Returns a set of dichotomous pairs based on http://www.cawcr.gov.au/projects/verification/#Contingency_table. The
     * test data comprises 83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365.
     * 
     * @return a set of dichotomous pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Boolean, Boolean>> getDichotomousPairsOne()
    {
        //Construct the dichotomous pairs using the example from http://www.cawcr.gov.au/projects/verification/#Contingency_table
        //83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365
        final List<Pair<Boolean, Boolean>> values = new ArrayList<>();
        //Hits
        for ( int i = 0; i < 82; i++ )
        {
            values.add( Pair.of( true, true ) );
        }
        //False alarms
        for ( int i = 82; i < 120; i++ )
        {
            values.add( Pair.of( false, true ) );
        }
        //Misses
        for ( int i = 120; i < 143; i++ )
        {
            values.add( Pair.of( true, false ) );
        }
        for ( int i = 144; i < 366; i++ )
        {
            values.add( Pair.of( false, false ) );
        }

        final PoolMetadata meta = Boilerplate.getPoolMetadata();
        return wres.datamodel.pools.Pool.of( values, meta ); //Construct the pairs
    }

    /**
     * Returns a set of multicategory pairs based on Table 4.2 in Joliffe and Stephenson (2012) Forecast Verification: A
     * Practitioner's Guide in Atmospheric Science. 2nd Ed. Wiley, Chichester.
     * 
     * @return a set of dichotomous pairs
     */

    public static wres.datamodel.pools.Pool<Pair<boolean[], boolean[]>> getMulticategoryPairsOne()
    {
        //Construct the multicategory pairs
        final List<Pair<boolean[], boolean[]>> values = new ArrayList<>();
        //(1,1)
        for ( int i = 0; i < 24; i++ )
        {
            values.add( Pair.of( new boolean[] { true, false, false },
                                 new boolean[] { true, false, false } ) );
        }
        //(1,2)
        for ( int i = 24; i < 87; i++ )
        {
            values.add( Pair.of( new boolean[] { false, true, false },
                                 new boolean[] { true, false, false } ) );
        }
        //(1,3)
        for ( int i = 87; i < 118; i++ )
        {
            values.add( Pair.of( new boolean[] { false, false, true },
                                 new boolean[] { true, false, false } ) );
        }
        //(2,1)
        for ( int i = 118; i < 181; i++ )
        {
            values.add( Pair.of( new boolean[] { true, false, false },
                                 new boolean[] { false, true, false } ) );
        }
        //(2,2)
        for ( int i = 181; i < 284; i++ )
        {
            values.add( Pair.of( new boolean[] { false, true, false },
                                 new boolean[] { false, true, false } ) );
        }
        //(2,3)
        for ( int i = 284; i < 426; i++ )
        {
            values.add( Pair.of( new boolean[] { false, false, true },
                                 new boolean[] { false, true, false } ) );
        }
        //(3,1)
        for ( int i = 426; i < 481; i++ )
        {
            values.add( Pair.of( new boolean[] { true, false, false },
                                 new boolean[] { false, false, true } ) );
        }
        //(3,2)
        for ( int i = 481; i < 591; i++ )
        {
            values.add( Pair.of( new boolean[] { false, true, false },
                                 new boolean[] { false, false, true } ) );
        }
        //(3,3)
        for ( int i = 591; i < 788; i++ )
        {
            values.add( Pair.of( new boolean[] { false, false, true },
                                 new boolean[] { false, false, true } ) );
        }

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( MetricTestDataFactory.getFeatureGroup( DRRC2, false ), null, null, null, false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        return wres.datamodel.pools.Pool.of( values, meta ); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pairs without a baseline.
     * 
     * @return discrete probability pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Probability, Probability>> getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 2.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 / 5.0 ) ) );

        final PoolMetadata meta = Boilerplate.getPoolMetadata();
        return wres.datamodel.pools.Pool.of( values, meta );
    }

    /**
     * Returns a set of discrete probability pairs with a baseline.
     * 
     * @return discrete probability pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Probability, Probability>> getDiscreteProbabilityPairsTwo()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 2.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1.0 / 5.0 ) ) );
        final List<Pair<Probability, Probability>> baseline = new ArrayList<>();
        baseline.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        baseline.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        baseline.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        baseline.add( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) );
        baseline.add( Pair.of( Probability.ZERO, Probability.of( 4.0 / 5.0 ) ) );
        baseline.add( Pair.of( Probability.ONE, Probability.of( 1.0 / 5.0 ) ) );
        final PoolMetadata main = Boilerplate.getPoolMetadata();
        final PoolMetadata base = Boilerplate.getPoolMetadata();
        return wres.datamodel.pools.Pool.of( values, main, baseline, base, null );
    }

    /**
     * <p>
     * Returns a set of discrete probability pairs that comprises probability of precipitation forecasts from the
     * Finnish Meteorological Institute for a 24h lead time, and corresponding observations, available here:
     * </p>
     * <p>
     * http://www.cawcr.gov.au/projects/verification/POP3/POP_3cat_2003.txt
     * </p>
     * 
     * @return a set of discrete probability pairs
     */

    public static wres.datamodel.pools.Pool<Pair<Probability, Probability>> getDiscreteProbabilityPairsThree()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final List<Pair<Probability, Probability>> values = new ArrayList<>();

        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.5 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.2 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.4 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.6 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.7 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.3 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.8 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 1 ) ) );
        values.add( Pair.of( Probability.ONE, Probability.of( 0.9 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.1 ) ) );

        final PoolMetadata main = Boilerplate.getPoolMetadata();
        return wres.datamodel.pools.Pool.of( values, main );
    }

    /**
     * Returns a set of discrete probability pairs without a baseline and comprising observed non-occurrences only.
     * 
     * @return discrete probability pairs with observed non-occurrences
     */

    public static wres.datamodel.pools.Pool<Pair<Probability, Probability>> getDiscreteProbabilityPairsFour()
    {
        final List<Pair<Probability, Probability>> values = new ArrayList<>();
        values.add( Pair.of( Probability.ZERO, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 3.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        values.add( Pair.of( Probability.ZERO, Probability.of( 1.0 / 5.0 ) ) );

        final PoolMetadata meta = Boilerplate.getPoolMetadata();
        return wres.datamodel.pools.Pool.of( values, meta );
    }

    /**
     * Returns a {@link PoolOf} with single-valued pairs containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsOne()
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

        // Add another time-series
        Instant secondId = Instant.parse( THIRD_TIME );
        SortedSet<Event<Pair<Double, Double>>> secondValues = new TreeSet<>();

        // Add some values
        secondValues.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 10.0, 1.0 ) ) );
        secondValues.add( Event.of( Instant.parse( FIFTH_TIME ), Pair.of( 1.0, 1.0 ) ) );
        secondValues.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 1.0, 10.0 ) ) );

        // Create some default metadata for the time-series
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( THIRD_TIME ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "A", false );

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
                      .addData( TimeSeries.of( getBoilerplateMetadataWithT0( secondId ),
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

        FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

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
     * Returns a {@link Pool} with single-valued pairs containing fake data with the same peak at multiple times.
     * 
     * @return a time-series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsFive()
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
        secondValues.add( Event.of( Instant.parse( SIXTH_TIME ), Pair.of( 10.0, 10.0 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), Pair.of( 2.0, 10.0 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-03T06:00:00Z" ), Pair.of( 4.0, 7.0 ) ) );

        // Create some default metadata for the time-series
        final TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( THIRD_TIME ),
                                                           Instant.parse( THIRD_TIME ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 30 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "A", false );

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

        return builder.addData( TimeSeries.of( getBoilerplateMetadataWithT0( secondId ),
                                               secondValues ) )
                      .setMetadata( metaData )
                      .build();
    }

    /**
     * Returns a list of {@link Pool} with single-valued pairs which correspond to the pairs 
     * associated with system test scenario504 as of commit e91b36a8f6b798d1987e78a0f37b38f3ca4501ae.
     * The pairs are reproduced to 2 d.p. only.
     * 
     * @return a time series of single-valued pairs
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Double>>> getTimeSeriesOfSingleValuedPairsNine()
    {
        Builder<TimeSeries<Pair<Double, Double>>> tsBuilder = new Builder<>();

        // Add the first time-series
        Instant basisTimeOne = Instant.parse( "2551-03-17T12:00:00Z" );
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();

        first.add( Event.of( Instant.parse( "2551-03-17T15:00:00Z" ),
                             Pair.of( 409.67, 73.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-17T18:00:00Z" ),
                             Pair.of( 428.33, 79.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-17T21:00:00Z" ),
                             Pair.of( 443.67, 83.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-18T00:00:00Z" ),
                             Pair.of( 460.33, 89.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ),
                             Pair.of( 477.67, 97.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ),
                             Pair.of( 497.67, 101.00 ) ) );
        first.add( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ),
                             Pair.of( 517.67, 103.00 ) ) );
        first.add( Event.of( Instant.parse( SEVENTH_TIME ),
                             Pair.of( 548.33, 107.00 ) ) );
        first.add( Event.of( Instant.parse( EIGHTH_TIME ),
                             Pair.of( 567.67, 109.00 ) ) );
        first.add( Event.of( Instant.parse( NINTH_TIME ),
                             Pair.of( 585.67, 113.00 ) ) );
        first.add( Event.of( Instant.parse( TENTH_TIME ),
                             Pair.of( 602.33, 127.00 ) ) );

        // Add second time-series
        Instant basisTimeTwo = Instant.parse( "2551-03-18T00:00:00Z" );
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();

        second.add( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ),
                              Pair.of( 477.67, 131.00 ) ) );
        second.add( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ),
                              Pair.of( 497.67, 137.00 ) ) );
        second.add( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ),
                              Pair.of( 517.67, 139.00 ) ) );
        second.add( Event.of( Instant.parse( SEVENTH_TIME ),
                              Pair.of( 548.33, 149.00 ) ) );
        second.add( Event.of( Instant.parse( EIGHTH_TIME ),
                              Pair.of( 567.67, 151.00 ) ) );
        second.add( Event.of( Instant.parse( NINTH_TIME ),
                              Pair.of( 585.67, 157.00 ) ) );
        second.add( Event.of( Instant.parse( TENTH_TIME ),
                              Pair.of( 602.33, 163.00 ) ) );
        second.add( Event.of( Instant.parse( ELEVENTH_TIME ),
                              Pair.of( 616.33, 167.00 ) ) );
        second.add( Event.of( Instant.parse( TWELFTH_TIME ),
                              Pair.of( 638.33, 173.00 ) ) );
        second.add( Event.of( Instant.parse( THIRTEENTH_TIME ),
                              Pair.of( 653.00, 179.00 ) ) );
        second.add( Event.of( Instant.parse( FOURTEENTH_TIME ),
                              Pair.of( 670.33, 181.00 ) ) );

        // Add third time-series
        Instant basisTimeThree = Instant.parse( SEVENTH_TIME );
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();

        third.add( Event.of( Instant.parse( EIGHTH_TIME ),
                             Pair.of( 567.67, 191.00 ) ) );
        third.add( Event.of( Instant.parse( NINTH_TIME ),
                             Pair.of( 585.67, 193.00 ) ) );
        third.add( Event.of( Instant.parse( TENTH_TIME ),
                             Pair.of( 602.33, 197.00 ) ) );
        third.add( Event.of( Instant.parse( ELEVENTH_TIME ),
                             Pair.of( 616.33, 199.00 ) ) );
        third.add( Event.of( Instant.parse( TWELFTH_TIME ),
                             Pair.of( 638.33, 211.00 ) ) );
        third.add( Event.of( Instant.parse( THIRTEENTH_TIME ),
                             Pair.of( 653.00, 223.00 ) ) );
        third.add( Event.of( Instant.parse( FOURTEENTH_TIME ),
                             Pair.of( 670.33, 227.00 ) ) );
        third.add( Event.of( Instant.parse( "2551-03-19T12:00:00Z" ),
                             Pair.of( 691.67, 229.00 ) ) );
        third.add( Event.of( Instant.parse( "2551-03-19T15:00:00Z" ),
                             Pair.of( 718.33, 233.00 ) ) );
        third.add( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ),
                             Pair.of( 738.33, 239.00 ) ) );
        third.add( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ),
                             Pair.of( 756.33, 241.00 ) ) );

        // Add third time-series
        Instant basisTimeFour = Instant.parse( ELEVENTH_TIME );
        SortedSet<Event<Pair<Double, Double>>> fourth = new TreeSet<>();

        fourth.add( Event.of( Instant.parse( TWELFTH_TIME ),
                              Pair.of( 638.33, 251.00 ) ) );
        fourth.add( Event.of( Instant.parse( THIRTEENTH_TIME ),
                              Pair.of( 653.00, 257.00 ) ) );
        fourth.add( Event.of( Instant.parse( FOURTEENTH_TIME ),
                              Pair.of( 670.33, 263.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-19T12:00:00Z" ),
                              Pair.of( 691.67, 269.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-19T15:00:00Z" ),
                              Pair.of( 718.33, 271.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ),
                              Pair.of( 738.33, 277.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ),
                              Pair.of( 756.33, 281.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-20T00:00:00Z" ),
                              Pair.of( 776.33, 283.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-20T03:00:00Z" ),
                              Pair.of( 805.67, 293.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-20T06:00:00Z" ),
                              Pair.of( 823.67, 307.00 ) ) );
        fourth.add( Event.of( Instant.parse( "2551-03-20T09:00:00Z" ),
                              Pair.of( 840.33, 311.00 ) ) );

        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                     Instant.parse( "2551-03-20T00:00:00Z" ),
                                                     Duration.ofSeconds( 10800 ),
                                                     Duration.ofSeconds( 118800 ) );

        FeatureGroup featureGroup = MetricTestDataFactory.getFeatureGroup( "FAKE2", false );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "DISCHARGE" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( featureGroup,
                                          window,
                                          null,
                                          null,
                                          false );

        PoolMetadata metaData = PoolMetadata.of( evaluation, pool );

        tsBuilder.setMetadata( metaData );

        return tsBuilder.addData( TimeSeries.of( getBoilerplateMetadataWithT0( basisTimeOne ), first ) )
                        .addData( TimeSeries.of( getBoilerplateMetadataWithT0( basisTimeTwo ), second ) )
                        .addData( TimeSeries.of( getBoilerplateMetadataWithT0( basisTimeThree ), third ) )
                        .addData( TimeSeries.of( getBoilerplateMetadataWithT0( basisTimeFour ), fourth ) )
                        .build();
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs with the same dataset as a baseline. Reads the pairs 
     * from testinput/metricTestDataFactory/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static wres.datamodel.pools.Pool<TimeSeries<Pair<Double, Ensemble>>> getTimeSeriesOfEnsemblePairsOne()
            throws IOException
    {
        //Construct some ensemble pairs
        SortedSet<Event<Pair<Double, Ensemble>>> values = new TreeSet<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsOne.asc" );
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

        FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

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
     * Hidden constructor
     */
    private MetricTestDataFactory()
    {
    }

}