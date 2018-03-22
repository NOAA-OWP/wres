package wres.datamodel;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.SafeMetricOutputMapByTimeAndThreshold.Builder;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * Tests the {@link SafeMetricOutputMapByTimeAndThreshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMetricOutputMapByTimeAndThresholdTest
{

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and slices the map, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByTimeAndFilterByThreshold()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 42 ) );
        final OneOrTwoThresholds q =
                OneOrTwoThresholds.of( outputFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 531.88 ),
                                                                  SafeOneOrTwoDoubles.of( 0.005 ),
                                                                  Operator.GREATER ) );
        final Pair<TimeWindow, OneOrTwoThresholds> testKeyOne = Pair.of( timeWindow, q );
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap =
                results.filterByTime( timeWindow ).filterByThreshold( q );

        //Check the results
        final double actualOne = subMap.get( testKeyOne ).getData();
        final double expectedOne = 0.026543876961751534;
        assertTrue( "Unexpected output from data slice [" + actualOne
                    + ","
                    + expectedOne
                    + "].",
                    Double.compare( actualOne, expectedOne ) == 0 );

        //Slice by threshold = 531.88
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap2 = results.filterByThreshold( q );

        //Acquire a submap by threshold = all data and lead time = 714
        final TimeWindow timeWindowTwo = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                        Instant.parse( "2010-12-31T11:59:59Z" ),
                                                        ReferenceTime.VALID_TIME,
                                                        Duration.ofHours( 714 ) );


        final OneOrTwoThresholds q2 =
                OneOrTwoThresholds.of( outputFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                  SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                  Operator.GREATER ) );
        final Pair<TimeWindow, OneOrTwoThresholds> testKeyTwo = Pair.of( timeWindowTwo, q2 );

        //Slice by threshold = all data
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap3 = results.filterByTime( timeWindowTwo )
                                                                                    .filterByThreshold( q2 );

        final double actualTwo = subMap3.get( testKeyTwo ).getData();
        final double expectedTwo = 0.005999378857020621;

        assertTrue( "Unexpected output from data slice [" + actualTwo
                    + ","
                    + expectedTwo
                    + "].",
                    Double.compare( actualTwo, expectedTwo ) == 0 );
        assertTrue( "Unexpected output size from data slice [" + subMap2.size()
                    + ","
                    + 29
                    + "].",
                    subMap2.size() == 29 );
        assertTrue( "Expected quantile thresholds in store.", subMap.hasQuantileThresholds() );

        //Filter by lead times directly        
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap4 =
                results.filterByLeadTime( timeWindow ).filterByThreshold( q );
        final double actualThree = subMap4.get( testKeyOne ).getData();
        final double expectedThree = expectedOne;
        assertTrue( "Unexpected output from data slice [" + actualThree
                    + ","
                    + expectedThree
                    + "].",
                    Double.compare( actualThree, expectedThree ) == 0 );
        //Check lead times
        Set<TimeWindow> benchmarkTimes = new TreeSet<>();
        for ( int i = 0; i < 29; i++ )
        {
            benchmarkTimes.add( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                               Instant.parse( "2010-12-31T11:59:59Z" ),
                                               ReferenceTime.VALID_TIME,
                                               Duration.ofHours( 42L + i * 24 ),
                                               Duration.ofHours( 42L + i * 24 ) ) );
        }
        assertTrue( "Unexpected lead times in dataset.",
                    results.setOfTimeWindowKey().equals( benchmarkTimes ) );

    }

    /**
     * Tests {@link SafeMetricOutputMapByTimeAndThreshold#setOfTimeWindowKeyByLeadTime()}.
     */

    @Test
    public void testSetOfTimeWindowKeyByLeadTime()
    {
        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();
        final MetricOutputMetadata meta = metaFac.getOutputMetadata( 1,
                                                                     metaFac.getDimension(),
                                                                     metaFac.getDimension(),
                                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                     MetricConstants.MAIN );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> testMap = new HashMap<>();
        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( outputFactory.ofThreshold( SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                             Instant.parse( "1985-01-02T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     outputFactory.ofDoubleScoreOutput( 1.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-03T00:00:00Z" ),
                                             Instant.parse( "1985-01-04T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     outputFactory.ofDoubleScoreOutput( 2.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-05T00:00:00Z" ),
                                             Instant.parse( "1985-01-06T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     outputFactory.ofDoubleScoreOutput( 3.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-07T00:00:00Z" ),
                                             Instant.parse( "1985-01-08T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     outputFactory.ofDoubleScoreOutput( 4.0, meta ) );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> test = outputFactory.ofMap( testMap );
        Set<TimeWindow> benchmarkTimes = new TreeSet<>();
        benchmarkTimes.add( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 1 ),
                                           Duration.ofHours( 2 ) ) );
        assertTrue( "Unexpected windows in filtered dataset.",
                    test.setOfTimeWindowKeyByLeadTime().equals( benchmarkTimes ) );
    }

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and tests the 
     * {@link SafeMetricOutputMapByTimeAndThreshold#setOfThresholdOne()} for equality against a benchmark.
     */

    @Test
    public void testSetOfThresholdOne()
    {
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        Set<Threshold> benchmark = new HashSet<>();
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 3588.66667 ),
                                                 SafeOneOrTwoDoubles.of( 0.6 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1996.4 ),
                                                 SafeOneOrTwoDoubles.of( 0.4 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 2707.5 ),
                                                 SafeOneOrTwoDoubles.of( 0.5 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 12641.14286 ),
                                                 SafeOneOrTwoDoubles.of( 0.94 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 11819.66667 ),
                                                 SafeOneOrTwoDoubles.of( 0.93 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 13685.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.95 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 4749.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.7 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 10944.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.92 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 858.04 ),
                                                 SafeOneOrTwoDoubles.of( 0.1 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 9647.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.9 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 560.26 ),
                                                 SafeOneOrTwoDoubles.of( 0.01 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 26648.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.99 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1513.25 ),
                                                 SafeOneOrTwoDoubles.of( 0.3 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 713.6 ),
                                                 SafeOneOrTwoDoubles.of( 0.05 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 6315.33333 ),
                                                 SafeOneOrTwoDoubles.of( 0.8 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 18122.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.97 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1147.05263 ),
                                                 SafeOneOrTwoDoubles.of( 0.2 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 15278.4 ),
                                                 SafeOneOrTwoDoubles.of( 0.96 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 22448.0 ),
                                                 SafeOneOrTwoDoubles.of( 0.98 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                 SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 531.88 ),
                                                 SafeOneOrTwoDoubles.of( 0.005 ),
                                                 Operator.GREATER ) );
        benchmark.add( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 10261.71429 ),
                                                 SafeOneOrTwoDoubles.of( 0.91 ),
                                                 Operator.GREATER ) );

        assertTrue( "Unexpected set of thresholds.", results.setOfThresholdOne().equals( benchmark ) );

    }

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and slices the map by 
     * {@link SafeMetricOutputMapByTimeAndThreshold#filterByThresholdOne(Threshold)}, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 42 ) );
        final Threshold q = outputFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 531.88 ),
                                                               SafeOneOrTwoDoubles.of( 0.005 ),
                                                               Operator.GREATER );
        final Pair<TimeWindow, OneOrTwoThresholds> testKeyOne = Pair.of( timeWindow, OneOrTwoThresholds.of( q ) );
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap =
                results.filterByTime( timeWindow ).filterByThresholdOne( q );

        //Check the results
        final double actualOne = subMap.get( testKeyOne ).getData();
        final double expectedOne = 0.026543876961751534;
        assertTrue( "Unexpected output from data slice [" + actualOne
                    + ","
                    + expectedOne
                    + "].",
                    Double.compare( actualOne, expectedOne ) == 0 );

        //Slice by threshold = 531.88
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap2 = results.filterByThresholdOne( q );

        //Acquire a submap by threshold = all data and lead time = 714
        final TimeWindow timeWindowTwo = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                        Instant.parse( "2010-12-31T11:59:59Z" ),
                                                        ReferenceTime.VALID_TIME,
                                                        Duration.ofHours( 714 ) );


        final OneOrTwoThresholds q2 =
                OneOrTwoThresholds.of( outputFactory.ofQuantileThreshold( SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                  SafeOneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                  Operator.GREATER ) );
        final Pair<TimeWindow, OneOrTwoThresholds> testKeyTwo = Pair.of( timeWindowTwo, q2 );

        //Slice by threshold = all data
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> subMap3 = results.filterByTime( timeWindowTwo )
                                                                                    .filterByThreshold( q2 );

        final double actualTwo = subMap3.get( testKeyTwo ).getData();
        final double expectedTwo = 0.005999378857020621;

        assertTrue( "Unexpected output from data slice [" + actualTwo
                    + ","
                    + expectedTwo
                    + "].",
                    Double.compare( actualTwo, expectedTwo ) == 0 );
        assertTrue( "Unexpected output size from data slice [" + subMap2.size()
                    + ","
                    + 29
                    + "].",
                    subMap2.size() == 29 );
        assertTrue( "Expected quantile thresholds in store.", subMap.hasQuantileThresholds() );

        // Check the empty set for the second threshold
        assertTrue( "Expected the empty set when filtering on the second threshold.",
                    Collections.emptySet().equals( results.setOfThresholdTwo() ) );

    }

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and tests the 
     * {@link SafeMetricOutputMapByTimeAndThreshold#setOfThresholdTwo()} for equality against a benchmark.
     */

    @Test
    public void testSetOfThresholdTwo()
    {
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdTwo();

        Set<Threshold> benchmark = new HashSet<>();
        benchmark.add( outF.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ), Operator.GREATER ) );
        benchmark.add( outF.ofThreshold( SafeOneOrTwoDoubles.of( 6.0 ), Operator.GREATER ) );

        assertTrue( "Unexpected set of thresholds.", results.setOfThresholdTwo().equals( benchmark ) );

    }

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and slices the map by 
     * {@link SafeMetricOutputMapByTimeAndThreshold#filterByThresholdTwo(Threshold)}, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByThresholdTwo()
    {
        DataFactory outF = DefaultDataFactory.getInstance();
        MetadataFactory metaF = DefaultMetadataFactory.getInstance();
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdTwo();

        // Filter first
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> filteredOne =
                results.filterByThresholdTwo( outF.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ), Operator.GREATER ) );

        Builder<DoubleScoreOutput> benchmarkOne = new SafeMetricOutputMapByTimeAndThreshold.Builder<>();

        //Metadata
        MetricOutputMetadata meta = metaF.getOutputMetadata( 1000,
                                                             metaF.getDimension(),
                                                             metaF.getDimension( "CMS" ),
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                             MetricConstants.MAIN,
                                                             metaF.getDatasetIdentifier( "DRRC2",
                                                                                         "SQIN",
                                                                                         "HEFS",
                                                                                         "ESP" ) );

        int[] leadTimes = new int[] { 1, 2, 3, 4, 5 };

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( leadTime ) );

            // Add first result
            OneOrTwoThresholds first =
                    OneOrTwoThresholds.of( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.0 ),
                                                             SafeOneOrTwoDoubles.of( 0.1 ),
                                                             Operator.GREATER ),
                                   outF.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ), Operator.GREATER ) );

            DoubleScoreOutput firstValue = outF.ofDoubleScoreOutput( 66.0, meta );

            benchmarkOne.put( Pair.of( timeWindow, first ), firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 2.0 ),
                                                             SafeOneOrTwoDoubles.of( 0.2 ),
                                                             Operator.GREATER ),
                                   outF.ofThreshold( SafeOneOrTwoDoubles.of( 5.0 ), Operator.GREATER ) );

            DoubleScoreOutput secondValue = outF.ofDoubleScoreOutput( 67.0, meta );

            benchmarkOne.put( Pair.of( timeWindow, second ), secondValue );

        }

        assertTrue( "Unexpected filtered result.", filteredOne.equals( benchmarkOne.build() ) );

        // Filter second        
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> filteredTwo =
                results.filterByThresholdTwo( outF.ofThreshold( SafeOneOrTwoDoubles.of( 6.0 ), Operator.GREATER ) );

        Builder<DoubleScoreOutput> benchmarkTwo = new SafeMetricOutputMapByTimeAndThreshold.Builder<>();

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( leadTime ) );

            // Add first result
            OneOrTwoThresholds first =
                    OneOrTwoThresholds.of( outF.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 3.0 ),
                                                             SafeOneOrTwoDoubles.of( 0.3 ),
                                                             Operator.GREATER ),
                                   outF.ofThreshold( SafeOneOrTwoDoubles.of( 6.0 ), Operator.GREATER ) );


            DoubleScoreOutput thirdValue = outF.ofDoubleScoreOutput( 68.0, meta );

            benchmarkTwo.put( Pair.of( timeWindow, first ), thirdValue );

        }

        assertTrue( "Unexpected filtered result.", filteredTwo.equals( benchmarkTwo.build() ) );

    }

}
