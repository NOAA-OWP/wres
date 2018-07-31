package wres.datamodel.outputs;

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

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link MetricOutputMapByTimeAndThreshold}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricOutputMapByTimeAndThresholdTest
{

    /**
     * Constructs a {@link MetricOutputMapByTimeAndThreshold} and slices the map, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByTimeAndFilterByThreshold()
    {
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 42 ) );
        final OneOrTwoThresholds q =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 531.88 ),
                                                                        OneOrTwoDoubles.of( 0.005 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ) );
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
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ) );
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
     * Tests {@link MetricOutputMapByTimeAndThreshold#setOfTimeWindowKeyByLeadTime()}.
     */

    @Test
    public void testSetOfTimeWindowKeyByLeadTime()
    {
        final MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( 1,
                                                                             MetadataFactory.getDimension(),
                                                                             MetadataFactory.getDimension(),
                                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                             MetricConstants.MAIN );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, DoubleScoreOutput> testMap = new HashMap<>();
        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                Operator.GREATER,
                                                                ThresholdDataType.LEFT ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                             Instant.parse( "1985-01-02T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     DoubleScoreOutput.of( 1.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-03T00:00:00Z" ),
                                             Instant.parse( "1985-01-04T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     DoubleScoreOutput.of( 2.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-05T00:00:00Z" ),
                                             Instant.parse( "1985-01-06T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     DoubleScoreOutput.of( 3.0, meta ) );
        testMap.put( Pair.of( TimeWindow.of( Instant.parse( "1985-01-07T00:00:00Z" ),
                                             Instant.parse( "1985-01-08T00:00:00Z" ),
                                             ReferenceTime.ISSUE_TIME,
                                             Duration.ofHours( 1 ),
                                             Duration.ofHours( 2 ) ),
                              threshold ),
                     DoubleScoreOutput.of( 4.0, meta ) );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> test =
                DataFactory.ofMetricOutputMapByTimeAndThreshold( testMap );
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
     * Constructs a {@link MetricOutputMapByTimeAndThreshold} and tests the 
     * {@link MetricOutputMapByTimeAndThreshold#setOfThresholdOne()} for equality against a benchmark.
     */

    @Test
    public void testSetOfThresholdOne()
    {
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        Set<Threshold> benchmark = new HashSet<>();
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 3588.66667 ),
                                                        OneOrTwoDoubles.of( 0.6 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1996.4 ),
                                                        OneOrTwoDoubles.of( 0.4 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 2707.5 ),
                                                        OneOrTwoDoubles.of( 0.5 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 12641.14286 ),
                                                        OneOrTwoDoubles.of( 0.94 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 11819.66667 ),
                                                        OneOrTwoDoubles.of( 0.93 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 13685.0 ),
                                                        OneOrTwoDoubles.of( 0.95 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 4749.0 ),
                                                        OneOrTwoDoubles.of( 0.7 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 10944.0 ),
                                                        OneOrTwoDoubles.of( 0.92 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 858.04 ),
                                                        OneOrTwoDoubles.of( 0.1 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 9647.0 ),
                                                        OneOrTwoDoubles.of( 0.9 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 560.26 ),
                                                        OneOrTwoDoubles.of( 0.01 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 26648.0 ),
                                                        OneOrTwoDoubles.of( 0.99 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1513.25 ),
                                                        OneOrTwoDoubles.of( 0.3 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 713.6 ),
                                                        OneOrTwoDoubles.of( 0.05 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 6315.33333 ),
                                                        OneOrTwoDoubles.of( 0.8 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 18122.0 ),
                                                        OneOrTwoDoubles.of( 0.97 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1147.05263 ),
                                                        OneOrTwoDoubles.of( 0.2 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 15278.4 ),
                                                        OneOrTwoDoubles.of( 0.96 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 22448.0 ),
                                                        OneOrTwoDoubles.of( 0.98 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                        OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 531.88 ),
                                                        OneOrTwoDoubles.of( 0.005 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 10261.71429 ),
                                                        OneOrTwoDoubles.of( 0.91 ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected set of thresholds.", results.setOfThresholdOne().equals( benchmark ) );

    }

    /**
     * Constructs a {@link MetricOutputMapByTimeAndThreshold} and slices the map by 
     * {@link MetricOutputMapByTimeAndThreshold#filterByThresholdOne(Threshold)}, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByThresholdOne()
    {
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 42 ) );
        final Threshold q = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 531.88 ),
                                                             OneOrTwoDoubles.of( 0.005 ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
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
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT ) );
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
     * Constructs a {@link MetricOutputMapByTimeAndThreshold} and tests the 
     * {@link MetricOutputMapByTimeAndThreshold#setOfThresholdTwo()} for equality against a benchmark.
     */

    @Test
    public void testSetOfThresholdTwo()
    {
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdTwo();

        Set<Threshold> benchmark = new HashSet<>();
        benchmark.add( Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                Operator.GREATER,
                                                ThresholdDataType.LEFT ) );
        benchmark.add( Threshold.of( OneOrTwoDoubles.of( 6.0 ),
                                                Operator.GREATER,
                                                ThresholdDataType.LEFT ) );

        assertTrue( "Unexpected set of thresholds.", results.setOfThresholdTwo().equals( benchmark ) );

    }

    /**
     * Constructs a {@link MetricOutputMapByTimeAndThreshold} and slices the map by 
     * {@link MetricOutputMapByTimeAndThreshold#filterByThresholdTwo(Threshold)}, testing for equality against a
     * benchmark.
     */

    @Test
    public void testFilterByThresholdTwo()
    {
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdTwo();

        // Filter first
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> filteredOne =
                results.filterByThresholdTwo( Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) );

        MetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> benchmarkOne =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();

        //Metadata
        Location location = MetadataFactory.getLocation( "DRRC2" );
        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( 1000,
                                                                       MetadataFactory.getDimension(),
                                                                       MetadataFactory.getDimension( "CMS" ),
                                                                       MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                       MetricConstants.MAIN,
                                                                       MetadataFactory.getDatasetIdentifier( location,
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
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1.0 ),
                                                                            OneOrTwoDoubles.of( 0.1 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );

            DoubleScoreOutput firstValue = DoubleScoreOutput.of( 66.0, meta );

            benchmarkOne.put( Pair.of( timeWindow, first ), firstValue );


            // Add second result
            OneOrTwoThresholds second =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 2.0 ),
                                                                            OneOrTwoDoubles.of( 0.2 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );

            DoubleScoreOutput secondValue = DoubleScoreOutput.of( 67.0, meta );

            benchmarkOne.put( Pair.of( timeWindow, second ), secondValue );

        }

        assertTrue( "Unexpected filtered result.", filteredOne.equals( benchmarkOne.build() ) );

        // Filter second        
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> filteredTwo =
                results.filterByThresholdTwo( Threshold.of( OneOrTwoDoubles.of( 6.0 ),
                                                                       Operator.GREATER,
                                                                       ThresholdDataType.LEFT ) );

        MetricOutputMapByTimeAndThresholdBuilder<DoubleScoreOutput> benchmarkTwo =
                new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();

        //Iterate through the lead times
        for ( int leadTime : leadTimes )
        {
            final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                         Instant.parse( "2010-12-31T11:59:59Z" ),
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( leadTime ) );

            // Add first result
            OneOrTwoThresholds first =
                    OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 3.0 ),
                                                                            OneOrTwoDoubles.of( 0.3 ),
                                                                            Operator.GREATER,
                                                                            ThresholdDataType.LEFT ),
                                           Threshold.of( OneOrTwoDoubles.of( 6.0 ),
                                                                    Operator.GREATER,
                                                                    ThresholdDataType.LEFT ) );


            DoubleScoreOutput thirdValue = DoubleScoreOutput.of( 68.0, meta );

            benchmarkTwo.put( Pair.of( timeWindow, first ), thirdValue );

        }

        assertTrue( "Unexpected filtered result.", filteredTwo.equals( benchmarkTwo.build() ) );

    }

}
