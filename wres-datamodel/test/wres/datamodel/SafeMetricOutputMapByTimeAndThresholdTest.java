package wres.datamodel;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.Threshold.Operator;
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
    public void test1ConstructAndFilter()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( 42 ) );
        final Threshold q = outputFactory.getQuantileThreshold( 531.88, 0.005, Operator.GREATER );
        final Pair<TimeWindow, Threshold> testKeyOne = Pair.of( timeWindow, q );
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


        final Threshold q2 = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                 Double.NEGATIVE_INFINITY,
                                                                 Operator.GREATER );
        final Pair<TimeWindow, Threshold> testKeyTwo = Pair.of( timeWindowTwo, q2 );

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
    public void test2ConstructAndFilter()
    {
        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();
        final MetricOutputMetadata meta = metaFac.getOutputMetadata( 1,
                                                                     metaFac.getDimension(),
                                                                     metaFac.getDimension(),
                                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                     MetricConstants.MAIN );
        Map<Pair<TimeWindow, Threshold>, DoubleScoreOutput> testMap = new HashMap<>();
        Threshold threshold = outputFactory.getThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER );
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

}
