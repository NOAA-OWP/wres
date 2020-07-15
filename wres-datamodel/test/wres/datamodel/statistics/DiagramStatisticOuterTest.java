package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link DiagramStatisticOuter}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class DiagramStatisticOuterTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private StatisticMetadata metadata;

    @Before
    public void runBeforeEachTest()
    {
        this.metadata = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                 DatasetIdentifier.of( Location.of( "A" ),
                                                                                       "B",
                                                                                       "C" ) ),
                                              10,
                                              MeasurementUnit.of(),
                                              MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                              MetricConstants.MAIN );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and tests for equality with another {@link DiagramStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        Location l2 = Location.of( "A" );
        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     11,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                     MetricConstants.MAIN );
        Location l3 = Location.of( "B" );
        StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l3,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                     MetricConstants.MAIN );

        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( podComponent )
                                            .addComponents( pofdComponent )
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocTwo = DiagramStatistic.newBuilder()
                                                  .addStatistics( podTwo )
                                                  .addStatistics( pofdTwo )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podThree =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) )
                                         .build();

        DiagramStatisticComponent pofdThree =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) )
                                         .build();

        DiagramStatistic rocThree = DiagramStatistic.newBuilder()
                                                    .addStatistics( podThree )
                                                    .addStatistics( pofdThree )
                                                    .setMetric( metric )
                                                    .build();

        DiagramStatisticOuter s = DiagramStatisticOuter.of( rocOne, this.metadata );
        DiagramStatisticOuter t = DiagramStatisticOuter.of( rocTwo, this.metadata );

        assertEquals( s, t );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );
        assertNotEquals( s, DiagramStatisticOuter.of( rocThree, this.metadata ) );
        assertNotEquals( s, DiagramStatisticOuter.of( rocThree, m2 ) );
        DiagramStatisticOuter q = DiagramStatisticOuter.of( rocOne, m2 );
        DiagramStatisticOuter r = DiagramStatisticOuter.of( rocTwo, m3 );
        assertEquals( q, q );
        assertNotEquals( s, q );
        assertNotEquals( q, s );
        assertNotEquals( q, r );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the {@link DiagramStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        Location l2 = Location.of( "B" );
        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                     MetricConstants.MAIN );

        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( podComponent )
                                            .addComponents( pofdComponent )
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticOuter q = DiagramStatisticOuter.of( rocOne, this.metadata );
        DiagramStatisticOuter r = DiagramStatisticOuter.of( rocOne, m2 );

        assertNotEquals( q.getMetadata(), r.getMetadata() );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the {@link DiagramStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( podComponent )
                                            .addComponents( pofdComponent )
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocTwo = DiagramStatistic.newBuilder()
                                                  .addStatistics( podTwo )
                                                  .addStatistics( pofdTwo )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticOuter q = DiagramStatisticOuter.of( rocOne, this.metadata );
        DiagramStatisticOuter r = DiagramStatisticOuter.of( rocTwo, this.metadata );

        assertEquals( q.hashCode(), r.hashCode() );
    }

    @Test
    public void testGetData()
    {
        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( podComponent )
                                            .addComponents( pofdComponent )
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticOuter outer = DiagramStatisticOuter.of( rocOne, this.metadata );

        assertEquals( rocOne, outer.getData() );
    }


    @Test
    public void testExceptionOnNullData()
    {
        assertThrows( StatisticException.class, () -> DiagramStatisticOuter.of( null, this.metadata ) );
    }

    @Test
    public void testExceptionOnNullMetadata()
    {
        DiagramStatistic statistic = DiagramStatistic.getDefaultInstance();

        assertThrows( StatisticException.class, () -> DiagramStatisticOuter.of( statistic, null ) );
    }

}
