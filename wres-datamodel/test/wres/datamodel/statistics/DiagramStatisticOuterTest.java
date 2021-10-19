package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link DiagramStatisticOuter}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class DiagramStatisticOuterTest
{

    private PoolMetadata metadata;

    @Before
    public void runBeforeEachTest()
    {
        FeatureKey feature = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( feature, feature, feature ) ),
                                          null,
                                          null,
                                          null,
                                          false,
                                          1 );

        this.metadata = PoolMetadata.of( evaluation, pool );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and tests for equality with another {@link DiagramStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        FeatureKey l2 = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( l2, l2, l2 ) ),
                                          null,
                                          null,
                                          null,
                                          false,
                                          1 );

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );
        FeatureKey l3 = FeatureKey.of( "B" );

        Pool poolTwo = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( l3, l3, l3 ) ),
                                             null,
                                             null,
                                             null,
                                             false,
                                             1 );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolTwo );

        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocTwo = DiagramStatistic.newBuilder()
                                                  .addStatistics( podTwo )
                                                  .addStatistics( pofdTwo )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podThree =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) )
                                         .build();

        DiagramStatisticComponent pofdThree =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
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
        assertNotEquals( s, r );
        assertNotEquals( r, s );
        assertNotEquals( q, r );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the {@link DiagramStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        FeatureKey l2 = FeatureKey.of( "B" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( FeatureGroup.of( new FeatureTuple( l2, l2, l2 ) ),
                                          null,
                                          null,
                                          null,
                                          false,
                                          1 );

        PoolMetadata m2 = PoolMetadata.of( evaluation, pool );

        DiagramMetricComponent podComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                      .build();

        DiagramMetricComponent pofdComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
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
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatistic rocOne = DiagramStatistic.newBuilder()
                                                  .addStatistics( podOne )
                                                  .addStatistics( pofdOne )
                                                  .setMetric( metric )
                                                  .build();

        DiagramStatisticComponent podTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
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
                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                            .build();

        DiagramStatisticComponent podOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( podComponent )
                                         .addAllValues( List.of( 0.1, 0.2, 0.3, 0.4 ) )
                                         .build();

        DiagramStatisticComponent pofdOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( pofdComponent )
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
