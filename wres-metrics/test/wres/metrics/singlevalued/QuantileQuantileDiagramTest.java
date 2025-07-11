package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Before;
import org.junit.Test;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link QuantileQuantileDiagram}.
 *
 * @author James Brown
 */
public final class QuantileQuantileDiagramTest
{

    /** Default instance of a {@link QuantileQuantileDiagram}. */
    private QuantileQuantileDiagram qqd;

    /** Observed quantiles. */
    private static final DiagramMetric.DiagramMetricComponent OBSERVED_QUANTILES =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.OBSERVED_QUANTILES )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                .setMinimum( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.getMinimum() )
                                                .setMaximum( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.getMaximum() )
                                                .build();

    /** Predicted quantiles. */
    private static final DiagramMetric.DiagramMetricComponent PREDICTED_QUANTILES =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.PREDICTED_QUANTILES )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                .setMinimum( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.getMinimum() )
                                                .setMaximum( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.getMaximum() )
                                                .build();

    @Before
    public void setupBeforeEachTest()
    {
        this.qqd = QuantileQuantileDiagram.of( 10 );
    }

    @Test
    public void testApply()
    {
        //Generate some data
        final List<Pair<Double, Double>> values = new ArrayList<>();
        for ( int i = 1; i < 11; i++ )
        {
            values.add( Pair.of( ( double ) i, ( double ) i ) );
        }

        Pool<Pair<Double, Double>> input = Pool.of( values, PoolMetadata.of() );

        //Check the results        
        DiagramStatisticOuter actual = this.qqd.apply( input );

        List<Double> observedQ = new ArrayList<>();
        List<Double> predictedQ = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            double next = Precision.round( i, 1 );
            observedQ.add( next );
            predictedQ.add( next );
        }

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( OBSERVED_QUANTILES.toBuilder()
                                                                       .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( observedQ )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( PREDICTED_QUANTILES.toBuilder()
                                                                        .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( predictedQ )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( oqs )
                                                    .addStatistics( pqs )
                                                    .setMetric( QuantileQuantileDiagram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DiagramStatisticOuter actual = this.qqd.apply( input );

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( OBSERVED_QUANTILES.toBuilder()
                                                                       .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( PREDICTED_QUANTILES.toBuilder()
                                                                        .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( oqs )
                                                    .addStatistics( pqs )
                                                    .setMetric( QuantileQuantileDiagram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.toString(), this.qqd.getMetricNameString() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        NullPointerException expected = assertThrows( NullPointerException.class, () -> this.qqd.apply( null ) );

        assertEquals( "Specify non-null input to the 'QUANTILE QUANTILE DIAGRAM'.", expected.getMessage() );
    }

}
