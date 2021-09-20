package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Tests the {@link QuantileQuantileDiagram}.
 * 
 * @author James Brown
 */
public final class QuantileQuantileDiagramTest
{

    /**
     * Default instance of a {@link QuantileQuantileDiagram}.
     */

    private QuantileQuantileDiagram qqd;

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
            double left = i;
            double right = left;
            values.add( Pair.of( left, right ) );
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
                                         .setMetric( QuantileQuantileDiagram.OBSERVED_QUANTILES.toBuilder()
                                                                                               .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( observedQ )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( QuantileQuantileDiagram.PREDICTED_QUANTILES.toBuilder()
                                                                                                .setUnits( "DIMENSIONLESS" ) )
                                         .addAllValues( predictedQ )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( oqs )
                                                    .addStatistics( pqs )
                                                    .setMetric( QuantileQuantileDiagram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DiagramStatisticOuter actual = this.qqd.apply( input );

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( QuantileQuantileDiagram.OBSERVED_QUANTILES.toBuilder()
                                                                                               .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( QuantileQuantileDiagram.PREDICTED_QUANTILES.toBuilder()
                                                                                                .setUnits( "DIMENSIONLESS" ) )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( oqs )
                                                    .addStatistics( pqs )
                                                    .setMetric( QuantileQuantileDiagram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.toString(), this.qqd.getName() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        NullPointerException expected = assertThrows( NullPointerException.class, () -> this.qqd.apply( null ) );

        assertEquals( "Specify non-null input to the 'QUANTILE QUANTILE DIAGRAM'.", expected.getMessage() );
    }

}
