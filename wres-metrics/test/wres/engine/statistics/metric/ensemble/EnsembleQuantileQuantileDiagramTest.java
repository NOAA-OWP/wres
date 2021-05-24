package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Tests the {@link EnsembleQuantileQuantileDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsembleQuantileQuantileDiagramTest
{

    /**
     * Default instance of a {@link EnsembleQuantileQuantileDiagram}.
     */

    private EnsembleQuantileQuantileDiagram eqq;

    @Before
    public void setupBeforeEachTest()
    {
        this.eqq = EnsembleQuantileQuantileDiagram.of( 10 );
    }

    @Test
    public void testApply() throws IOException
    {
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();

        Labels labels = Labels.of( "1", "2" );

        pairs.add( Pair.of( 25.7, Ensemble.of( new double[] { 23, 43 }, labels ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( new double[] { 19, 16 }, labels ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( new double[] { 23, 54 }, labels ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( new double[] { 12, 54 }, labels ) ) );
        pairs.add( Pair.of( 12.1, Ensemble.of( new double[] { 9, 8 }, labels ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( new double[] { 23, 12 }, labels ) ) );

        Pool<Pair<Double, Ensemble>> pool = BasicPool.of( pairs, PoolMetadata.of() );

        DiagramStatisticOuter actual = this.eqq.apply( pool );

        List<Double> expectedObsQOne = List.of( 12.1, 21.4, 25.7, 32.1, 43.0, 47.0 );
        List<Double> expectedPredQOne = List.of( 9.0, 12.0, 19.0, 23.0, 23.0, 23.0 );
        List<Double> expectedObsQTwo = List.of( 12.1, 21.4, 25.7, 32.1, 43.0, 47.0 );
        List<Double> expectedPredQTwo = List.of( 8.0, 12.0, 16.0, 43.0, 54.0, 54.0 );

        DiagramStatisticComponent obsQOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( EnsembleQuantileQuantileDiagram.OBSERVED_QUANTILES.toBuilder()
                                                                                                       .setUnits( MeasurementUnit.DIMENSIONLESS ) )
                                         .addAllValues( expectedObsQOne )
                                         .setName( "1" )
                                         .build();

        DiagramStatisticComponent predQOne =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( EnsembleQuantileQuantileDiagram.PREDICTED_QUANTILES.toBuilder()
                                                                                                        .setUnits( MeasurementUnit.DIMENSIONLESS ) )
                                         .addAllValues( expectedPredQOne )
                                         .setName( "1" )
                                         .build();

        DiagramStatisticComponent obsQTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( EnsembleQuantileQuantileDiagram.OBSERVED_QUANTILES.toBuilder()
                                                                                                       .setUnits( MeasurementUnit.DIMENSIONLESS ) )
                                         .addAllValues( expectedObsQTwo )
                                         .setName( "2" )
                                         .build();

        DiagramStatisticComponent predQTwo =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( EnsembleQuantileQuantileDiagram.PREDICTED_QUANTILES.toBuilder()
                                                                                                        .setUnits( MeasurementUnit.DIMENSIONLESS ) )
                                         .addAllValues( expectedPredQTwo )
                                         .setName( "2" )
                                         .build();

        DiagramStatistic expectedInner = DiagramStatistic.newBuilder()
                                                         .addStatistics( obsQOne )
                                                         .addStatistics( predQOne )
                                                         .addStatistics( obsQTwo )
                                                         .addStatistics( predQTwo )
                                                         .setMetric( EnsembleQuantileQuantileDiagram.BASIC_METRIC )
                                                         .build();

        DiagramStatisticOuter expected = DiagramStatisticOuter.of( expectedInner, PoolMetadata.of() );

        assertEquals( expected, actual );
    }

}
