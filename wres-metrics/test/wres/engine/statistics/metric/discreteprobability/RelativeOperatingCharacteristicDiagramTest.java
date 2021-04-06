package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Tests the {@link RelativeOperatingCharacteristicDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RelativeOperatingCharacteristicDiagramTest
{

    /**
     * Default instance of a {@link RelativeOperatingCharacteristicDiagram}.
     */

    private RelativeOperatingCharacteristicDiagram roc;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.roc = RelativeOperatingCharacteristicDiagram.of();
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicDiagram#apply(Pool)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Metadata for the output
        PoolMetadata m1 = Boilerplate.getSampleMetadata();

        //Check the results       
        DiagramStatisticOuter actual = this.roc.apply( input );

        List<Double> expectedPoD = List.of( 0.0,
                                            0.13580246913580246,
                                            0.2345679012345679,
                                            0.43209876543209874,
                                            0.6296296296296297,
                                            0.7037037037037037,
                                            0.8024691358024691,
                                            0.8518518518518519,
                                            0.9135802469135802,
                                            0.9753086419753086,
                                            1.0 );
        List<Double> expectedPoFD = List.of( 0.0,
                                             0.007518796992481203,
                                             0.018796992481203006,
                                             0.04887218045112782,
                                             0.11654135338345864,
                                             0.17669172932330826,
                                             0.22932330827067668,
                                             0.2857142857142857,
                                             0.42105263157894735,
                                             0.6240601503759399,
                                             1.0 );

        DiagramStatisticComponent pod =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                         .addAllValues( expectedPoD )
                                         .build();

        DiagramStatisticComponent pofd =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( expectedPoFD )
                                         .build();

        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .addStatistics( pod )
                                                      .addStatistics( pofd )
                                                      .setMetric( RelativeOperatingCharacteristicDiagram.BASIC_METRIC )
                                                      .build();

        DiagramStatisticOuter expected = DiagramStatisticOuter.of( rocDiagram, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link RelativeOperatingCharacteristicDiagram#apply(Pool)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Probability, Probability>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DiagramStatisticOuter actual = this.roc.apply( input );

        List<Double> source = List.of( Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN,
                                       Double.NaN );

        DiagramStatisticComponent pod =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                         .addAllValues( source )
                                         .build();

        DiagramStatisticComponent pofd =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( source )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( pod )
                                                    .addStatistics( pofd )
                                                    .setMetric( RelativeOperatingCharacteristicDiagram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getData() );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicDiagram#getName()} returns 
     * {@link MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( this.roc.getName().equals( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.toString() ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link RelativeOperatingCharacteristicDiagram#apply(DiscreteProbabilityPairs)} with null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.roc.apply( (Pool<Pair<Probability, Probability>>) null ) );

        assertEquals( "Specify non-null input to the '" + this.roc.getName() + "'.", actual.getMessage() );
    }


}
