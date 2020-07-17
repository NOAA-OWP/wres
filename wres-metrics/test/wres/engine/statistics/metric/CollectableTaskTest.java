package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.categorical.ThreatScore;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link CollectableTask}.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
public final class CollectableTaskTest
{
    private static final double DOUBLE_COMPARE_THRESHOLD = 0.00001;

    private ExecutorService pairPool;
    private Collectable<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> m;

    /** 
     * Metadata for the output 
     */

    private SampleMetadata m1;

    @Before
    public void setupBeforeEachTest()
    {
        // Tests can run simultaneously, use only 1 (additional) Thread per test
        this.pairPool = Executors.newFixedThreadPool( 1 );
        //Add some appropriate metrics to the collection
        this.m = ThreatScore.of();

        this.m1 = SampleMetadata.of( MeasurementUnit.of() );
    }

    @Test
    public void testCollectableTask() throws ExecutionException, InterruptedException
    {

        //Wrap an input in a future
        final FutureTask<DoubleScoreStatisticOuter> futureInput =
                new FutureTask<DoubleScoreStatisticOuter>( () -> {

                    DoubleScoreStatistic table =
                            DoubleScoreStatistic.newBuilder()
                                                .setMetric( DoubleScoreMetric.newBuilder()
                                                                             .setName( MetricName.CONTINGENCY_TABLE ) )
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                             .setValue( 1 ) )
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                             .setValue( 1 ) )
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                             .setValue( 1 ) )
                                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                             .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                             .setValue( 1 ) )
                                                .build();

                    return DoubleScoreStatisticOuter.of( table, this.m1 );
                } );

        CollectableTask<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> task =
                new CollectableTask<>( this.m, futureInput );

        //Compute the pairs
        this.pairPool.submit( futureInput );

        //Should not throw an exception
        DoubleScoreStatisticOuter output = task.call();

        assertEquals( 0.333333,
                      output.getComponent( MetricConstants.MAIN ).getData().getValue(),
                      DOUBLE_COMPARE_THRESHOLD );
    }

    @Test
    public void testExceptionOnNullInput() throws ExecutionException, InterruptedException
    {
        final FutureTask<DoubleScoreStatisticOuter> futureInputNull =
                new FutureTask<>( () -> null );

        this.pairPool.submit( futureInputNull );

        final CollectableTask<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> task2 =
                new CollectableTask<>( this.m,
                                       futureInputNull );

        //Should throw an exception
        assertThrows( SampleDataException.class, () -> task2.call() );
    }

    @After
    public void tearDownAfterEachTest()
    {
        this.pairPool.shutdownNow();
    }
}
