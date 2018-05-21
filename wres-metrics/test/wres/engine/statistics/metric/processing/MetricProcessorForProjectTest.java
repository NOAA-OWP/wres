package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link MetricProcessorForProject}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorForProjectTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Instance of a data factory.
     */

    private DataFactory dataFac;

    /**
     * Instance of a metric factory.
     */

    private MetricFactory metricFac;

    /**
     * Instance of a single-valued processor.
     */

    private MetricProcessorForProject singleValuedProcessor;

    /**
     * Instance of an ensemble processor.
     */

    private MetricProcessorForProject ensembleProcessor;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        dataFac = DefaultDataFactory.getInstance();
        metricFac = MetricFactory.getInstance( dataFac );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        // Mock the config
        ProjectConfig mockedConfigSingleValued =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        ProjectConfig mockedConfigEnsemble =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        singleValuedProcessor = new MetricProcessorForProject( metricFac,
                                                               mockedConfigSingleValued,
                                                               null,
                                                               ForkJoinPool.commonPool(),
                                                               ForkJoinPool.commonPool() );

        ensembleProcessor = new MetricProcessorForProject( metricFac,
                                                           mockedConfigEnsemble,
                                                           null,
                                                           ForkJoinPool.commonPool(),
                                                           ForkJoinPool.commonPool() );

    }

    /**
     * Tests the {@link MetricProcessorForProject#getMetricProcessorForSingleValuedPairs()}.
     */

    @Test
    public void testGetMetricProcessorForSingleValuedPairs()
    {
        assertNotNull( singleValuedProcessor.getMetricProcessorForSingleValuedPairs() );
    }

    /**
     * Tests the {@link MetricProcessorForProject#getMetricProcessorForEnsemblePairs()}.
     */

    @Test
    public void testGetMetricProcessorForEnsemblePairs()
    {
        assertNotNull( ensembleProcessor.getMetricProcessorForEnsemblePairs() );
    }

    /**
     * Tests the {@link MetricProcessorForProject#hasCachedMetricOutput()}.
     */

    @Test
    public void testHasCachedMetricOutput()
    {
        assertFalse( singleValuedProcessor.hasCachedMetricOutput() );
        assertFalse( ensembleProcessor.hasCachedMetricOutput() );
    }

    /**
     * Tests the {@link MetricProcessorForProject#getMetricOutputTypesToCache()}.
     */

    @Test
    public void testGetMetricOutputTypesToCache()
    {
        assertTrue( singleValuedProcessor.getMetricOutputTypesToCache()
                                         .equals( new HashSet<>( Arrays.asList( MetricOutputGroup.PAIRED,
                                                                                MetricOutputGroup.DOUBLE_SCORE ) ) ) );
        assertTrue( ensembleProcessor.getMetricOutputTypesToCache()
                    .equals( new HashSet<>( Arrays.asList( MetricOutputGroup.PAIRED,
                                                           MetricOutputGroup.DOUBLE_SCORE ) ) ) );
    }

    /**
     * Tests the {@link MetricProcessorForProject#getCachedMetricOutputTypes()}.
     * @throws InterruptedException if the execution is interrupted
     */

    @Test
    public void testGetCachedMetricOutputTypes() throws InterruptedException
    {
        assertTrue( singleValuedProcessor.getCachedMetricOutputTypes()
                                         .equals( new HashSet<>() ) );
        assertTrue( ensembleProcessor.getCachedMetricOutputTypes()
                    .equals( new HashSet<>( ) ) );
    }    
    
    /**
     * Tests the {@link MetricProcessorForProject#getCachedMetricOutput()}.
     * @throws InterruptedException if the execution is interrupted
     */

    @Test
    public void testCachedMetricOutput() throws InterruptedException
    {
        assertNotNull( singleValuedProcessor.getCachedMetricOutput() );
        assertNotNull( ensembleProcessor.getCachedMetricOutput() );
    }  

    /**
     * Tests the construction of a {@link MetricProcessorForProject} for processing simulations.
     * @throws MetricParameterException if the construction failed
     */
    
    @Test
    public void testConstructionForSimulations() throws MetricParameterException
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_ERROR ) );

        // Mock the config
        ProjectConfig mockedConfigSingleValued =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SIMULATIONS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessorForProject simulationProcessor = new MetricProcessorForProject( metricFac,
                                                               mockedConfigSingleValued,
                                                               null,
                                                               ForkJoinPool.commonPool(),
                                                               ForkJoinPool.commonPool() );
        
        assertFalse( simulationProcessor.getMetricOutputTypesToCache().isEmpty() );
    }
    
    /**
     * Tests that the {@link MetricProcessorForProject#getMetricProcessorForSingleValuedPairs()} throws an exception
     * when the processor is built for ensemble pairs.
     */

    @Test
    public void testGetMetricProcessorForSingleValuedPairsThrowsException()
    {
        exception.expect( MetricProcessorException.class );
        exception.expectMessage( "This metric processor was not built to consume ensemble pairs." );
        
        singleValuedProcessor.getMetricProcessorForEnsemblePairs();
    }

    /**
     * Tests that the {@link MetricProcessorForProject#getMetricProcessorForEnsemblePairs()} throws an exception
     * when the processor is built for single-valued pairs.
     */

    @Test
    public void testGetMetricProcessorForEnsemblePairsThrowsException()
    {        
        exception.expect( MetricProcessorException.class );
        exception.expectMessage( "This metric processor was not built to consume single-valued pairs." );
    
        ensembleProcessor.getMetricProcessorForSingleValuedPairs();
    }    
    
}
