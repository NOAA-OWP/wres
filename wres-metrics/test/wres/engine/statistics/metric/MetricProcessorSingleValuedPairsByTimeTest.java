package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.Executors;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorSingleValuedPairsByTime}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorSingleValuedPairsByTimeTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairsByTime} and application of
     * {@link MetricProcessorSingleValuedPairsByTime#apply(wres.datamodel.inputs.pairs.SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml and pairs 
     * obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}.
     */

    @Test
    public void test1ApplyNoThresholds()
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor = MetricFactory.getInstance( dataFactory )
                                                                                            .getMetricProcessorByTime( config,
                                                                                                                           Executors.newSingleThreadExecutor(),
                                                                                                                           null,
                                                                                                                           (MetricOutputGroup[]) null );
            SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
            MetricOutputForProjectByTimeAndThreshold results = processor.apply( pairs );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> bias = results.getScalarOutput()
                                                                       .get( MetricConstants.BIAS_FRACTION );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> cod =
                    results.getScalarOutput()
                           .get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rho = results.getScalarOutput()
                                                                      .get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> mae = results.getScalarOutput()
                                                                      .get( MetricConstants.MEAN_ABSOLUTE_ERROR );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> me =
                    results.getScalarOutput().get( MetricConstants.MEAN_ERROR );
            MetricOutputMapByTimeAndThreshold<ScalarOutput> rmse = results.getScalarOutput()
                                                                       .get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
            //Test contents
            assertTrue( "Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                        bias.getValue( 0 ).getData().equals( 1.6666666666666667 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                        cod.getValue( 0 ).getData().equals( 1.0 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                        rho.getValue( 0 ).getData().equals( 1.0 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                        mae.getValue( 0 ).getData().equals( 5.0 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ERROR,
                        me.getValue( 0 ).getData().equals( 5.0 ) );
            assertTrue( "Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                        rmse.getValue( 0 ).getData().equals( 5.0 ) );

        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairsByTime} and application of
     * {@link MetricProcessorSingleValuedPairsByTime#apply(wres.datamodel.inputs.pairs.SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml and 
     * pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}. Tests the output for multiple 
     * calls with separate forecast lead times.
     */

    @Test
    public void test2ApplyThresholds()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test2ApplyThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.values() );
            SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
            final MetadataFactory metFac = metIn.getMetadataFactory();
            //Generate results for 10 nominal lead times
            for ( int i = 1; i < 11; i++ )
            {
                final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         ReferenceTime.VALID_TIME,
                                                         i );
                final Metadata meta = metFac.getMetadata( metFac.getDimension( "CMS" ),
                                                          metFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ),
                                                          window );
                SingleValuedPairs next = metIn.ofSingleValuedPairs( pairs.getData(), meta );
                processor.apply( next );
            }           

            //Validate a subset of the data            
            processor.getStoredMetricOutput().getScalarOutput().forEach( ( key, value ) -> {
                if ( key.getKey() == MetricConstants.CRITICAL_SUCCESS_INDEX )
                {
                    assertTrue( "Expected ten results for the " + key.getKey()
                                + ": "
                                + value.size(),
                                value.size() == 10 );
                }
                else
                {
                    assertTrue( "Expected twenty results for the " + key.getKey()
                                + ": "
                                + value.size(),
                                value.size() == 20 );
                }
            } );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests for exceptions associated with a {@link MetricProcessorSingleValuedPairsByTime}.
     */

    @Test
    public void test3Exceptions()
    {

        //Check for null input
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String testOne = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsOne.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testOne ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( null );
            fail( "Expected a checked exception on processing the project configuration '" + testOne + "'." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testOne
                  + "' "
                  + "with null input." );
        }
        //Check for fail on insufficient data for a single-valued metric
        String testTwo = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsTwo.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testTwo ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testTwo
                  + "' "
                  + "with insufficient data for a single-valued metric." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testTwo
                  + "' "
                  + "with insufficient data for a single-valued metric." );
        }
        //Check for fail on insufficient data for a dichotomous metric
        String testThree = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsThree.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testThree ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testThree
                  + "' "
                  + "with insufficient data for a dichotomous metric." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testThree
                  + "' "
                  + "with insufficient data for a dichotomous metric." );
        }
        //Check for absence of thresholds on metrics that require them
        String testFour = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsFour.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testFour ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testFour
                  + "' "
                  + "with no thresholds for metrics that require them." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testFour
                  + "' "
                  + "with no thresholds for metrics that require them." );
        }
        //Checked for value thresholds that do not apply to left
        String testFive = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsFive.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testFive ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testFive
                  + "' "
                  + "with value thresholds that do not apply to left." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testFive
                  + "' "
                  + "with value thresholds that do not apply to left." );
        }
        //Checked for probability thresholds that do not apply to left
        String testSix = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsSix.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testSix ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testSix
                  + "' "
                  + "with probability thresholds that do not apply to left." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testSix
                  + "' "
                  + "with probability thresholds that do not apply to left." );
        }
        //Checked for metric-local thresholds, which are not supported
        String testSeven = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsSeven.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testSeven ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testSeven
                  + "' "
                  + "with metric-local thresholds that are not supported." );
        }
        catch ( MetricConfigurationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testSeven
                  + "' "
                  + "with metric-local thresholds that are not supported." );
        }
        //Check for insufficient data to compute climatological probability thresholds
        String testEight = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsEight.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testEight ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testEight
                  + "' "
                  + "with metric-local thresholds that are not supported." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testEight
                  + "' "
                  + "with metric-local thresholds that are not supported." );
        }
        //Check for insufficient data to compute a dichotomous metric
        String testNine = "testinput/metricProcessorSingleValuedPairsTest/test3ExceptionsNine.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testNine ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.SCALAR );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsFour() );
            fail( "Expected a checked exception on processing the project configuration '" + testNine
                  + "' with insufficient data." );
        }
        catch ( MetricCalculationException e )
        {
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing the project configuration '" + testNine
                  + "' with insufficient data." );
        }        
    }

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairsByTime} for all valid metrics associated
     * with single-valued inputs.
     */

    @Test
    public void test4AllValid()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test4AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByTime( config,
                                                                MetricOutputGroup.values() );
            //Check for the expected number of metrics
            assertTrue( "Unexpected number of metrics.",
                        processor.metrics.size() == MetricInputGroup.SINGLE_VALUED.getMetrics().size()
                                                    + MetricInputGroup.DICHOTOMOUS.getMetrics().size() );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }


}
