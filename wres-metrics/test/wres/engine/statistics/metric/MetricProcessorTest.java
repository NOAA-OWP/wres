package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.Threshold.Operator;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessor}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorTest
{

    /**
     * Tests the {@link MetricProcessor#fromMetricConfigName(wres.config.generated.MetricConfigName)}.
     */

    @Test
    public void test1FromMetricConfigName()
    {
        try
        {
            assertTrue( "Failed to convert '" + MetricConfigName.ALL_VALID
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.ALL_VALID ) == null );
            assertTrue( "Failed to convert '" + MetricConfigName.BIAS_FRACTION
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.BIAS_FRACTION ) == MetricConstants.BIAS_FRACTION );
            assertTrue( "Failed to convert '" + MetricConfigName.BRIER_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.BRIER_SCORE ) == MetricConstants.BRIER_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.BRIER_SKILL_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.BRIER_SKILL_SCORE ) == MetricConstants.BRIER_SKILL_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.COEFFICIENT_OF_DETERMINATION
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.COEFFICIENT_OF_DETERMINATION ) == MetricConstants.COEFFICIENT_OF_DETERMINATION );
            assertTrue( "Failed to convert '" + MetricConfigName.CONTINGENCY_TABLE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.CONTINGENCY_TABLE ) == MetricConstants.CONTINGENCY_TABLE );
            assertTrue( "Failed to convert '" + MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) == MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) == MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.CORRELATION_PEARSONS
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.CORRELATION_PEARSONS ) == MetricConstants.CORRELATION_PEARSONS );
            assertTrue( "Failed to convert '" + MetricConfigName.CRITICAL_SUCCESS_INDEX
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.CRITICAL_SUCCESS_INDEX ) == MetricConstants.CRITICAL_SUCCESS_INDEX );
            assertTrue( "Failed to convert '" + MetricConfigName.EQUITABLE_THREAT_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.EQUITABLE_THREAT_SCORE ) == MetricConstants.EQUITABLE_THREAT_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.FREQUENCY_BIAS
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.FREQUENCY_BIAS ) == MetricConstants.FREQUENCY_BIAS );
            assertTrue( "Failed to convert '" + MetricConfigName.INDEX_OF_AGREEMENT
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.INDEX_OF_AGREEMENT ) == MetricConstants.INDEX_OF_AGREEMENT );
            assertTrue( "Failed to convert '" + MetricConfigName.KLING_GUPTA_EFFICIENCY
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.KLING_GUPTA_EFFICIENCY ) == MetricConstants.KLING_GUPTA_EFFICIENCY );
            assertTrue( "Failed to convert '" + MetricConfigName.MEAN_ABSOLUTE_ERROR
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.MEAN_ABSOLUTE_ERROR ) == MetricConstants.MEAN_ABSOLUTE_ERROR );
            assertTrue( "Failed to convert '" + MetricConfigName.MEAN_ERROR
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.MEAN_ERROR ) == MetricConstants.MEAN_ERROR );
            assertTrue( "Failed to convert '" + MetricConfigName.MEAN_SQUARE_ERROR
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.MEAN_SQUARE_ERROR ) == MetricConstants.MEAN_SQUARE_ERROR );
            assertTrue( "Failed to convert '" + MetricConfigName.MEAN_SQUARE_ERROR_SKILL_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.MEAN_SQUARE_ERROR_SKILL_SCORE ) == MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.PEIRCE_SKILL_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.PEIRCE_SKILL_SCORE ) == MetricConstants.PEIRCE_SKILL_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.PROBABILITY_OF_DETECTION
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.PROBABILITY_OF_DETECTION ) == MetricConstants.PROBABILITY_OF_DETECTION );
            assertTrue( "Failed to convert '" + MetricConfigName.PROBABILITY_OF_FALSE_DETECTION
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.PROBABILITY_OF_FALSE_DETECTION ) == MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
            assertTrue( "Failed to convert '" + MetricConfigName.QUANTILE_QUANTILE_DIAGRAM
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.QUANTILE_QUANTILE_DIAGRAM ) == MetricConstants.QUANTILE_QUANTILE_DIAGRAM );
            assertTrue( "Failed to convert '" + MetricConfigName.RANK_HISTOGRAM
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.RANK_HISTOGRAM ) == MetricConstants.RANK_HISTOGRAM );
            assertTrue( "Failed to convert '" + MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) == MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM );
            assertTrue( "Failed to convert '" + MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) == MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE );
            assertTrue( "Failed to convert '" + MetricConfigName.RELIABILITY_DIAGRAM
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.RELIABILITY_DIAGRAM ) == MetricConstants.RELIABILITY_DIAGRAM );
            assertTrue( "Failed to convert '" + MetricConfigName.ROOT_MEAN_SQUARE_ERROR
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) == MetricConstants.ROOT_MEAN_SQUARE_ERROR );
            assertTrue( "Failed to convert '" + MetricConfigName.SAMPLE_SIZE
                        + "'.",
                        MetricProcessor.fromMetricConfigName( MetricConfigName.SAMPLE_SIZE ) == MetricConstants.SAMPLE_SIZE );

            //Test exception cases
            try
            {
                MetricProcessor.fromMetricConfigName( null );
                fail( "Expected a checked exception on null input." );
            }
            catch ( final MetricConfigurationException e )
            {
            }
            catch ( final Exception e )
            {
                fail( "Unexpected exception on checking for null input." );
            }
        }
        catch ( MetricConfigurationException e )
        {
            fail( "Unexpected exception on mapping metric names." );
        }
    }

    /**
     * Tests the {@link MetricProcessor#fromThresholdOperator(wres.config.generated.ThresholdOperator)}.
     */

    @Test
    public void test2FromThresholdOperator()
    {
        try
        {
            assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN
                        + "'.",
                        MetricProcessor.fromThresholdOperator( ThresholdOperator.GREATER_THAN ) == Operator.GREATER );
            assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN
                        + "'.",
                        MetricProcessor.fromThresholdOperator( ThresholdOperator.LESS_THAN ) == Operator.LESS );
            assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN_OR_EQUAL_TO
                        + "'.",
                        MetricProcessor.fromThresholdOperator( ThresholdOperator.GREATER_THAN_OR_EQUAL_TO ) == Operator.GREATER_EQUAL );
            assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN_OR_EQUAL_TO
                        + "'.",
                        MetricProcessor.fromThresholdOperator( ThresholdOperator.LESS_THAN_OR_EQUAL_TO ) == Operator.LESS_EQUAL );

            //Test exception cases
            try
            {
                MetricProcessor.fromThresholdOperator( null );
                fail( "Expected a checked exception on null input." );
            }
            catch ( final NullPointerException e )
            {
            }
            catch ( final Exception e )
            {
                fail( "Unexpected exception on checking for null input." );
            }
        }
        catch ( MetricConfigurationException e )
        {
            fail( "Unexpected exception on mapping metric names." );
        }
    }

    /**
     * Tests the {@link MetricProcessor#willStoreMetricOutput()}.
     */

    @Test
    public void test3WillStoreMetricOutput()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test4AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByLeadThreshold> trueProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.values() );
            MetricProcessor<MetricOutputForProjectByLeadThreshold> falseProcessor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config );
            //Check for storage
            assertTrue( "Expected a metric processor that stores metric outputs.",
                        trueProcessor.willStoreMetricOutput() == true );
            assertTrue( "Expected a metric processor that does not store metric outputs.",
                        falseProcessor.willStoreMetricOutput() == false );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }

    /**
     * Tests all methods related to whether metrics exist in a {@link MetricProcessor}, namely:
     * 
     * <ol>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup, MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(wres.datamodel.MetricConstants.MetricInputGroup)}</li>
     * <li>{@link MetricProcessor#hasMetrics(MetricOutputGroup)}</li>
     * <li>{@link MetricProcessor#hasThresholdMetrics()}</li>
     * </ol>
     */

    @Test
    public void test4HasMetrics()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test4AllValid.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByLeadThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .getMetricProcessorByLeadTime( config,
                                                                MetricOutputGroup.values() );
            //Check for existence of metrics
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "' and '"
                        + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ) == true );
            assertTrue( "Expected metrics for '" + MetricInputGroup.SINGLE_VALUED
                        + "'.",
                        processor.hasMetrics( MetricInputGroup.SINGLE_VALUED ) == true );
            assertTrue( "Expected metrics for '" + MetricOutputGroup.SCALAR
                        + ".",
                        processor.hasMetrics( MetricOutputGroup.SCALAR ) == true );
            assertTrue( "Expected threshold metrics.", processor.hasThresholdMetrics() == true );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPath + "'." );
        }
    }


}
