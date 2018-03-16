package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;

import org.junit.Test;

import wres.config.ProjectConfigPlus;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.engine.statistics.metric.processing.MetricProcessor;

/**
 * Tests the {@link MetricFactory}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricFactoryTest
{

    /**
     * Output factory.
     */

    final DataFactory outF = DefaultDataFactory.getInstance();

    /**
     * Metric factory.
     */

    final MetricFactory metF = MetricFactory.getInstance( outF );

    /**
     * Tests the individual metrics in {@link MetricFactory}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1MetricFactory() throws MetricParameterException
    {
        metF.ofBiasFraction();
        metF.ofBrierScore();
        metF.ofBrierSkillScore();
        metF.ofDichotomousContingencyTable();
        metF.ofCriticalSuccessIndex();
        metF.ofEquitableThreatScore();
        metF.ofMeanAbsoluteError();
        metF.ofMeanError();
        metF.ofMeanSquareError();
        metF.ofMeanSquareErrorSkillScore();
        metF.ofPeirceSkillScore();
        metF.ofPeirceSkillScoreMulti();
        metF.ofProbabilityOfDetection();
        metF.ofProbabilityOfFalseDetection();
        metF.ofRootMeanSquareError();
        metF.ofSampleSize();
        metF.ofReliabilityDiagram();
        metF.ofRelativeOperatingCharacteristic();
        metF.ofRelativeOperatingCharacteristicScore();
        metF.ofRankHistogram();
        metF.ofQuantileQuantileDiagram();
        metF.ofFrequencyBias();
        metF.ofIndexOfAgreement();
        metF.ofKlingGuptaEfficiency();
        metF.ofCorrelationPearsons();
        metF.ofCoefficientOfDetermination();
        metF.ofContinuousRankedProbabilityScore();
        metF.ofContinuousRankedProbabilitySkillScore();
        metF.ofBoxPlotErrorByObserved();
        metF.ofBoxPlotErrorByForecast();
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void test2OfSingleValuedScalar() throws MetricParameterException
    {
        metF.ofSingleValuedScore( MetricConstants.BIAS_FRACTION );
        metF.ofSingleValuedScore( MetricConstants.MEAN_ABSOLUTE_ERROR );
        metF.ofSingleValuedScore( MetricConstants.MEAN_ERROR );
        metF.ofSingleValuedScore( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        metF.ofSingleValuedScore( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        metF.ofSingleValuedScore( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        metF.ofSingleValuedScore( MetricConstants.INDEX_OF_AGREEMENT );
        metF.ofSingleValuedScore( MetricConstants.SAMPLE_SIZE );
        metF.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR );
        metF.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE );
        metF.ofSingleValuedScore( MetricConstants.KLING_GUPTA_EFFICIENCY );
        try
        {
            metF.ofSingleValuedScore( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScore(MetricConstants)} 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test3OfDiscreteProbabilityScore() throws MetricParameterException
    {
        metF.ofDiscreteProbabilityScore( MetricConstants.BRIER_SCORE );
        metF.ofDiscreteProbabilityScore( MetricConstants.BRIER_SKILL_SCORE );
        metF.ofDiscreteProbabilityScore( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE );
        try
        {
            metF.ofDiscreteProbabilityScore( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test4OfDichotomousScore() throws MetricParameterException
    {
        metF.ofDichotomousScore( MetricConstants.THREAT_SCORE );
        metF.ofDichotomousScore( MetricConstants.EQUITABLE_THREAT_SCORE );
        metF.ofDichotomousScore( MetricConstants.PEIRCE_SKILL_SCORE );
        metF.ofDichotomousScore( MetricConstants.PROBABILITY_OF_DETECTION );
        metF.ofDichotomousScore( MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
        try
        {
            metF.ofDichotomousScore( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofMulticategoryScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test5OfMulticategoryScore() throws MetricParameterException
    {
        metF.ofMulticategoryScore( MetricConstants.PEIRCE_SKILL_SCORE );
        try
        {
            metF.ofMulticategoryScore( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousMatrix(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test6OfMulticategoryMatrix() throws MetricParameterException
    {
        metF.ofDichotomousMatrix( MetricConstants.CONTINGENCY_TABLE );
        try
        {
            metF.ofDichotomousMatrix( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }
    
    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlot(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test7OfBoxPlot() throws MetricParameterException
    {
        metF.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE );
        try
        {
            metF.ofEnsembleBoxPlot( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }
    

    /**
     * Tests {@link MetricFactory#ofSingleValuedScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void test8OfSingleValuedScoreCollection() throws MetricParameterException
    {
        metF.ofSingleValuedScoreCollection( MetricConstants.MEAN_ABSOLUTE_ERROR );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test9OfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        metF.ofDiscreteProbabilityScoreCollection( MetricConstants.BRIER_SCORE );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test10OfDichotomousScoreCollection() throws MetricParameterException
    {
        metF.ofDichotomousScoreCollection( MetricConstants.THREAT_SCORE );
        metF.ofDichotomousScoreCollection( MetricConstants.FREQUENCY_BIAS );
    }
    
    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test11OfSingleValuedMultiVectorCollection() throws MetricParameterException
    {
        metF.ofSingleValuedMultiVectorCollection( MetricConstants.QUANTILE_QUANTILE_DIAGRAM );
        try
        {
            metF.ofSingleValuedMultiVectorCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }    
    
    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric collection could not be constructed
     */
    @Test
    public void test12OfDiscreteProbabilityMultiVectorCollection() throws MetricParameterException
    {
        metF.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELIABILITY_DIAGRAM );
        metF.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM );
        try
        {
            metF.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }    
    
    /**
     * Tests {@link MetricFactory#ofDichotomousMatrixCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test13OfMulticategoryMatrixCollection() throws MetricParameterException
    {
        metF.ofDichotomousMatrixCollection( MetricConstants.CONTINGENCY_TABLE );
    }    
    
    /**
     * Tests {@link MetricFactory#ofEnsembleScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test14OfEnsembleScoreCollection() throws MetricParameterException
    {
        metF.ofEnsembleScoreCollection( MetricConstants.SAMPLE_SIZE );
        try
        {
            metF.ofEnsembleScoreCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }    
    
    /**
     * Tests {@link MetricFactory#ofEnsembleScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test15OfEnsembleScoreCollection() throws MetricParameterException
    {
        metF.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );
        metF.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        try
        {
            metF.ofEnsembleScoreCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }    
    
    /**
     * Tests {@link MetricFactory#ofEnsembleMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test16OfEnsembleMultiVectorCollection() throws MetricParameterException
    {
        metF.ofEnsembleMultiVectorCollection( MetricConstants.RANK_HISTOGRAM );
        try
        {
            metF.ofEnsembleMultiVectorCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }  

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlotCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test17OfEnsembleBoxPlotCollection() throws MetricParameterException
    {
        metF.ofEnsembleBoxPlotCollection( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE );
        try
        {
            metF.ofEnsembleBoxPlotCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    } 

    /**
     * Tests for exceptions in {@link MetricFactory}.
     * 
     * @throws SecurityException if reflection fails
     * @throws NoSuchMethodException if reflection fails
     * @throws InstantiationException if reflection fails
     * @throws IllegalAccessException if reflection fails
     * @throws InvocationTargetException if reflection fails
     */

    @Test
    public void test18Exceptions() throws InstantiationException,
            IllegalAccessException,
            InvocationTargetException,
            NoSuchMethodException,
            SecurityException
    {
        try
        {
            Constructor<MetricFactory> cons = MetricFactory.class.getDeclaredConstructor( DataFactory.class );
            cons.setAccessible( true );
            cons.newInstance( (DataFactory) null );
            fail( "Expected a checked exception on building a metric factory with a null output factory." );
        }
        catch ( InvocationTargetException e )
        {
            e.printStackTrace();
            assertTrue( "Expected an IllegalArgumentException from the factory constructor.",
                        e.getTargetException() instanceof IllegalArgumentException );
        }
    }

    /**
     * Tests the construction of {@link MetricProcessor}. 
     */

    @Test
    public void test19MetricProcessor()
    {
        //Single-valued processor
        String configPathSingleValued =
                "testinput/metricProcessorSingleValuedPairsByTimeTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
            MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                         .ofMetricProcessorByTimeSingleValuedPairs( config, (MetricOutputGroup) null );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPathSingleValued + "'." );
        }
        //Ensemble processor        
        String configPathEnsemble = "testinput/metricProcessorEnsemblePairsByTimeTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
            MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                         .ofMetricProcessorByTimeEnsemblePairs( config, (MetricOutputGroup) null );
        }
        catch ( Exception e )
        {
            fail( "Unexpected exception on processing project configuration '" + configPathEnsemble + "'." );
        }
    }

}
