package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.io.config.ProjectConfigPlus;

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
        metF.ofContingencyTable();
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
     * Tests {@link MetricFactory#ofSingleValuedScalar(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void test2OfSingleValuedScalar() throws MetricParameterException
    {
        metF.ofSingleValuedScalar( MetricConstants.BIAS_FRACTION );
        metF.ofSingleValuedScalar( MetricConstants.MEAN_ABSOLUTE_ERROR );
        metF.ofSingleValuedScalar( MetricConstants.MEAN_ERROR );
        metF.ofSingleValuedScalar( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        metF.ofSingleValuedScalar( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        metF.ofSingleValuedScalar( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        metF.ofSingleValuedScalar( MetricConstants.INDEX_OF_AGREEMENT );
        metF.ofSingleValuedScalar( MetricConstants.SAMPLE_SIZE );
        try
        {
            metF.ofSingleValuedScalar( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void test3OfSingleValuedVector() throws MetricParameterException
    {
        metF.ofSingleValuedVector( MetricConstants.MEAN_SQUARE_ERROR );
        metF.ofSingleValuedVector( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE );
        metF.ofSingleValuedVector( MetricConstants.KLING_GUPTA_EFFICIENCY );
        try
        {
            metF.ofSingleValuedVector( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityVector(MetricConstants)} 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test4OfDiscreteProbabilityVector() throws MetricParameterException
    {
        metF.ofDiscreteProbabilityVector( MetricConstants.BRIER_SCORE );
        metF.ofDiscreteProbabilityVector( MetricConstants.BRIER_SKILL_SCORE );
        metF.ofDiscreteProbabilityVector( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE );
        try
        {
            metF.ofDiscreteProbabilityVector( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScalar(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test5OfDichotomousScalar() throws MetricParameterException
    {
        metF.ofDichotomousScalar( MetricConstants.CRITICAL_SUCCESS_INDEX );
        metF.ofDichotomousScalar( MetricConstants.EQUITABLE_THREAT_SCORE );
        metF.ofDichotomousScalar( MetricConstants.PEIRCE_SKILL_SCORE );
        metF.ofDichotomousScalar( MetricConstants.PROBABILITY_OF_DETECTION );
        metF.ofDichotomousScalar( MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
        try
        {
            metF.ofDichotomousScalar( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofMulticategoryScalar(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test6OfMulticategoryScalar() throws MetricParameterException
    {
        metF.ofMulticategoryScalar( MetricConstants.PEIRCE_SKILL_SCORE );
        try
        {
            metF.ofMulticategoryScalar( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    /**
     * Tests {@link MetricFactory#ofMulticategoryMatrix(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test7OfMulticategoryMatrix() throws MetricParameterException
    {
        metF.ofMulticategoryMatrix( MetricConstants.CONTINGENCY_TABLE );
        try
        {
            metF.ofMulticategoryMatrix( MetricConstants.MAIN );
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
    public void test8OfBoxPlot() throws MetricParameterException
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
     * Tests {@link MetricFactory#ofSingleValuedScalarCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void test9OfSingleValuedScalarCollection() throws MetricParameterException
    {
        metF.ofSingleValuedScalarCollection( MetricConstants.MEAN_ABSOLUTE_ERROR );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test8OfSingleValuedVectorCollection() throws MetricParameterException
    {
        metF.ofSingleValuedVectorCollection( MetricConstants.MEAN_SQUARE_ERROR );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test10OfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        metF.ofDiscreteProbabilityVectorCollection( MetricConstants.BRIER_SCORE );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScalarCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test11OfDichotomousScalarCollection() throws MetricParameterException
    {
        metF.ofDichotomousScalarCollection( MetricConstants.CRITICAL_SUCCESS_INDEX );
        metF.ofDichotomousScalarCollection( MetricConstants.FREQUENCY_BIAS );
    }
    
    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test12OfSingleValuedMultiVectorCollection() throws MetricParameterException
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
    public void test13OfDiscreteProbabilityMultiVectorCollection() throws MetricParameterException
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
     * Tests {@link MetricFactory#ofMulticategoryMatrixCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test14OfMulticategoryMatrixCollection() throws MetricParameterException
    {
        metF.ofMulticategoryMatrixCollection( MetricConstants.CONTINGENCY_TABLE );
    }    
    
    /**
     * Tests {@link MetricFactory#ofEnsembleScalarCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test15OfEnsembleScalarCollection() throws MetricParameterException
    {
        metF.ofEnsembleScalarCollection( MetricConstants.SAMPLE_SIZE );
        try
        {
            metF.ofEnsembleScalarCollection( MetricConstants.MAIN );
            fail( "Expected a checked exception on attempting to construct a metric with an incorrect identifier." );
        }
        catch ( IllegalArgumentException e )
        {
        }        
    }    
    
    /**
     * Tests {@link MetricFactory#ofEnsembleVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void test16OfEnsembleVectorCollection() throws MetricParameterException
    {
        metF.ofEnsembleVectorCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );
        metF.ofEnsembleVectorCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        try
        {
            metF.ofEnsembleVectorCollection( MetricConstants.MAIN );
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
    public void test17OfEnsembleMultiVectorCollection() throws MetricParameterException
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
    public void test19Exceptions() throws InstantiationException,
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
    public void test20MetricProcessor()
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
