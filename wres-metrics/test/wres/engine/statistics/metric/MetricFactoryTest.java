package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MetricConstants;

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
    
    final MetricFactory metF = MetricFactory.getInstance(outF);
    
    /**
     * Tests the individual metrics in {@link MetricFactory}.
     */

    @Test
    public void test1MetricFactory()
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
    }
    
    /**
     * Tests {@link MetricFactory#ofSingleValuedScalar(MetricConstants)}. 
     */
    @Test
    public void test2OfSingleValuedScalar()
    {
        metF.ofSingleValuedScalar(MetricConstants.BIAS_FRACTION);
        metF.ofSingleValuedScalar(MetricConstants.MEAN_ABSOLUTE_ERROR);
        metF.ofSingleValuedScalar(MetricConstants.MEAN_ERROR);
        metF.ofSingleValuedScalar(MetricConstants.ROOT_MEAN_SQUARE_ERROR);
        metF.ofSingleValuedScalar(MetricConstants.CORRELATION_PEARSONS);
        metF.ofSingleValuedScalar(MetricConstants.COEFFICIENT_OF_DETERMINATION);
        try {
            metF.ofSingleValuedScalar(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {          
        }
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedVector(MetricConstants)}. 
     */
    @Test
    public void test3OfSingleValuedVector()
    {
        metF.ofSingleValuedVector(MetricConstants.MEAN_SQUARE_ERROR);
        metF.ofSingleValuedVector(MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE);
        try {
            metF.ofSingleValuedVector(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {          
        }
    }    
    
    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityVector(MetricConstants)} 
     */
    @Test
    public void test4OfDiscreteProbabilityVector()
    {
        metF.ofDiscreteProbabilityVector(MetricConstants.BRIER_SCORE);
        metF.ofDiscreteProbabilityVector(MetricConstants.BRIER_SKILL_SCORE);
        try {
            metF.ofDiscreteProbabilityVector(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {         
        }
    }     
    
    /**
     * Tests {@link MetricFactory#ofDichotomousScalar(MetricConstants)}. 
     */
    @Test
    public void test5OfDichotomousScalar()
    {
        metF.ofDichotomousScalar(MetricConstants.CRITICAL_SUCCESS_INDEX);
        metF.ofDichotomousScalar(MetricConstants.EQUITABLE_THREAT_SCORE);
        metF.ofDichotomousScalar(MetricConstants.PEIRCE_SKILL_SCORE);
        metF.ofDichotomousScalar(MetricConstants.PROBABILITY_OF_DETECTION);
        metF.ofDichotomousScalar(MetricConstants.PROBABILITY_OF_FALSE_DETECTION);
        try {
            metF.ofDichotomousScalar(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {            
        }
    }
    
    /**
     * Tests {@link MetricFactory#ofMulticategoryScalar(MetricConstants)}. 
     */
    @Test
    public void test6OfMulticategoryScalar()
    {
        metF.ofMulticategoryScalar(MetricConstants.PEIRCE_SKILL_SCORE);
        try {
            metF.ofMulticategoryScalar(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {            
        }
    }    
    
    /**
     * Tests {@link MetricFactory#ofMulticategoryMatrix(MetricConstants)}. 
     */
    @Test
    public void test7OfMulticategoryMatrix()
    {
        metF.ofMulticategoryMatrix(MetricConstants.CONTINGENCY_TABLE);
        try {
            metF.ofMulticategoryMatrix(MetricConstants.NONE);
            fail("Expected a checked exception on attempting to construct a metric with an incorrect identifier.");
        } catch(IllegalArgumentException e) {            
        }
    }      
    
    /**
     * Tests {@link MetricFactory#ofSingleValuedScalarCollection(MetricConstants...)}. 
     */
    @Test
    public void test8OfSingleValuedScalarCollection()
    {
        metF.ofSingleValuedScalarCollection(MetricConstants.MEAN_ABSOLUTE_ERROR);
    }      
    
    /**
     * Tests {@link MetricFactory#ofSingleValuedVector(MetricConstants)}. 
     */
    @Test
    public void test8OfSingleValuedVectorCollection()
    {
        metF.ofSingleValuedVectorCollection(MetricConstants.MEAN_SQUARE_ERROR);
    }    
    
    /**
     * Tests {@link MetricFactory#ofDichotomousScalarCollection(MetricConstants...)}. 
     */
    @Test
    public void test9OfDiscreteProbabilityVectorCollection()
    {
        metF.ofDiscreteProbabilityVectorCollection(MetricConstants.BRIER_SCORE);
    }       
    
    /**
     * Tests {@link MetricFactory#ofDichotomousScalarCollection(MetricConstants...)}. 
     */
    @Test
    public void test10OfDichotomousScalarCollection()
    {
        metF.ofDichotomousScalarCollection(MetricConstants.CRITICAL_SUCCESS_INDEX);
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
    public void test11Exceptions() throws InstantiationException,
                                  IllegalAccessException,
                                  InvocationTargetException,
                                  NoSuchMethodException,
                                  SecurityException
    {
        try
        {
            Constructor<MetricFactory> cons = MetricFactory.class.getDeclaredConstructor(DataFactory.class);
            cons.setAccessible(true);
            cons.newInstance((DataFactory)null);
            fail("Expected a checked exception on building a metric factory with a null output factory.");
        }
        catch(InvocationTargetException e)
        {
            assertTrue("Expected an IllegalArgumentException from the factory constructor.",
                       e.getTargetException() instanceof IllegalArgumentException);
        }
    }

}
