package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.categorical.ContingencyTableScore;
import wres.engine.statistics.metric.categorical.CriticalSuccessIndex;

/**
 * Tests the {@link ContingencyTableScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContingencyTableScoreTest
{

    /**
     * Constructs a {@link ContingencyTableScore} and tests it.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1ContingencyTableScore() throws MetricParameterException
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outputFactory.getMetadataFactory();
        final MetricFactory metricFactory = MetricFactory.getInstance( outputFactory );
        final CriticalSuccessIndex cs = metricFactory.ofCriticalSuccessIndex();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 365,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN );

        final double[][] benchmark = new double[][] { { 82.0, 38.0 }, { 23.0, 222.0 } };
        final MatrixOutput expected = outputFactory.ofMatrixOutput( benchmark, m1 );

        assertTrue( "The Critical Success Index should not have real units.", !cs.hasRealUnits() );
        try
        {
            cs.is2x2ContingencyTable( expected, cs );
        }
        catch ( final Exception e )
        {
            fail( "Expected a 2x2 contingency table: " + e.getMessage() );
        }
    }

    /**
     * Constructs a {@link ContingencyTableScore} and checks for exceptional cases.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final MetricFactory metricFactory = MetricFactory.getInstance( outF );
        final CriticalSuccessIndex cs = metricFactory.ofCriticalSuccessIndex();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 365,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN );
        //Check the exceptions
        try
        {
            cs.apply( (DichotomousPairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        try
        {
            cs.is2x2ContingencyTable( outF.ofMatrixOutput( new double[][] { { 1.0 } }, m1 ), cs );
            fail( "Expected an exception on construction with an incorrectly sized matrix." );
        }
        catch ( final Exception e )
        {
        }
        try
        {
            cs.is2x2ContingencyTable( outF.ofMatrixOutput( new double[][] { { 1.0, 1.0, 1.0 }, { 1.0, 1.0, 1.0 } },
                                                           m1 ),
                                      cs );
            fail( "Expected an exception on construction with a non-square matrix." );
        }
        catch ( final Exception e )
        {
        }
        //Null input
        try
        {
            cs.is2x2ContingencyTable( (MatrixOutput) null, cs );
            fail( "Expected an exception on testing for a 2x2 contingency table with null input." );
        }
        catch ( final Exception e )
        {
        }        
        try
        {
            cs.is2x2ContingencyTable( outF.ofMatrixOutput( new double[][] { { 1.0 } }, m1 ), null );
            fail( "Expected an exception on testing for a 2x2 contingency table with a null metric." );
        }
        catch ( final Exception e )
        {
        }          
        try
        {
            cs.isContingencyTable( (MatrixOutput) null, cs );
            fail( "Expected an exception on testing for a contingency table with a null input." );
        }
        catch ( final Exception e )
        {
        }        
        try
        {
            cs.isContingencyTable( outF.ofMatrixOutput( new double[][] { { 1.0 } }, m1 ), null );
            fail( "Expected an exception on testing for a contingency table with a null metric." );
        }
        catch ( final Exception e )
        {
        }           
    }

}
