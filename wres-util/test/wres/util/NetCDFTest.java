package wres.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class NetCDFTest
{
    /**
     * Test to ensure that the VectorVariableIterator can properly buffer and iterate through a netcdf file
     * <p>
     *     The data looks like:
     * </p>
     * <pre>
     * netcdf IteratorTestData {
     *     dimensions:
     *         test_dimension = 20 ;
     *     variables:
     *         double test_variable(test_dimension) ;
     *     data:
     *         test_variable = 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6,
     *             -7, -8, -9 ;
     * }
     * </pre>
     * <p>
     *     Since all of the values are whole numbers, low precision is reasonable for value testing
     * </p>
     */
    @Test
    public void VectorVariableIteratorTest()
    {
        // We only need to be within 0.01 of the actual values for this test since every value is a whole number
        final double epsilon = 0.01;

        NetcdfFile testFile = null;
        try
        {
            testFile = NetcdfFile.open("testinput/NetCDFTest/IteratorTestData.nc");
        }
        catch ( IOException e )
        {
            Assert.fail( "The test netcdf file could not be loaded." );
        }

        // Get the variable from the test data
        Variable testVariable = testFile.findVariable( "test_variable" );

        Array testVariableData = null;

        try
        {
            // Load the data from the test file as control data since this is the UCAR
            // provide method of loading netcdf data
            testVariableData = testVariable.read();
        }
        catch ( IOException e )
        {
            Assert.fail( "Data from the 'test_variable' variable could not be read" );
        }

        // Load the control data into a list for random access
        List<Double> dataFromFile = new ArrayList<>();

        while (testVariableData.hasNext())
        {
            Double next = testVariableData.nextDouble();
            dataFromFile.add( next );
        }

        // Make sure that the proper index for a read value can be retrieved and that the iterator can
        // iterate through all values in the variable, in a single buffer
        NetCDF.VectorVariableIterator variableIterator = NetCDF.VectorVariableIterator.from( testVariable );

        for (int valueIndex = 0; valueIndex < testVariable.getSize(); ++valueIndex)
        {
            Assert.assertTrue(
                    "The iterator doesn't think it can iterate through all of the data in the variable.",
                    variableIterator.hasNext()
            );
            variableIterator.next();
            Assert.assertEquals(valueIndex, variableIterator.getIndexOfLastValue() );
        }

        Assert.assertFalse(
                "The iterator somehow thinks it can read more data than what exists in the variable",
                variableIterator.hasNext()
        );


        // Make sure that the proper index for a read value can be retrieved and that the iterator can
        // iterate through all values in the variable, across multiple buffers
        variableIterator = NetCDF.VectorVariableIterator.from( testVariable, 7 );

        for (int valueIndex = 0; valueIndex < testVariable.getSize(); ++valueIndex)
        {
            Assert.assertTrue(
                    "The iterator doesn't think it can iterate through all of the data in the variable.",
                    variableIterator.hasNext()
            );
            variableIterator.next();
            Assert.assertEquals(valueIndex, variableIterator.getIndexOfLastValue() );
        }

        Assert.assertFalse(
                "The iterator somehow thinks it can read more data than what exists in the variable",
                variableIterator.hasNext()
        );

        // Iterate through each value in the iterator and compare each value with the matching control value
        while (variableIterator.hasNext())
        {
            Double next = variableIterator.nextDouble();
            int lastIndex = variableIterator.getIndexOfLastValue();
            Double nextFromFile = dataFromFile.get( lastIndex );
            Assert.assertEquals(nextFromFile, next, epsilon);
        }

        // Basic, buffer-less test
        //    The default buffer is 1000 values, but this file only has 20; no actual buffering will occur
        variableIterator = NetCDF.VectorVariableIterator.from( testVariable );

        // Iterate through each value in the iterator and compare each value with the matching control value
        while (variableIterator.hasNext())
        {
            Double next = variableIterator.nextDouble();
            int lastIndex = variableIterator.getIndexOfLastValue();
            Double nextFromFile = dataFromFile.get( lastIndex );
            Assert.assertEquals(nextFromFile, next, epsilon);
        }

        // Test with a buffer with a size of a prime number
        //     The iterator will be forced to create a smaller buffer to avoid an InvalidRangeException
        variableIterator = NetCDF.VectorVariableIterator.from( testVariable, 13 );

        while (variableIterator.hasNext())
        {
            Double next = variableIterator.nextDouble();
            int lastIndex = variableIterator.getIndexOfLastValue();
            Double nextFromFile = dataFromFile.get( lastIndex );
            Assert.assertEquals(nextFromFile, next, epsilon);
        }
    }
}
