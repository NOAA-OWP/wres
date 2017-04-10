package wres.configcontrol.datamodel.attribute;

import junit.framework.TestCase;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayInt;

/**
 * Tests of {@link UnidataArrayIterable}.
 * 
 * @author Hank.Herr
 */
public class UnidataArrayIteratableTest extends TestCase
{
    /**
     * Instantiate an {@link ArrayDouble} and use it within an {@link UnidataArrayIterable} of type {@link Double}.
     */
    public void test1ArrayIteratableDoubleInstantiation()
    {
        final ArrayDouble.D2 data = new ArrayDouble.D2(10, 10);

        for(int i = 0; i < 10; i++)
        {
            for(int j = 0; j < 10; j++)
            {
                data.set(i, j, i * 10 + j);
            }
        }
        final UnidataArrayIterable<Double> iterator = new UnidataArrayIterable<Double>(data);

        double checkValue = 0.0;
        for(final Double d: iterator)
        {
            if(!d.equals(checkValue))
            {
                fail("Unexpected value... expected " + checkValue
                    + " but received " + d);
            }
            checkValue += 1.0d;
        }
    }

    /**
     * Confirm error when instantiating an {@link ArrayDouble} and attempting to use it within an {@link UnidataArrayIterable}
     * of type {@link String}.
     */
    public void test2ErrorTestOfMappingArrayDoubleToStringIterable()
    {
        final ArrayDouble.D2 data = new ArrayDouble.D2(10, 10);

        for(int i = 0; i < 10; i++)
        {
            for(int j = 0; j < 10; j++)
            {
                data.set(i, j, i * 10 + j);
            }
        }
        try
        {
            final UnidataArrayIterable<String> iterator =
                                                 new UnidataArrayIterable<String>(data);
            for(final String d: iterator)
            {
                System.out.println("Did not expect success..." + d);
            }
            fail("Expected to fail while attempting to access ArrayDouble.D2 through Strings, but it didn't fail.");
        }
        catch(final Throwable t)
        {
            //Failure expected due to attempt at mapping an ArrayDouble to a String iterator.
        }
    }

    /**
     * Confirm error when instantiating an {@link ArrayInt} and attempting to use it within an {@link UnidataArrayIterable} of
     * type {@link Double}.
     */
    public void test3ErrorTestOfMappingArrayDoubleToIntegerIterable()
    {
        final ArrayInt.D2 data = new ArrayInt.D2(10, 10);

        for(int i = 0; i < 10; i++)
        {
            for(int j = 0; j < 10; j++)
            {
                data.set(i, j, i * 10 + j);
            }
        }
        try
        {
            final UnidataArrayIterable<Integer> iterator =
                                                  new UnidataArrayIterable<Integer>(data);
            for(final Integer d: iterator)
            {
                System.out.println("Did not expect success..." + d);
            }
            fail("Expected to fail while attempting to access ArrayDouble.D2 through Integers, but it didn't fail.");
        }
        catch(final Throwable t)
        {
            //Failure expected due to attempt at mapping an ArrayDouble to a String iterator.
        }
    }
}
