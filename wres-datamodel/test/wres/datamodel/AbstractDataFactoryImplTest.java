package wres.datamodel;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class AbstractDataFactoryImplTest
{
    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory 
        final PairOfDoubles tuple = AbstractDataFactoryImpl.of.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo() == 2.0);
    }

}
