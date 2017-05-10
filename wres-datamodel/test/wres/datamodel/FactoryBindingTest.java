package wres.datamodel;

import org.junit.Test;
import static org.junit.Assert.*;

public class FactoryBindingTest
{
    @Test
    public void useDataFactoryTest()
    {
        PairOfDoubles tuple = DataFactory.tupleOf(1.0, 2.0);
        assertNotNull(tuple);
        assert(tuple.getItemOne() == 1.0);
        assert(tuple.getItemTwo() == 2.0);
    }
}
