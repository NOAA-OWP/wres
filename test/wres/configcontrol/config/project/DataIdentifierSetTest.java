package wres.configcontrol.config.project;

// Imports
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

// WRES dependencies
import wres.configcontrol.config.CompoundIdentifier;

/**
 * Test class for {@link DataIdentifierSet}
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataIdentifierSetTest
{

    /**
     * Tests for equality of two {@link DataIdentifierSet}.
     */
    @Test
    public void assertEqual()
    {
        final Map<Integer, String> a = new TreeMap<>();
        a.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> c = new TreeMap<>();
        c.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");

        final CompoundIdentifier id1 = new CompoundIdentifier(a);
        final CompoundIdentifier id2 = new CompoundIdentifier(b);
        final CompoundIdentifier id3 = new CompoundIdentifier(c);
        URI u1 = null;
        URI u2 = null;
        URI u3 = null;
        try
        {
            u1 = new URI("some.location");
            u2 = new URI("some.location");
            u3 = new URI("some.other.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final DataIdentifier p = new DataIdentifier(id1, u1);
        final DataIdentifier q = new DataIdentifier(id2, u2);
        final DataIdentifier r = new DataIdentifier(id3, u3);

        final DataIdentifierSet x = new DataIdentifierSet();
        x.fluentAdd(p).fluentAdd(q).fluentAdd(r);
        final DataIdentifierSet y = x.deepCopy();

        final Map<Integer, String> d = new TreeMap<>();
        d.put(CompoundIdentifier.OBJECT_IDENTIFIER, "FCFM8");
        d.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        d.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");
        final CompoundIdentifier id4 = new CompoundIdentifier(a);
        final DataIdentifier s = new DataIdentifier(id4, u1);
        final DataIdentifierSet z = new DataIdentifierSet();
        z.add(s);
        //Test
        new EqualsTester().addEqualityGroup(x, y).addEqualityGroup(z).testEquals();
    }

}
