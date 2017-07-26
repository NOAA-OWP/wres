package wres.configcontrol.config.project;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

// Imports
import com.google.common.testing.EqualsTester;

// WRES dependencies
import wres.configcontrol.config.CompoundIdentifier;

/**
 * Test class for {@link CompoundIdentifier}
 * 
 * @author james.brown@hydrosolved.com
 */
public class CompoundIdentifierTest
{

    /**
     * Tests for equality of two {@link CompoundIdentifier}.
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
        //Test
        new EqualsTester().addEqualityGroup(id1, id2).addEqualityGroup(id3).testEquals();
    }

    /**
     * Tests the {@link Comparable#compareTo(Object)} methods of two {@link CompoundIdentifier}.
     */
    @Test
    public void assertComparable()
    {
        final Map<Integer, String> a = new TreeMap<>();
        a.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(CompoundIdentifier.SCENARIO_IDENTIFIER, "GEFS");
        final String msgA = "Objects were not comparable: <" + a + ">, <" + b + ">";
        final CompoundIdentifier id1 = new CompoundIdentifier(a);
        final CompoundIdentifier id2 = new CompoundIdentifier(b);
        Assert.assertTrue(msgA, id1.compareTo(id2) == 0);
        Assert.assertTrue(msgA, id2.compareTo(id1) == 0);
        final Map<Integer, String> c = new TreeMap<>();
        c.put(CompoundIdentifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(CompoundIdentifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        final String msgB = "Comparison failed: <" + b + ">, <" + c + ">";
        final CompoundIdentifier id3 = new CompoundIdentifier(c);
        Assert.assertTrue(msgB, id1.compareTo(id3) > 0);
        Assert.assertTrue(msgB, id3.compareTo(id1) < 0);
    }

}
