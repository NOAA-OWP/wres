package wres.util;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

public class LRUMapTest
{
    @Test
    public void valueExpires()
    {
        int maximumEntries = 3;
        LRUMap<Integer, String> testMap = new LRUMap<>( maximumEntries );

        final Integer firstKey = 1;
        final String firstValue = "one";

        final Integer secondKey = 2;
        final String secondValue = "two";

        final Integer thirdKey = 3;
        final String thirdValue = "three";

        final Integer fourthKey = 4;
        final String fourthValue = "four";

        final Integer fifthKey = 5;
        final String fifthValue = "five";

        testMap.put(firstKey, firstValue);
        Assert.assertTrue( "The first key and value never entered the test map", testMap.containsKey( firstKey ) );

        testMap.put(secondKey, secondValue);
        Assert.assertTrue( "Inserting the second key and value removed the first key and value", testMap.containsKey( firstKey ) );
        Assert.assertTrue( "The second key and value never entered the test map", testMap.containsKey( secondKey ) );

        testMap.put(thirdKey, thirdValue);
        Assert.assertTrue( "Inserting the third key and value removed the first key and value", testMap.containsKey( firstKey ) );
        Assert.assertTrue( "Inserting the third key and value removed the second key and value", testMap.containsKey( secondKey ) );
        Assert.assertTrue( "The third key and value never entered the test map", testMap.containsKey( thirdKey ) );

        testMap.put(fourthKey, fourthValue);
        Assert.assertTrue("Inserting the fourth key did not remove the first key and value", !testMap.containsKey( firstKey ));
        Assert.assertTrue( "Inserting the fourth key and value removed the second key and value", testMap.containsKey( secondKey ) );
        Assert.assertTrue( "Inserting the fourth key and value removed the third key and value", testMap.containsKey( thirdKey ) );
        Assert.assertTrue( "The fourth key and value never entered the test map", testMap.containsKey( fourthKey ) );

        testMap.put(fifthKey, fifthValue);
        Assert.assertTrue("Inserting the fifth key did not remove the second key and value", !testMap.containsKey( secondKey ));
        Assert.assertTrue( "Inserting the fifth key and value removed the third key and value", testMap.containsKey( thirdKey ) );
        Assert.assertTrue( "Inserting the fifth key and value removed the fourth key and value", testMap.containsKey( fourthKey ) );
        Assert.assertTrue( "The fifth key and value never entered the test map", testMap.containsKey( fifthKey ) );

        testMap.put(firstKey, firstValue);
        Assert.assertTrue("Inserting the first key did not remove the third key and value", !testMap.containsKey( thirdKey ));
        Assert.assertTrue( "Inserting the first key and value removed the fourth key and value", testMap.containsKey( fourthKey ) );
        Assert.assertTrue( "Inserting the first key and value removed the fifth key and value", testMap.containsKey( fifthKey ) );
        Assert.assertTrue( "The first key and value never entered the test map", testMap.containsKey( firstKey ) );
    }

    @Test
    public void expireActionTriggered()
    {
        int maximumEntries = 3;
        final Map<Integer, String> expireMap = new TreeMap<>(  );
        LRUMap<Integer, String> testMap = new LRUMap<>(
                maximumEntries,
                entry ->  expireMap.put(entry.getKey(), entry.getValue())
        );

        final Integer firstKey = 1;
        final String firstValue = "one";

        final Integer secondKey = 2;
        final String secondValue = "two";

        final Integer thirdKey = 3;
        final String thirdValue = "three";

        final Integer fourthKey = 4;
        final String fourthValue = "four";

        testMap.put(firstKey, firstValue);
        Assert.assertTrue( "The first key and value never entered the test map", testMap.containsKey( firstKey ) );
        Assert.assertTrue( "The expire action was called when it should not have.", !expireMap.containsKey( firstKey ) );
        Assert.assertEquals( "The expire action was called when it should not have.", 0, expireMap.size() );

        testMap.put(secondKey, secondValue);
        Assert.assertTrue( "Inserting the second key and value removed the first key and value", testMap.containsKey( firstKey ) );
        Assert.assertTrue( "The second key and value never entered the test map", testMap.containsKey( secondKey ) );
        Assert.assertEquals( "The expire action was called when it should not have.", 0, expireMap.size() );

        testMap.put(thirdKey, thirdValue);
        Assert.assertTrue( "Inserting the third key and value removed the first key and value", testMap.containsKey( firstKey ) );
        Assert.assertTrue( "Inserting the third key and value removed the second key and value", testMap.containsKey( secondKey ) );
        Assert.assertTrue( "The third key and value never entered the test map", testMap.containsKey( thirdKey ) );
        Assert.assertEquals( "The expire action was called when it should not have.", 0, expireMap.size() );

        testMap.put(fourthKey, fourthValue);
        Assert.assertTrue("Inserting the fourth key did not remove the first key and value", !testMap.containsKey( firstKey ));
        Assert.assertTrue( "Inserting the fourth key and value removed the second key and value", testMap.containsKey( secondKey ) );
        Assert.assertTrue( "Inserting the fourth key and value removed the third key and value", testMap.containsKey( thirdKey ) );
        Assert.assertTrue( "The fourth key and value never entered the test map", testMap.containsKey( fourthKey ) );

        Assert.assertEquals( "The expire action wasn't called when it should have.", 1, expireMap.size() );
        Assert.assertTrue( "The expire action did not add the correct key and value to the map of expired values.",  expireMap.containsKey( firstKey ));
    }

    @Test
    public void constructionFromMap()
    {
        Map<Integer, String> originalMap = new TreeMap<>();

        originalMap.put(1, "one");
        originalMap.put(2, "two");
        originalMap.put(3, "three");
        originalMap.put(4, "four");
        originalMap.put(5, "five");

        LRUMap<Integer, String> lruMap = new LRUMap<>( 5, originalMap );

        Assert.assertEquals( "The maps don't have the same amount of values.", originalMap.size(), lruMap.size() );
        String missingKeyTemplate = "The key (%d) was in the original map but not the second.";
        String messageTemplate = "The key (%d) and value (%s) from the original map weren't in the second.";
        for (Integer key : originalMap.keySet())
        {
            String missingKeyMessage = String.format(missingKeyTemplate, key);
            String failMessage = String.format(messageTemplate, key, originalMap.get(key));

            Assert.assertTrue( missingKeyMessage, lruMap.containsKey( key ) );
            Assert.assertEquals( failMessage, originalMap.get(key), lruMap.get(key) );
        }

        lruMap.put(6, "six");

        Assert.assertTrue("Adding a new value did not remove the first.", !lruMap.containsKey( 1 ));
    }
}
