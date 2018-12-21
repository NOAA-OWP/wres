package wres.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class LRUContainerTest
{
    @Test
    public void collectionConstructor()
    {
        final LRUTestObject one = new LRUTestObject( "1", 1 );
        final LRUTestObject two = new LRUTestObject( "2", 2 );
        final LRUTestObject three = new LRUTestObject( "3", 3 );
        final LRUTestObject four = new LRUTestObject( "4", 4 );
        final LRUTestObject five = new LRUTestObject( "5", 5 );
        final LRUTestObject six = new LRUTestObject( "6", 6 );

        Collection<LRUTestObject> objects = Arrays.asList( one, two, three, four, five, six);
        LRUContainer<LRUTestObject> container = new LRUContainer<>( 4, objects );

        // Since the limit was 4, but the collection had 6 objects, object "1"
        // shouldn't be in the container
        Assert.assertNull( container.get(object -> object.equals(one)) );

        // Since the limit was 4, but the collection had 6 objects, object "2"
        // should be in the container since it was the second item added
        Assert.assertNull( container.get(object -> object.equals(two)) );

        // Since the limit was 4, but the collection had 6 objects, object "3"
        // should be in the container
        Assert.assertNotNull( container.get(object -> object.equals(three)) );

        // Since the limit was 4, but the collection had 6 objects, object "4"
        // should be in the container
        Assert.assertNotNull( container.get(object -> object.equals(four)) );

        // Since the limit was 4, but the collection had 6 objects, object "5"
        // should be in the container
        Assert.assertNotNull( container.get(object -> object.equals(five)) );

        // Since the limit was 4, but the collection had 6 objects, object "6"
        // should be in the container
        Assert.assertNotNull( container.get(object -> object.equals(six)) );
    }

    @Test
    public void add()
    {
        LRUContainer<LRUTestObject> container = new LRUContainer<>( 4 );

        final LRUTestObject one = new LRUTestObject( "1", 1 );
        final LRUTestObject two = new LRUTestObject( "2", 2 );
        final LRUTestObject three = new LRUTestObject( "3", 3 );
        final LRUTestObject four = new LRUTestObject( "4", 4 );
        final LRUTestObject five = new LRUTestObject( "5", 5 );
        final LRUTestObject six = new LRUTestObject( "6", 6 );

        container.add( one );

        // Since "one" is the only element, it should be both the head and the tail
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), one );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), one );

        container.add(two);

        // Since "two" was added after "one", "one" should be the least recently
        // used and "two" should be the most recently used
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), one );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), two );

        container.add(three);

        // Since "three" was added after "two", which was after "one", "one"
        // should be the least recently used and "three" should be the most recently used
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), one );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), three );

        container.add(four);

        // Since "four" was added after "three", which  was added after "two",
        // which was after "one", "one" should be the least recently used and "four"
        // should be the most recently used
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), one );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), four );

        container.add(five);

        // Since "five" was added after "four", which was added after "three",
        // which  was added after "two", "one" should no longer be in the container
        // since the collection exceeded the limit of 4 objects making "two" the least
        // recently used, while "five" should be the most recently used
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), two );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), five );

        container.add(six);

        // Since "six" was added after "five", which was added after "four", which was
        // added after "three", "two" should no longer be in the container
        // since the collection exceeded the limit of 4 objects making "three" the least
        // recently used, while "six" should be the most recently used
        Assert.assertEquals( this.getLeastRecentlyUsed( container ), three );
        Assert.assertEquals( this.getMostRecentlyUsed( container ), six );
    }

    @Test
    public void get()
    {
        LRUContainer<LRUTestObject> container = new LRUContainer<>( 4 );

        final LRUTestObject one = new LRUTestObject( "1", 1 );
        final LRUTestObject two = new LRUTestObject( "2", 2 );
        final LRUTestObject three = new LRUTestObject( "3", 3 );
        final LRUTestObject four = new LRUTestObject( "4", 4 );

        container.add( one );
        container.add( two );
        container.add( three );
        container.add( four );

        // Since "one" was the first element added, "one" should be the least recently used
        Assert.assertEquals( one, this.getLeastRecentlyUsed( container ) );

        // Since "four" was the last element added, it should be the most recently used
        Assert.assertEquals( four, this.getMostRecentlyUsed( container ) );

        LRUTestObject retrievedObject = container.get(object -> object.equals( one ));

        // The function "object.equals(one)" should have retrieved "one" from the container
        // since it is in the container
        Assert.assertEquals( one, retrievedObject );

        // Since we just got "one" from the collection, it should be the most recently used
        Assert.assertEquals( one, this.getMostRecentlyUsed( container ) );

        // Since "one" became the most recently used, "two" should become the least recently used
        Assert.assertEquals( two, this.getLeastRecentlyUsed( container ) );

        retrievedObject = container.get(object -> object.key.equals("3") && object.value == 3);

        // The function "object.key.equals("3") && object.value == 3" should have retrieved
        // "three" from the container since it is in there
        Assert.assertEquals( three, retrievedObject );

        // Since we just got "three" from the collection, it should be the most recently used
        Assert.assertEquals( three, this.getMostRecentlyUsed( container ) );

        // Since "two" was not manipulated, it should still be the least recently used object
        Assert.assertEquals( two, this.getLeastRecentlyUsed( container ) );

        retrievedObject = container.get(object -> object.key.equals( "2" ));

        // The function "object.key.equals("2")" should retrieve "two"
        Assert.assertEquals( two, retrievedObject );

        // Since we just got "two", it should be the most recently used
        Assert.assertEquals( two, this.getMostRecentlyUsed( container ) );

        // Since we never manipulated "four", it should be the least recently used
        Assert.assertEquals( four, this.getLeastRecentlyUsed( container ) );

        retrievedObject = container.get(object -> object.equals( four ));

        // The function "object.equals(four)" should retrieve "four"
        Assert.assertEquals( four, retrievedObject );

        // Since we manipulated "one", "three", "two", now "four", "one should now be the least recently used
        Assert.assertEquals( one, this.getLeastRecentlyUsed( container ) );

        final String textOne = "Example Text";
        final String textTwo = "More Example Text";

        retrievedObject.mutableList.add(textOne);
        retrievedObject.mutableList.add(textTwo);

        retrievedObject = container.get(object -> object.equals( three ));

        // The function "object.equals(three)" should retrieve "three"
        Assert.assertEquals( three, retrievedObject );

        // Since we manipulated "three", "one" should still be the least recently used
        Assert.assertEquals( one, this.getLeastRecentlyUsed( container ) );

        // Since we never added items to "three"'s list, it should be empty
        Assert.assertTrue( retrievedObject.mutableList.isEmpty() );

        retrievedObject = container.get( object -> object.equals( four ) );

        // Again, "object.equals(four)" should return "four"
        Assert.assertEquals( four, retrievedObject );

        // Since we added items to "four"'s list, it shouldn't be empty
        Assert.assertFalse( retrievedObject.mutableList.isEmpty() );

        // Since we added "textOne" to "four", it should still be there
        Assert.assertTrue( retrievedObject.mutableList.contains( textOne ) );

        // Since we added "textTwo" to "four", it should still be there
        Assert.assertTrue( retrievedObject.mutableList.contains( textTwo ) );

        retrievedObject = container.get(
                object -> object.key.equals( "Shouldn't exist" ) && object.value == Integer.MIN_VALUE
        );

        // Since we didn't add an object that contains the above constraints, we should get null
        Assert.assertNull( retrievedObject );

        retrievedObject = container.get(object -> object.equals( three ));

        retrievedObject.mutableList.add(textTwo);

        retrievedObject = container.get(object -> object.equals( two ));

        // We should have retrieved "two"
        Assert.assertEquals( two, retrievedObject );

        // Since we didn't add anything to "two"'s list, it should be empty
        Assert.assertTrue( retrievedObject.mutableList.isEmpty() );

        retrievedObject = container.get(
                object -> object.mutableList.contains( textOne ) && object.mutableList.contains( textTwo )
        );

        // Since we wanted the object that has both "textOne" AND "textTwo",
        // we should have gotten "four"
        Assert.assertEquals( four, retrievedObject );

        retrievedObject = container.get(
                object -> object.mutableList.contains( textTwo ) && !object.mutableList.contains( textOne )
        );

        // Since we wanted the object that has "textTwo" but not "textOne",
        // we should have gotten "three"
        Assert.assertEquals( three, retrievedObject );

        retrievedObject = container.get(object -> object.mutableList.isEmpty());

        // Since we want an object with an empty list,
        // we should get the most recently used item with nothing in the list,
        // which should be "two"
        Assert.assertEquals( two, retrievedObject );
    }

    private <U> U getLeastRecentlyUsed(LRUContainer<U> container)
    {
        return this.getInnerList( container ).getFirst();
    }

    private <U> U getMostRecentlyUsed(LRUContainer<U> container)
    {
        return this.getInnerList( container ).getLast();
    }

    private <U> LinkedList<U> getInnerList(LRUContainer<U> container)
    {
        return Whitebox.getInternalState(container, "innerList");
    }

    /**
     * An object used specifically for testing purposes containing an immutable key,
     * an immutable value, and a list that may be manipulated.
     */
    private static class LRUTestObject
    {
        private final String key;
        private final int value;
        private final List<String> mutableList = new ArrayList<>();

        private LRUTestObject(final String key, final int value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals( Object obj )
        {
            boolean equals = obj instanceof LRUTestObject;

            if(equals)
            {
                LRUTestObject other = (LRUTestObject)obj;

                equals = this.key.equals(other.key) && this.value == other.value;
            }

            return equals;
        }

        @Override
        public String toString()
        {
            return String.format("Key: %s, Value: %d", this.key, this.value);
        }
    }
}
