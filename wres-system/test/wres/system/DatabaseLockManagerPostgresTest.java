package wres.system;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DatabaseLockManagerPostgresTest
{
    /**
     * Verify that when the Long values overflow, the overflow makes sense
     * to the DatabaseLockManagerPostgres technique of using the positive value
     * and the negative value of a positive lock number.
     */
    @Test
    public void TestLockIdConversion()
    {
        Long first = 5L;
        Long second = 2147483653L;

        Integer firstLockName = DatabaseLockManagerPostgres.getIntegerLockNameFromLong( first );
        Integer secondLockName = DatabaseLockManagerPostgres.getIntegerLockNameFromLong( second );
        assertEquals( firstLockName, secondLockName );
    }
}
