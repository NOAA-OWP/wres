package wres.worker;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RegexTest
{
    private static final String DESIRED_STRING = "-Dwres.attemptToMigrate=true";

    @Test
    public void attemptToMigrateIsAppended()
    {
        String opts = "-Dcheesewhiz=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.attemptToMigrate",
                                                        "true" );
        assertTrue( replaced.endsWith( DESIRED_STRING ) );
    }

    @Test
    public void attemptToMigrateIsReplacedAtEnd()
    {
        String opts = "-Dcheesewhiz=yes -Dwres.attemptToMigrate=false";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.attemptToMigrate",
                                                        "true" );
        assertTrue( replaced.endsWith( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }

    @Test
    public void attemptToMigrateIsReplacedAtBeginning()
    {
        String opts = "-Dwres.attemptToMigrate=28537nabsd -Dcheesewhiz=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.attemptToMigrate",
                                                        "true" );
        assertTrue( replaced.startsWith( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }

    @Test
    public void attemptToMigrateIsReplacedInMiddle()
    {
        String opts = "-Xms=512m -Dwres.attemptToMigrate=noggin -Dblah=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.attemptToMigrate",
                                                        "true" );
        assertTrue( replaced.contains( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }

    @Test
    public void hostIsReplacedInMiddleWithChange()
    {
        String opts = "-Xms=512m -Dwres.databaseHost=test-wresdb-t01.you.me.them.gov -Dwres.attemptToMigrate=noggin -Dblah=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.databaseHost",
                                                        "test-wresdb-d01.you.me.them.gov" );
        //I expect to see the new database host.
        assertTrue( "String after replacement, \"" + replaced + "\", not as expected.", 
                replaced.equals( 
                    "-Xms=512m -Dwres.databaseHost=test-wresdb-d01.you.me.them.gov -Dwres.attemptToMigrate=noggin -Dblah=yes" ) ); 
    }

    @Test
    public void hostIsReplacedInMiddleWithSameHost()
    { 
        String opts = "-Xms=512m -Dwres.databaseHost=test-wresdb-t01.you.me.them.gov -Dwres.attemptToMigrate=noggin -Dblah=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.databaseHost",
                                                        "test-wresdb-t01.you.me.them.gov" );
        //There should be no change from the calls above.
        assertTrue( "New string \"" + replaced + "\", is not the same as original, \"" 
                + opts + "\".", replaced.equals( opts ) ); 
    } 


    @Test
    public void portIsReplacedInMiddleWithChange()
    {
        String opts = "-Xms=512m -Dwres.databasePort=5432 -Dwres.attemptToMigrate=noggin -Dblah=yes";
        String replaced = JobReceiver.setJavaOptOption( opts,
                                                        "wres.databasePort",
                                                        "0000" );
        //I expect to see the new database host.
        assertTrue( "String after replacement, \"" + replaced + "\", not as expected.",
                replaced.equals(
                    "-Xms=512m -Dwres.databasePort=0000 -Dwres.attemptToMigrate=noggin -Dblah=yes" ) );
    }

}
