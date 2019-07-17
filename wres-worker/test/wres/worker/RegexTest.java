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
        String replaced = JobReceiver.setAttemptToMigrateTrue( opts );
        assertTrue( replaced.endsWith( DESIRED_STRING ) );
    }

    @Test
    public void attemptToMigrateIsReplacedAtEnd()
    {
        String opts = "-Dcheesewhiz=yes -Dwres.attemptToMigrate=false";
        String replaced = JobReceiver.setAttemptToMigrateTrue( opts );
        assertTrue( replaced.endsWith( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }

    @Test
    public void attemptToMigrateIsReplacedAtBeginning()
    {
        String opts = "-Dwres.attemptToMigrate=28537nabsd -Dcheesewhiz=yes";
        String replaced = JobReceiver.setAttemptToMigrateTrue( opts );
        assertTrue( replaced.startsWith( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }

    @Test
    public void attemptToMigrateIsReplacedInMiddle()
    {
        String opts = "-Xms=512m -Dwres.attemptToMigrate=noggin -Dblah=yes";
        String replaced = JobReceiver.setAttemptToMigrateTrue( opts );
        assertTrue( replaced.contains( DESIRED_STRING ) );
        assertFalse( replaced.contains( opts ) );
    }
}
