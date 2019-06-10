package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;

/**
 * Helps caller determine whether a given source has completed ingest and/or to
 * mark the source as completed. The source must have a
 * database-instance-specific numeric id already.
 */
public class SourceCompletedDetails
{
    private final int sourceId;
    private boolean definitiveCompletedAnswerFound= false;
    private boolean wasCompleted = false;

    /**
     * @param sourceDetails an already existing source complete with non-null id
     * @throws NullPointerException when sourceDetails has a null id
     */
    public SourceCompletedDetails( SourceDetails sourceDetails )
    {
        Objects.requireNonNull( sourceDetails.getId(), "Invalid SourceDetails!" );
        this.sourceId = sourceDetails.getId();
    }

    /**
     * @param sourceId the raw source_id from the database instance
     */
    public SourceCompletedDetails( int sourceId )
    {
        this.sourceId = sourceId;
    }

    /**
     * Mark the Source as completed. Not idempotent, must be called exactly once
     * by the ingester responsible for ingest of a source. To mark a source
     * complete helps other ingesters know for sure that they can finish and
     * that the WRES can begin the evaluation without waiting any longer for a
     * source to complete ingest.
     * @throws SQLException when communication with db fails or query fails
     * @throws IllegalStateException when count of rows modified not equal to 1
     */

    public void markCompleted() throws SQLException
    {
        String insertStatement = "INSERT INTO wres.SourceCompleted ( source_id ) VALUES ( ? )";
        DataScripter scripter = new DataScripter( insertStatement );
        scripter.setHighPriority( true );
        int rowsModified = scripter.execute( this.sourceId );

        if ( rowsModified != 1 )
        {
            throw new IllegalStateException( "Failed to insert a SourceCompleted row for id "
                                             + this.sourceId
                                             + ". Query that failed: '"
                                             + scripter + "'" );
        }

        wasCompleted = true;
        // Only this method can say "definitive answer found", because it did
        // the insert successfully. When calling wasCompleted, however, we may
        // want to get up-to-date information from the db via repeated calls.
        definitiveCompletedAnswerFound = true;
    }


    /**
     * Was this source completed?
     * @return true if it has been marked completed, false otherwise.
     * @throws SQLException when communication with db fails or query fails
     */

    public boolean wasCompleted() throws SQLException
    {
        if ( definitiveCompletedAnswerFound )
        {
            return this.wasCompleted;
        }

        String selectStatement = "SELECT COUNT( source_id ) AS n FROM wres.SourceCompleted WHERE source_id = ?";
        DataScripter scripter = new DataScripter( selectStatement );
        scripter.setHighPriority( true );

        try ( DataProvider dataProvider = scripter.getData( this.sourceId ) )
        {
            int countOfSourceId = dataProvider.getInt( "n" );
            return countOfSourceId == 1;
        }

        // Only markCompleted can say "definitive answer found", because it did
        // the insert successfully. When calling this method, however, we may
        // want to get up-to-date information from the db via repeated calls.
        // That is why we do not set this.wasCompleted nor set
        // this.definitiveCompletedAnswerFound in this method.
    }
}
