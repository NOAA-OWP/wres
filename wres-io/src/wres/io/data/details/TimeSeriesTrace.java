package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Represents a row which in turn represents metadata for one timeseries trace
 * @author Christopher Tubbs
 */
public class TimeSeriesTrace
{
    private final Database database;

    /**
     * The ID of the ensemble for the time series. A time series without
     * an ensemble should be indicated as the "default" time series.
     */
	private final long ensembleID;

    /**
     * The ID of the time series in the database
     */
    private Long timeSeriesID = null;


    /**
     * The ID of the initial source of the data for the time series
     */
    private final long sourceID;


    public TimeSeriesTrace( Database database,
                            long ensembleID,
                            long sourceID )
    {
        Objects.requireNonNull( database );
        this.database = database;
        this.ensembleID = ensembleID;
        this.sourceID = sourceID;
    }

    public long getEnsembleId()
    {
        return this.ensembleID;
    }

	/**
	 * @return Returns the ID in the database corresponding to this
     * Time Series. If the ID is not present, it is retrieved from the database
	 * @throws SQLException Thrown if the value could not be retrieved from the database
	 */
	public long getTimeSeriesID() throws SQLException
	{
		if (timeSeriesID == null)
		{
			save();
		}
		return timeSeriesID;
	}

	/**
	 * Creates a new entry in the database representing this time series
	 * @throws SQLException Thrown if successful communication with the database
     * could not be established.
	 */
    private void save() throws SQLException
	{
        DataScripter script = new DataScripter( this.database );

        script.addTab().addLine( "INSERT INTO wres.TimeSeriesTrace (" );
        script.addTab(  2  ).addLine("ensemble_id,");
        script.addTab(  2  ).addLine( "source_id" );
        script.addTab().addLine(")");
        script.addTab().addLine( "VALUES ( ?, ? );" );
        script.addArgument( this.ensembleID );
        script.addArgument( this.sourceID );

        int rowsModified = script.execute();
        long insertedId = script.getInsertedIds()
                                .get( 0 );

        if ( rowsModified != 1 )
        {
            throw new IllegalStateException( "Failed to insert a row using "
                                             + script );
        }

        if ( script.getInsertedIds().size() <= 0 )
        {
            throw new IllegalStateException( "Failed to get inserted id using"
                                             + script );
        }

        this.timeSeriesID = insertedId;
    }

}