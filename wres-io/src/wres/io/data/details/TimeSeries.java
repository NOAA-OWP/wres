package wres.io.data.details;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeScale.TimeScaleFunction;
import wres.io.utilities.DataScripter;

/**
 * Defines details about a forecasted time series
 * @author Christopher Tubbs
 */
public class TimeSeries
{

    /**
     * The number of unique lead times contained within a partition within
     * the database for values linked to a forecasted time series
     */
    private static final short TIMESERIESVALUE_PARTITION_SPAN = 1200;

    /**
     * Mapping between the number of a forecast value partition and its name
     */
    private static final Map<Integer, String> TIMESERIESVALUE_PARTITION_NAMES =
            new ConcurrentHashMap<>();

    /**
     * The ID of the ensemble for the time series. A time series without
     * an ensemble should be indicated as the "default" time series.
     */
	private Integer ensembleID = null;

    /**
     * The ID of the cross section between a variable and its location
     */
	private Integer variableFeatureID = null;

    /**
     * The unit of measurement that values for the time series were taken in
     */
	private Integer measurementUnitID = null;

    /**
     * The ID of the time series in the database
     */
    private Integer timeSeriesID = null;

    /**
     * The time scale associated with the data
     */

    private TimeScale timeScale = null;

    /**
     * The ID of the initial source of the data for the time series
     */
    private final Integer sourceID;

    /**
     * The string representation of the date and time of when the forecast
     * began. For instance, if a forecasted value for a time series at a lead
     * time of 1 occured at '01-01-2017 13:00:00', the initialization date
     * would be '01-01-2017 12:00:00'.
     */
    private final String initializationDate;

    public TimeSeries( Integer sourceID, String initializationDate )
    {
        this.sourceID = sourceID;
        this.initializationDate = initializationDate;
    }
	
	/**
	 * Sets the ID of the Ensemble that the time series is linked to. The ID of
     * the time series is invalidated if the ID of the Ensemble it is linked
     * to changes
	 * @param ensembleId The ID of the new ensemble
	 */
	public void setEnsembleID(Integer ensembleId)
	{
		if (this.ensembleID != null && !this.ensembleID.equals(ensembleId))
		{
			this.timeSeriesID = null;
		}
        this.ensembleID = ensembleId;
	}
	
	/**
	 * Sets the ID of the relationship between the variable and its location
     * for this time series. The ID of the time series is
	 * invalidated if the ID of the linked Variable location changes
	 * @param variableFeatureID The ID of the new variable location
	 */
	public void setVariableFeatureID(int variableFeatureID)
	{
		if (this.variableFeatureID != null && this.variableFeatureID != variableFeatureID)
		{
			this.timeSeriesID = null;
		}
        this.variableFeatureID = variableFeatureID;
	}

    /**
     * @return The ID of the union between the variable and location
     */
	public Integer getVariableFeatureID()
    {
        return this.variableFeatureID;
    }

	/**
	 * Sets the ID of the unit of measurement connected to the ensemble for
     * this Time Series. The ID of the Time Series
	 * is invalidated if the ID of the linked Measurement Unit changes
	 * @param measurementUnitID The ID of the new unit of measurement
	 */
	public void setMeasurementUnitID(int measurementUnitID)
	{
		if (this.measurementUnitID != null && this.measurementUnitID != measurementUnitID)
		{
			this.timeSeriesID = null;
		}
        this.measurementUnitID = measurementUnitID;
	}

    public String getInitializationDate()
    {
        return this.initializationDate;
    }

    public TimeScale getTimeScale()
    {
        return this.timeScale;
    }

    public void setTimeScale(final TimeScale timeScale)
    {
        this.timeScale = timeScale;
    }
    
    /**
     * Sets the time scale information from an integer period and string function.
     * @param period the period
     * @param function the function string
     */
    
    private void setTimeScale( final int period, final String function )
    {
        this.timeScale = TimeScale.of( Duration.ofMinutes( period ), 
                                       TimeScaleFunction.valueOf( function ) );
    }

    public int getEnsembleId()
    {
        return this.ensembleID;
    }
	
	/**
	 * @return Returns the ID in the database corresponding to this
     * Time Series. If the ID is not present, it is retrieved from the database
	 * @throws SQLException Thrown if the value could not be retrieved from the database
	 */
	public int getTimeSeriesID() throws SQLException
	{
		if (timeSeriesID == null)
		{
			save();
		}
		return timeSeriesID;
	}
	
	/**
	 * Invalidate the cache of partition names. See #61206.
	 */
	
	public static void invalidateGlobalCache()
	{
	    synchronized ( TIMESERIESVALUE_PARTITION_NAMES )
	    {
	        TimeSeries.TIMESERIESVALUE_PARTITION_NAMES.clear();
	    }
	}
	
	/**
	 * Creates a new entry in the database representing this time series
	 * @throws SQLException Thrown if successful communication with the database
     * could not be established.
	 */
    private void save() throws SQLException
	{
        DataScripter script = new DataScripter(  );

        // Scale information, missing by default
        Integer scalePeriod = null;
        TimeScaleFunction scaleFunction = TimeScaleFunction.UNKNOWN;
        
        if( Objects.nonNull( this.getTimeScale() ) )
        {
            scalePeriod = (int) this.getTimeScale().getPeriod().toMinutes();
            scaleFunction = this.getTimeScale().getFunction();
        }

        script.addTab().addLine("INSERT INTO wres.TimeSeries (");
        script.addTab(  2  ).addLine("variablefeature_id,");
        script.addTab(  2  ).addLine("ensemble_id,");
        script.addTab(  2  ).addLine("measurementunit_id,");
        script.addTab(  2  ).addLine("initialization_date,");
        script.addTab(  2  ).addLine("scale_period,");
        script.addTab(  2  ).addLine("scale_function");
        script.addTab().addLine(")");
        script.addTab().addLine( "VALUES ( ?, ?, ?, (?)::timestamp without time zone, ?, (?)::scale_function );" );
        script.addArgument( this.variableFeatureID );
        script.addArgument( this.ensembleID );
        script.addArgument( this.measurementUnitID );
        script.addArgument( this.initializationDate );
        script.addArgument( scalePeriod );
        script.addArgument( scaleFunction.name() );

        int rowsModified = script.execute();
        int insertedId = script.getInsertedId();

        if ( rowsModified != 1 )
        {
            throw new IllegalStateException( "Failed to insert a row using "
                                             + script );
        }

        if ( script.getInsertedId() <= 0 )
        {
            throw new IllegalStateException( "Failed to get inserted id using"
                                             + script );
        }

        this.timeSeriesID = insertedId;

        DataScripter scriptTwo = new DataScripter();
        scriptTwo.addLine( "INSERT INTO wres.TimeSeriesSource ( timeseries_id, source_id )" );
        scriptTwo.addTab( 1 ).addLine( "VALUES ( ?, ? );" );
        scriptTwo.addArgument( this.timeSeriesID );
        scriptTwo.addArgument( this.sourceID );
        scriptTwo.execute();
    }

    /**
     * Either creates or returns the name of the partition of where values
     * for this timeseries should be saved based on lead time
     * Must be kept in sync with liquibase scripts.
     * TODO: Move to a more appropriate location
     * @param lead The lead time of this time series where values of interest
     *             should be saved
     * @return The name of the partition where values for the indicated lead time
     * should be saved.
     */
    public static String getTimeSeriesValuePartition( int lead )
    {
        int partitionNumber = lead / TimeSeries.TIMESERIESVALUE_PARTITION_SPAN;

        String name = TIMESERIESVALUE_PARTITION_NAMES.get( partitionNumber );

        if ( name == null )
        {
            String partitionNumberWord;

            // Sometimes the lead times are negative, but the dash is not a
            // valid character in a name in sql, so we replace with a word.
            if ( partitionNumber < -10 )
            {
                partitionNumberWord = "Below_Negative_10";
            }
            else if ( partitionNumber > 150 )
            {
                partitionNumberWord = "Above_150";
            }
            else if ( partitionNumber < 0 )
            {
                partitionNumberWord = "Negative_"
                                      + Math.abs( partitionNumber );
            }
            else
            {
                partitionNumberWord = Integer.toString( partitionNumber );
            }

            name = "wres.TimeSeriesValue_Lead_" + partitionNumberWord;

            TimeSeries.TIMESERIESVALUE_PARTITION_NAMES.putIfAbsent( partitionNumber, name);
        }

        return name;
    }
}
