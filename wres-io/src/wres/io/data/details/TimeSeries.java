package wres.io.data.details;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Represents a row which in turn represents metadata for one timeseries trace
 * @author Christopher Tubbs
 */
public class TimeSeries
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeries.class );

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

    private final Database database;

    /**
     * The ID of the ensemble for the time series. A time series without
     * an ensemble should be indicated as the "default" time series.
     */
	private final int ensembleID;

    /**
     * The unit of measurement that values for the time series were taken in
     */
	private final int measurementUnitID;

    /**
     * The ID of the time series in the database
     */
    private Integer timeSeriesID = null;

    /**
     * The time scale associated with the data
     */

    private TimeScaleOuter timeScale = null;

    /**
     * The ID of the initial source of the data for the time series
     */
    private final int sourceID;

    /**
     * The db ID of the feature associated with the time series.
     */
    private final int featureID;

    /**
     * The variable name associated with the time series. For USGS data, this is
     * a Physical Element code. For NWM, it's the variable.
     */
    private final String variableName;

    private final Instant initializationDate;

    public TimeSeries( Database database,
                       int ensembleID,
                       int measurementUnitID,
                       Instant initializationDate,
                       int sourceID,
                       String variableName,
                       int featureID )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( initializationDate );
        Objects.requireNonNull( variableName );
        this.database = database;
        this.ensembleID = ensembleID;
        this.measurementUnitID = measurementUnitID;
        this.initializationDate = initializationDate;
        this.sourceID = sourceID;
        this.variableName = variableName;
        this.featureID = featureID;
    }

    public Instant getInitializationDate()
    {
        return this.initializationDate;
    }

    public TimeScaleOuter getTimeScale()
    {
        return this.timeScale;
    }

    public void setTimeScale(final TimeScaleOuter timeScale)
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
        this.timeScale = TimeScaleOuter.of( Duration.ofMinutes( period ),
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
        DataScripter script = new DataScripter( this.database );

        // Scale information, missing by default
        Integer scalePeriod = null;
        TimeScaleFunction scaleFunction = TimeScaleFunction.UNKNOWN;
        
        if( Objects.nonNull( this.getTimeScale() ) )
        {
            scalePeriod = (int) this.getTimeScale().getPeriod().toMinutes();
            scaleFunction = this.getTimeScale().getFunction();
        }

        script.addTab().addLine("INSERT INTO wres.TimeSeries (");
        script.addTab(  2  ).addLine("ensemble_id,");
        script.addTab(  2  ).addLine("measurementunit_id,");
        script.addTab(  2  ).addLine("initialization_date,");
        script.addTab(  2  ).addLine("scale_period,");
        script.addTab(  2  ).addLine("scale_function,");
        script.addTab(  2  ).addLine("source_id,");
        script.addTab(  2  ).addLine( "variable_name," );
        script.addTab(  2  ).addLine( "feature_id" );
        script.addTab().addLine(")");
        script.addTab().addLine( "VALUES ( ?, ?, ?, ?, ?, ?, ?, ? );" );
        script.addArgument( this.ensembleID );
        script.addArgument( this.measurementUnitID );
        OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant( this.initializationDate,
                                                                  ZoneOffset.UTC );
        script.addArgument( offsetDateTime );
        script.addArgument( scalePeriod );
        script.addArgument( scaleFunction.name() );
        script.addArgument( this.sourceID );
        script.addArgument( this.variableName );
        script.addArgument( this.featureID );

        int rowsModified = script.execute();
        int insertedId = script.getInsertedIds()
                               .get( 0 )
                               .intValue();

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

        LOGGER.debug( "Given Instant {} translated to OffsetDateTime {} for wres.TimeSeries id {}",
                      this.initializationDate, offsetDateTime, insertedId );
        this.timeSeriesID = insertedId;
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
