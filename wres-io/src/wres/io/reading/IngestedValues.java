package wres.io.reading;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metadata.TimeScale;
import wres.io.data.details.TimeSeries;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.Database;
import wres.system.SystemSettings;
import wres.util.TimeHelper;

/**
 * Facilitates a shared location for copying forecast timeseries values to
 * the database
 */
public final class IngestedValues
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestedValues.class );

    private static final IngestedValues ourInstance = new IngestedValues();

    public static IngestedValues getInstance()
    {
        return ourInstance;
    }

    private IngestedValues()
    {
    }

    // Key = partition name, i.e. "partitions.forecastvalue_lead_0"
    // Value = List of values to save to the partition
    private static final ConcurrentMap<String, DataBuilder> VALUES_TO_SAVE = new ConcurrentHashMap<>(  );

    /** Guards VALUES_TO_SAVE */
    private static final Object VALUES_TO_SAVE_LOCK = new Object();

    private static final String[] TIMESERIES_COLUMN_NAMES = {"timeseries_id", "lead", "series_value"};

    private static final Object OBSERVATIONS_LOCK = new Object();

    private static final DataBuilder OBSERVATIONS = DataBuilder.with(
            "variablefeature_id",
            "observation_time",
            "observed_value",
            "measurementunit_id",
            "source_id",
            "scale_period",
            "scale_function",
            "time_step"
    );

    /**
     * Add an observation with possibly missing time scale information.
     * 
     * @param variableFeatureID the variableFeature identifier
     * @param observationTime the observation time
     * @param observedValue the observed value
     * @param measurementUnitID the measurement unit identifier
     * @param sourceID the source identifier
     * @param scalePeriod the optional period associated with the time scale (may be null)
     * @param scaleFunction the optional function associated with the time scale (may be null)
     * @param timeStep the time-step of the values
     */
    
    static void addObservation(
            final int variableFeatureID,
            final TemporalAccessor observationTime,
            final Double observedValue,
            final int measurementUnitID,
            final int sourceID,
            final Duration scalePeriod,
            final TimeScale.TimeScaleFunction scaleFunction,
            final Duration timeStep)
    {
        synchronized ( OBSERVATIONS_LOCK )
        {
            IngestedValues.OBSERVATIONS.addRow();

            IngestedValues.OBSERVATIONS.set("variablefeature_id", variableFeatureID);
            IngestedValues.OBSERVATIONS.set("observation_time", observationTime);
            IngestedValues.OBSERVATIONS.set("observed_value", observedValue);
            IngestedValues.OBSERVATIONS.set("measurementunit_id", measurementUnitID);
            IngestedValues.OBSERVATIONS.set("source_id", sourceID);

            // Only apply the time scale information where defined
            // See #59536
            if ( Objects.nonNull( scalePeriod ) )
            {
                int scalePeriodAsInt = (int) TimeHelper.durationToLongUnits( scalePeriod, TimeHelper.LEAD_RESOLUTION );
                IngestedValues.OBSERVATIONS.set( "scale_period", scalePeriodAsInt );
            }
            
            IngestedValues.OBSERVATIONS.set( "scale_function", scaleFunction );

            int timeStepAsInt = (int) TimeHelper.durationToLongUnits( timeStep, TimeHelper.LEAD_RESOLUTION );
            IngestedValues.OBSERVATIONS.set( "time_step", timeStepAsInt );

            if (IngestedValues.OBSERVATIONS.getRowCount() > SystemSettings.getMaximumCopies())
            {
                Future<?> copy = IngestedValues.OBSERVATIONS.build().copy( "wres.Observation", true );
                Database.storeIngestTask(copy);
            }
        }
    }

    /**
     * Stores a time series value so that it may be copied to the database later
     * @param timeSeriesID The ID of the time series that the value belongs to
     * @param lead The lead time for the value
     * @param value The value itself
     * @throws SQLException Thrown if the name of the proper partition could not
     * be retrieved.
     */
    public static void addTimeSeriesValue( final int timeSeriesID, final int lead, final Double value)
            throws SQLException
    {
        String partitionName = TimeSeries.getTimeSeriesValuePartition( lead );

        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            // Add a list for the values if it isn't present
            VALUES_TO_SAVE.putIfAbsent( partitionName, DataBuilder.with( IngestedValues.TIMESERIES_COLUMN_NAMES ));

            // Add the values to the list for the partition
            VALUES_TO_SAVE.get( partitionName ).addRow( timeSeriesID, lead, value );

            // If the maximum number of values to copy has been reached, copy the
            // values
            if ( VALUES_TO_SAVE.get( partitionName ).getRowCount() >= SystemSettings.getMaximumCopies())
            {
                IngestedValues.copy( partitionName );
            }
        }
    }

    /**
     * Creates and executes a task used to copy the values for a partition to
     * the database
     * @param partitionName The name of the partition whose values need to be
     *                      saved
     */
    private static Future<?> copy(String partitionName)
    {
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            return VALUES_TO_SAVE.get( partitionName ).build().copy( partitionName );
        }
    }

    /**
     * Send all values across all stored partitions to the database
     * @throws  IOException Thrown if an error occurred while attempting to save out read data
     */
    public static void complete() throws IOException
    {
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            LOGGER.trace("Adding the rest of the consolidated observation and forecasted values.");
            List<Future<?>> tasks = new ArrayList<>();

            LOGGER.trace("Adding {} observation values", IngestedValues.OBSERVATIONS.getRowCount());
            tasks.add(IngestedValues.OBSERVATIONS.build().copy( "wres.Observation" ));

            for (String partitionName : VALUES_TO_SAVE.keySet())
            {
                tasks.add( IngestedValues.copy( partitionName ));
            }

            for (Future<?> task : tasks)
            {
                try
                {
                    task.get();
                }
                catch ( InterruptedException e )
                {
                    LOGGER.warn("Thread Interrupted.");
                    Thread.currentThread().interrupt();
                }
                catch ( ExecutionException e )
                {
                    throw new IOException( "Error occurred while attempting to save ingested values.", e );
                }
            }
        }
    }

    public static Observation observed(final Double value)
    {
        Observation observation = new Observation();
        observation.value = value;
        return observation;
    }

    public static class Observation
    {
        private Observation(){}

        public Observation at(final TemporalAccessor observationTime)
        {
            this.observationTime = observationTime;
            return this;
        }

        public Observation forVariableAndFeatureID(Integer variableFeatureId)
        {
            this.variableFeatureId = variableFeatureId;
            return this;
        }

        public Observation measuredIn(final int measurementUnitId)
        {
            this.measurementUnitId = measurementUnitId;
            return this;
        }

        public Observation inSource(final int sourceId)
        {
            this.sourceId = sourceId;
            return this;
        }

        public Observation every(final Duration timestep)
        {
            this.timeStep = timestep;
            return this;
        }

        public Observation scaleOf(final Duration scalePeriod)
        {
            if (scalePeriod != null)
            {
                this.scalePeriod = scalePeriod;
            }
            return this;
        }

        public Observation scaledBy(final TimeScale.TimeScaleFunction scaleFunction)
        {
            if (scaleFunction != null)
            {
                this.scaleFunction = scaleFunction;
            }
            return this;
        }

        public void add()
        {            
            IngestedValues.addObservation(
                    this.variableFeatureId,
                    this.observationTime,
                    this.value,
                    this.measurementUnitId,
                    this.sourceId,
                    this.scalePeriod,
                    this.scaleFunction,
                    this.timeStep
            );
        }

        private Integer variableFeatureId;
        private TemporalAccessor observationTime;
        private Double value;
        private Integer measurementUnitId;
        private Integer sourceId;
        private Duration scalePeriod;
        private TimeScale.TimeScaleFunction scaleFunction;
        private Duration timeStep = Duration.ZERO;
    }
}
