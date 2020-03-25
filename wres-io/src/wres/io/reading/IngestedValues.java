package wres.io.reading;

import java.time.Duration;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;
import wres.io.data.details.TimeSeries;
import wres.io.utilities.CopyException;
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

    private static final ConcurrentMap<String, Pair<CountDownLatch,CountDownLatch>> VALUES_SAVED_LATCHES = new ConcurrentHashMap<>();

    /** Guards VALUES_TO_SAVE and VALUES_SAVED_LATCHES and DataBuilders */
    private static final Object VALUES_TO_SAVE_LOCK = new Object();

    private static final String[] TIMESERIES_COLUMN_NAMES = {"timeseries_id", "lead", "series_value"};

    private static final Object OBSERVATIONS_LOCK = new Object();

    private static final String[] OBSERVATIONS_COLUMN_NAMES = {
            "variablefeature_id",
            "observation_time",
            "observed_value",
            "measurementunit_id",
            "source_id",
            "scale_period",
            "scale_function"
    };

    private static final String OBSERVATION_TABLE_NAME = "wres.Observation";

    private static Pair<CountDownLatch,CountDownLatch> OBSERVATIONS_SAVED_LATCHES = null;

    private static DataBuilder OBSERVATIONS = null;

    /**
     * Add an observation with possibly missing time scale information.
     *
     * @param systemSettings The system settings to use.
     * @param database THe database to use.
     * @param variableFeatureID the variableFeature identifier
     * @param observationTime the observation time
     * @param observedValue the observed value
     * @param measurementUnitID the measurement unit identifier
     * @param sourceID the source identifier
     * @param scalePeriod the optional period associated with the time scale (may be null)
     * @param scaleFunction the optional function associated with the time scale (may be null)
     * @return A synchronizer used to signal "waiting" left, "completed" right.
     */

    static Pair<CountDownLatch,CountDownLatch> addObservation(
            SystemSettings systemSettings,
            Database database,
            final int variableFeatureID,
            final TemporalAccessor observationTime,
            final Double observedValue,
            final int measurementUnitID,
            final int sourceID,
            final Duration scalePeriod,
            final TimeScale.TimeScaleFunction scaleFunction
    )
            throws IngestException
    {
        DataBuilder freshDataBuilder = DataBuilder.with( IngestedValues.OBSERVATIONS_COLUMN_NAMES );

        // The data builder to add to, whether existing or fresh.
        DataBuilder dataBuilderToUse;

        // The data builder removed from other threads' view, to be ingested.
        DataBuilder removedDataBuilder = null;

        // After copy has completed, this class will count down wasSavedLatch
        // This acts as a signal to callers that their data was saved.
        // When a task needs notification that its data was saved, it signals
        // by counting down taskWaitingLatch, which tells those tasks calling
        // this function to save the data earlier than it would have otherwise.
        CountDownLatch taskWaitingLatch = new CountDownLatch( 1 );
        CountDownLatch wasSavedLatch = new CountDownLatch( 1 );
        Pair<CountDownLatch,CountDownLatch> freshLatches = Pair.of( taskWaitingLatch,
                                                                    wasSavedLatch );

        // The existing latches to be got from the collection when put fails.
        Pair<CountDownLatch,CountDownLatch> latchesToUse = null;

        // When save is needed, removedLatches will be set.
        Pair<CountDownLatch,CountDownLatch> removedLatches = null;

        boolean doSave = false;

        // To avoid holding the VALUES_TO_SAVE_LOCK too long: after discovering
        // that the save needs to happen, set up new latches, then after
        // releasing the lock, do the save synchronously in this Thread rather
        // than in a Thread off to the side.
        synchronized ( OBSERVATIONS_LOCK )
        {
            if ( OBSERVATIONS == null )
            {
                OBSERVATIONS = freshDataBuilder;
            }

            dataBuilderToUse = OBSERVATIONS;

            if ( OBSERVATIONS_SAVED_LATCHES == null )
            {
                OBSERVATIONS_SAVED_LATCHES = freshLatches;
            }

            latchesToUse = OBSERVATIONS_SAVED_LATCHES;

            dataBuilderToUse.addRow();

            dataBuilderToUse.set( "variablefeature_id", variableFeatureID );
            dataBuilderToUse.set( "observation_time", observationTime );
            dataBuilderToUse.set( "observed_value", observedValue );
            dataBuilderToUse.set( "measurementunit_id", measurementUnitID );
            dataBuilderToUse.set( "source_id", sourceID );
            dataBuilderToUse.set( "scale_period", scalePeriod );
            dataBuilderToUse.set( "scale_function", scaleFunction );

            // Only apply the time scale information where defined
            // See #59536
            if ( Objects.nonNull( scalePeriod ) )
            {
                int scalePeriodAsInt = (int) TimeHelper.durationToLongUnits(
                        scalePeriod,
                        TimeHelper.LEAD_RESOLUTION );
                dataBuilderToUse.set( "scale_period", scalePeriodAsInt );
            }

            dataBuilderToUse.set( "scale_function", scaleFunction );


            int rowCount = dataBuilderToUse.getRowCount();
            int maximumCount = systemSettings.getMaximumCopies();

            if ( rowCount >= maximumCount )
            {
                // If the maximum number of values to copy has been reached, copy the
                // values
                LOGGER.trace( "Row count for observations is {}, larger than system setting {}",
                              rowCount, maximumCount );
                doSave = true;
            }


            // Can skip this step when row count condition already reached, or
            // when we are using a fresh latch, which is true when latchesToUse
            // is the same as freshLatches.
            if ( !latchesToUse.equals( freshLatches ) )
            {
                LOGGER.trace( "An existing latches object {} was found for observations, about to look for waiting tasks.",
                              latchesToUse );
                try
                {
                    // We could use getCount() <= 0, however, the documentation
                    // indicates that getCount() is for debugging, not control.
                    if ( latchesToUse.getLeft()
                                     .await( 0, TimeUnit.MICROSECONDS ) )
                    {
                        LOGGER.debug( "At least one task signaled it is waiting for data it sent to be ingested, will ingest {} observation rows.",
                                      rowCount );
                        doSave = true;
                    }
                }
                catch ( InterruptedException ie )
                {
                    // Usually warn, but this one happens on almost every cancel
                    LOGGER.debug( "Interrupted while awaiting wait signal from ingest task",
                                  ie );
                    Thread.currentThread().interrupt();
                }
            }

            if ( doSave )
            {
                removedDataBuilder = OBSERVATIONS;
                OBSERVATIONS = null;
                removedLatches = OBSERVATIONS_SAVED_LATCHES;
                OBSERVATIONS_SAVED_LATCHES = null;
                // It is expected that another Thread will set fresh values.
            }
        }

        // Save has been requested either from row count maximum or from some
        // other task waiting around for the data to be inserted.
        // It should be safe do this outside the synchronized block because
        // these objects will not be visible to threads that were waiting to
        // enter the above synchronized block.
        if ( doSave )
        {
            LOGGER.trace( "Attempting to ingest values for observations" );

            try
            {
                // Do copy synchronously here. Maybe modify other classes such
                // that synchronous copy is simpler? All that the asynchronous
                // copy does here is slow things down because Thread A awaits
                // Thread B which awaits Thread C to complete this ingest, where
                // it might be better to have Thread B complete the ingest and
                // leave Thread C to do other things. Even better might be to
                // let Thread A do the ingest since it is the one waiting.
                removedDataBuilder.build()
                                  .copy( database, OBSERVATION_TABLE_NAME );
            }
            catch( CopyException ce )
            {
                throw new IngestException( "Ingest of values to "
                                           + OBSERVATION_TABLE_NAME
                                           + " failed.", ce );
            }

            // Now that data has been copied, the associated latch should be
            // counted down to signal to Threads waiting to mark data as
            // completely ingested that they may safely do so.
            removedLatches.getRight()
                          .countDown();

            // Because ingest just happened for this partitionName and the
            // passed-in data, the caller need not wait anymore, (signified with
            // already-released latches).
            return Pair.of( new CountDownLatch( 0 ), new CountDownLatch( 0 ) );
        }
        else
        {
            LOGGER.trace( "Did not ingest values for observations, returning {}",
                          latchesToUse );
            // Because ingest did not happen for this partitionName, return the
            // latches associated with it.
            return latchesToUse;
        }
    }

    /**
     * Stores a time series value so that it may be copied to the database,
     * potentially later, but potentially in this Thread, especially if some
     * other Thread awaits it.
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param timeSeriesID The ID of the time series that the value belongs to
     * @param lead The lead time for the value
     * @param value The value itself
     * @return A synchronizer used to signal "waiting" left, "completed" right.
     * @throws IngestException when the proper partition could not be retrieved
     *                         or when the ingest fails.
     */
    public static Pair<CountDownLatch,CountDownLatch> addTimeSeriesValue( SystemSettings systemSettings,
                                                                          Database database,
                                                                          int timeSeriesID,
                                                                          int lead,
                                                                          Double value )
            throws IngestException
    {
        String partitionName = TimeSeries.getTimeSeriesValuePartition( lead );

        DataBuilder freshDataBuilder = DataBuilder.with( IngestedValues.TIMESERIES_COLUMN_NAMES );

        // The data builder to add to, whether existing or fresh.
        DataBuilder dataBuilderToUse;

        // The data builder removed from the collection, to be ingested.
        DataBuilder removedDataBuilder = null;

        // After copy has completed, this class will count down wasSavedLatch
        // This acts as a signal to callers that their data was saved.
        // When a task needs notification that its data was saved, it signals
        // by counting down taskWaitingLatch, which tells those tasks calling
        // this function to save the data earlier than it would have otherwise.
        CountDownLatch taskWaitingLatch = new CountDownLatch( 1 );
        CountDownLatch wasSavedLatch = new CountDownLatch( 1 );
        Pair<CountDownLatch,CountDownLatch> freshLatches = Pair.of( taskWaitingLatch,
                                                                    wasSavedLatch );

        // The existing latches to be got from the collection when put fails.
        Pair<CountDownLatch,CountDownLatch> latchesToUse = null;

        // When save is needed, removedLatches will be set.
        Pair<CountDownLatch,CountDownLatch> removedLatches = null;

        boolean doSave = false;

        // To avoid holding the VALUES_TO_SAVE_LOCK too long: after discovering
        // that the save needs to happen, set up new latches, then after
        // releasing the lock, do the save synchronously in this Thread rather
        // than in a Thread off to the side.
        synchronized ( VALUES_TO_SAVE_LOCK )
        {
            // Add a list for the values if it isn't present
            dataBuilderToUse = VALUES_TO_SAVE.putIfAbsent( partitionName,
                                                           freshDataBuilder );

            // When putIfAbsent returns null, it means it successfully put.
            if ( dataBuilderToUse == null )
            {
                dataBuilderToUse = freshDataBuilder;
            }

            // Add the values to the list for the partition
            dataBuilderToUse.addRow( timeSeriesID, lead, value );

            // Add latches for the values if not present
            latchesToUse = VALUES_SAVED_LATCHES.putIfAbsent( partitionName,
                                                             freshLatches );

            // When putIfAbsent returns null, it means it successfully put.
            if ( latchesToUse == null )
            {
                latchesToUse = freshLatches;
            }

            int rowCount = dataBuilderToUse.getRowCount();
            int maximumCount = systemSettings.getMaximumCopies();

            // If the maximum number of values to copy has been reached, copy the
            // values
            if ( rowCount >= maximumCount )
            {
                LOGGER.trace( "Row count for partition {} is {}, larger than system setting {}",
                              partitionName, rowCount, maximumCount );
                doSave = true;
            }

            // Can skip this step when row count condition already reached, or
            // when we are using a fresh latch, which is true when latchesToUse
            // is the same as freshLatches.
            if ( !latchesToUse.equals( freshLatches ) )
            {
                LOGGER.trace( "An existing latches object {} was found for partition {}, looking for waiting tasks.",
                              latchesToUse, partitionName );
                try
                {
                    // We could use getCount() <= 0, however, the documentation
                    // indicates that getCount() is for debugging, not control.
                    if ( latchesToUse.getLeft()
                                     .await( 0, TimeUnit.MICROSECONDS ) )
                    {
                        LOGGER.debug( "At least one task signaled it is waiting for ingest on {}, will ingest {} rows to {}.",
                                      latchesToUse, rowCount, partitionName );
                        doSave = true;
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.debug( "Interrupted while awaiting signal from ingester",
                                 ie );
                    Thread.currentThread().interrupt();
                }
            }

            if ( doSave )
            {
                removedLatches = VALUES_SAVED_LATCHES.remove( partitionName );
                removedDataBuilder = VALUES_TO_SAVE.remove( partitionName );
                // It is understood that another Thread will put fresh values.
            }
        }

        // Save has been requested either from row count maximum or from some
        // other task waiting around for the data to be inserted.
        // It should be safe do this outside the synchronized block because
        // these objects will not be visible to Threads that were waiting to
        // enter the above synchronized block.
        if ( doSave )
        {
            LOGGER.debug( "Attempting to ingest values for {} to {}",
                          removedLatches, partitionName );

            try
            {
                // Do copy synchronously here. Maybe modify other classes such
                // that synchronous copy is simpler? All that the asynchronous
                // copy does here is slow things down because Thread A awaits
                // Thread B which awaits Thread C to complete this ingest, where
                // it might be better to have Thread B complete the ingest and
                // leave Thread C to do other things. Even better might be to
                // let Thread A do the ingest since it is the one waiting.
                removedDataBuilder.build()
                                  .copy( database, partitionName );
            }
            catch( CopyException ce )
            {
                throw new IngestException( "Ingest of values to "
                                           + partitionName + " failed.", ce );
            }

            // Now that data has been copied, the associated latch should be
            // counted down to signal to tasks waiting to mark data as
            // completely ingested that they may safely do so.
            removedLatches.getRight()
                          .countDown();

            // Because ingest just happened for this partitionName and the
            // passed-in data, the caller need not wait anymore, (signified with
            // already-released latches).
            return Pair.of( new CountDownLatch( 0 ), new CountDownLatch( 0 ) );
        }
        else
        {
            LOGGER.trace( "Did not ingest values for partition {}, returning {}",
                          partitionName, latchesToUse );
            // Because ingest did not happen for this partitionName, return the
            // latches associated with it.
            return latchesToUse;
        }
    }


    /**
     * Call this when you are an ingester waiting to mark "completed" but no
     * other Thread has helped you out.
     *
     * @param database The database to use.
     * @param synchronizer the handle returned by this class to the ingester,
     *                     representing a superset of data the ingester sent.
     */
    static boolean flush( Database database,
                          Pair<CountDownLatch,CountDownLatch> synchronizer )
            throws IngestException
    {
        LOGGER.trace( "Began flush for synchronizer {}...", synchronizer );
        boolean isObservations = false;

        // The data builder removed from the collection, to be ingested.
        DataBuilder removedDataBuilder = null;

        // When save is needed, removedLatches will be set.
        Pair<CountDownLatch,CountDownLatch> removedLatches = null;

        String partitionName = null;

        // The call might be for obs, check that first.
        synchronized ( OBSERVATIONS_LOCK )
        {
            if ( OBSERVATIONS_SAVED_LATCHES != null &&
                 OBSERVATIONS_SAVED_LATCHES.equals( synchronizer ) )
            {
                partitionName = OBSERVATION_TABLE_NAME;
                removedDataBuilder = OBSERVATIONS;
                OBSERVATIONS = null;
                removedLatches = OBSERVATIONS_SAVED_LATCHES;
                OBSERVATIONS_SAVED_LATCHES = null;
                isObservations = true;
            }
        }


        if ( !isObservations )
        {
            synchronized ( VALUES_TO_SAVE_LOCK )
            {
                for ( Map.Entry<String,Pair<CountDownLatch,CountDownLatch>> latchPair : VALUES_SAVED_LATCHES.entrySet() )
                {
                    // "synchronizer" was an arg to this method
                    if ( latchPair.getValue().equals( synchronizer ) )
                    {
                        partitionName = latchPair.getKey();
                    }
                }

                // This means it was found, remove associated builder, latches.
                if ( partitionName != null )
                {
                    removedLatches = VALUES_SAVED_LATCHES.remove( partitionName );
                    removedDataBuilder = VALUES_TO_SAVE.remove( partitionName );
                }
            }
        }

        if ( partitionName == null )
        {
            LOGGER.debug( "Unable to find the synchronizer {} in the current lists (another task is saving, no need to flush here)",
                         synchronizer );
            return false;
        }

        // It should be safe do this outside the synchronized block because
        // these objects will not be visible to Threads that were waiting to
        // enter the above synchronized block.
        LOGGER.trace( "Attempting to flush values for partition {} with {}",
                      partitionName, removedDataBuilder );

        try
        {
            // Do copy synchronously here. Maybe modify other classes such
            // that synchronous copy is simpler? All that the asynchronous
            // copy does here is slow things down because Thread A awaits
            // Thread B which awaits Thread C to complete this ingest, where
            // it might be better to have Thread B complete the ingest and
            // leave Thread C to do other things. Even better might be to
            // let Thread A do the ingest since it is the one waiting.
            removedDataBuilder.build()
                              .copy( database, partitionName );
        }
        catch( CopyException ce )
        {
            throw new IngestException( "Ingest of values to "
                                       + partitionName + " failed.", ce );
        }

        // If these latches are not the same as what was passed, something went
        // wrong and the bug needs to be fixed.
        if ( !removedLatches.equals( synchronizer ) )
        {
            throw new IllegalStateException( "Issue when saving data: "
                                             + synchronizer + " was passed but "
                                             + removedLatches + " was removed.");
        }

        // Now that data has been copied, the associated latch should be
        // counted down to signal to tasks waiting to mark data as
        // completely ingested that they may safely do so.
        removedLatches.getRight()
                      .countDown();
        LOGGER.trace( "Completed flush for {}", synchronizer );

        return true;
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

        public Pair<CountDownLatch,CountDownLatch> add( SystemSettings systemSettings,
                                                        Database database ) throws IngestException
        {
            return IngestedValues.addObservation(
                    systemSettings,
                    database,
                    this.variableFeatureId,
                    this.observationTime,
                    this.value,
                    this.measurementUnitId,
                    this.sourceId,
                    this.scalePeriod,
                    this.scaleFunction
            );
        }

        private Integer variableFeatureId;
        private TemporalAccessor observationTime;
        private Double value;
        private Integer measurementUnitId;
        private Integer sourceId;
        private Duration scalePeriod;
        private TimeScale.TimeScaleFunction scaleFunction;
    }
}
