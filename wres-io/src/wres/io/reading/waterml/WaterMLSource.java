package wres.io.reading.waterml;

import java.io.IOException;
import java.sql.SQLException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.math3.util.Precision.EPSILON;

import wres.io.concurrency.CopyExecutor;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.functional.ExceptionalConsumer;
import wres.io.utilities.ScriptBuilder;
import wres.util.NotImplementedException;

/**
 * Saves WaterML Response objects to the database
 *
 * TODO: Fix database deadlocking issues that prevent heavy concurrency
 */
public class WaterMLSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WaterMLSource.class);

    private static final String OBSERVATION_HEADER = "wres.Observation ("
                                                     + "variablefeature_id, "
                                                     + "observation_id, "
                                                     + "observation_time, "
                                                     + "observed_value, "
                                                     + "measurementunit_id, "
                                                     + "source_id)";
    private static final String DELIMITER = "|";
    private static final String COPY_NULL = "\\N";

    private final int sourceId;
    private final Response waterML;
    private final SortedMap<String, Integer> variableFeatureIDs;
    private final int variableId;
    private ExceptionalConsumer<TimeSeries, IOException> invalidSeriesHandler;
    private ExceptionalConsumer<TimeSeries, IOException> seriesReadCompleteHandler;
    private final int waterMLMeasurementId;
    private ScriptBuilder copyScript;
    private int copyCount = 0;
    private String copyHeader;


    public WaterMLSource( Response waterML,
                          int sourceId,
                          int waterMLMeasurementId,
                          int variableId)
    {
        this.waterML = waterML;
        this.variableFeatureIDs = new TreeMap<>();
        this.sourceId = sourceId;
        this.waterMLMeasurementId = waterMLMeasurementId;
        this.variableId = variableId;
    }

    public void setInvalidSeriesHandler(ExceptionalConsumer<TimeSeries, IOException> handler)
    {
        this.invalidSeriesHandler = handler;
    }

    public void setSeriesReadCompleteHandler(ExceptionalConsumer<TimeSeries, IOException> handler)
    {
        this.seriesReadCompleteHandler = handler;
    }

    private void handleInvalidSeries(TimeSeries invalidSeries)
            throws IOException
    {
        if (this.invalidSeriesHandler != null)
        {
            this.invalidSeriesHandler.accept( invalidSeries );
        }
    }

    private void handleReadComplete(TimeSeries series) throws IOException
    {
        if (this.seriesReadCompleteHandler != null)
        {
            this.seriesReadCompleteHandler.accept( series );
        }
    }

    public int readObservationResponse() throws IOException
    {
        this.copyHeader = WaterMLSource.OBSERVATION_HEADER;
        int readSeriesCount = 0;

        if (this.waterML.getValue().getNumberOfPopulatedTimeSeries() > 0)
        {

            LOGGER.debug(
                    "There are a grand total of {} different locations "
                    + "that we want to save data to.",
                    this.waterML.getValue()
                                .getNumberOfPopulatedTimeSeries() );

            for ( TimeSeries series : this.waterML.getValue().getTimeSeries() )
            {
                boolean validSeries = this.readObservationSeries( series );

                if ( validSeries )
                {
                    readSeriesCount++;
                }
            }

            LOGGER.debug( "Data for {} different locations have been saved.",
                          readSeriesCount );

            try
            {
                this.performCopy();
            }
            catch ( SQLException e )
            {
                e.printStackTrace();
            }
        }

        return readSeriesCount;
    }

    private boolean readObservationSeries( TimeSeries series )
            throws IOException
    {
        if (!series.isPopulated())
        {
            this.handleInvalidSeries( series );
            return false;
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            if (valueSet.getValue().length == 0)
            {
                continue;
            }

            for (TimeSeriesValue value : valueSet.getValue())
            {
                try
                {
                    Double readValue = value.getValue();

                    if (series.getVariable().getNoDataValue() != null &&
                        Precision.equals( readValue, series.getVariable().getNoDataValue(), EPSILON))
                    {
                        readValue = null;
                    }

                    this.addObservationValue( series.getSourceInfo().getSiteCode()[0].getValue(),
                                   value.getDateTime(),
                                   readValue);
                }
                catch ( SQLException e )
                {
                    throw new IOException( e );
                }
            }
        }

        this.handleReadComplete( series );
        return true;
    }

    private void addObservationValue( String gageID, String observationTime, Double value)
            throws SQLException
    {
        if (this.copyScript == null)
        {
            this.copyScript = new ScriptBuilder(  );
        }

        this.copyScript.add(this.getVariableFeatureID( gageID )).add(DELIMITER)
                       .add("'" + observationTime + "'").add(DELIMITER);

        if (value == null)
        {
            this.copyScript.add(COPY_NULL).add(DELIMITER);
        }
        else
        {
            this.copyScript.add( value ).add( DELIMITER );
        }

        this.copyScript.add(this.waterMLMeasurementId).add(DELIMITER)
                       .addLine(this.sourceId);

        this.copyCount++;

        if ( this.copyCount >= SystemSettings.getMaximumCopies())
        {
            this.performCopy();
        }
    }

    private void performCopy() throws SQLException
    {
        if (this.copyCount > 0)
        {
            CopyExecutor copier = new CopyExecutor( this.copyHeader, this.copyScript.toString(), DELIMITER );

            // TODO: If we want to only update the ProgressMonitor for files, remove these handlers
            // Tell the copier to increase the number representing the
            // total number of operations to perform when the thread starts.
            // It is debatable whether we should increase the number in this
            // thread or in the thread operating on the actual database copy
            // statement
            copier.setOnRun( ProgressMonitor.onThreadStartHandler() );

            // Tell the copier to inform the ProgressMonitor that work has been
            // completed when the thread has finished
            copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

            // Send the copier to the Database handler's task queue and add
            // the resulting future to our list of copy operations
            Database.ingest( copier );

            // Reset the values to copy
            this.copyScript = new ScriptBuilder(  );

            // Reset the count of values to copy
            this.copyCount = 0;

        }
    }

    private int getVariableFeatureID(String gageId) throws SQLException
    {
        if (!this.variableFeatureIDs.containsKey( gageId ))
        {
            FeatureDetails feature = Features.getDetailsByGageID( gageId );
            this.variableFeatureIDs.put(
                    gageId,
                    Features.getVariableFeatureByFeature( feature, this.variableId )
            );
        }
        return this.variableFeatureIDs.get(gageId);
    }
}
