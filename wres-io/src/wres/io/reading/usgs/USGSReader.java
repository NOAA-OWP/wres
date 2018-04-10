package wres.io.reading.usgs;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.Executor;
import wres.io.concurrency.StatementRunner;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.ProjectDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.waterml.Response;
import wres.io.reading.waterml.WaterMLSource;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.io.utilities.IOExceptionalConsumer;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.TimeHelper;

public class USGSReader extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( USGSReader.class );

    public static final String EARLIEST_DATE = "2008-01-01T00:00:00Z";

    // There's a chance this operation will output the time in the wrong format
    public static final String LATEST_DATE = TimeHelper.convertDateToString( OffsetDateTime.now( ZoneId.of( "UTC" ) ) );

    /**
     * The amount of features that may be retrieved in a single USGS request
     */
    private static final int FEATURE_REQUEST_LIMIT = 100;

    public USGSReader( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        List<Collection<FeatureDetails>> featureDetails;
        List<IngestResult> results = new ArrayList<>();

        List<Future<IngestResult>> ingests = new ArrayList<>();

        try
        {
            // Break down locations to request in easily digestible blocks
            featureDetails = this.getFeatureRequestBlocks();

            if (featureDetails.size() == 0)
            {
                throw new IngestException( "No features to ingest from USGS "
                                           + "could be found." );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException( "The process used to determine what "
                                   + "features to request failed.", e );
        }

        // Request Observation data for each block of locations
        for (Collection<FeatureDetails> details : featureDetails)
        {
            USGSRegionSaver saver = new USGSRegionSaver( details,
                                                         this.getProjectConfig(),
                                                         this.getDataSourceConfig() );
            saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
            saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            saver.setOnUpdate( this::seriesEvaluated );

            Future<IngestResult> usgsIngest = Executor.submit( saver );

            ingests.add( usgsIngest );
        }

        // Ensure that each request is completed
        for (Future<IngestResult> ingest : ingests)
        {
            try
            {
                IngestResult result = ingest.get();

                if (result != null)
                {
                    results.add(result);
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                throw new IngestException( "USGS data could not be ingested.", e );
            }
        }

        // Throw an error if nothing was saved
        if (results.size() == 0)
        {
            throw new IngestException( "No data from any USGS features could "
                                       + "be saved for evaluation." );
        }

        return results;
    }

    private void seriesEvaluated(final TimeSeries series) throws IOException
    {
        if ( series.isPopulated())
        {
            LOGGER.info(
                    "A USGS time series has been parsed for location '{}' ({}/{})",
                    series.getSourceInfo().getSiteName(),
                    this.parsedLocationCount.incrementAndGet(),
                    this.totalLocationCount );
        }
        else
        {
            LOGGER.info("The location '{}' doesn't have valid USGS data. ({}/{})",
                        series.getSourceInfo().getSiteName(),
                        this.parsedLocationCount.incrementAndGet(),
                        this.totalLocationCount);
        }
    }

    private List<Collection<FeatureDetails>> getFeatureRequestBlocks()
            throws SQLException, IngestException
    {
        List<Collection<FeatureDetails>> requestBlocks = new ArrayList<>();

        Collection<FeatureDetails> details = Features.getAllDetails( this.getProjectConfig() );

        if (details.isEmpty())
        {
            throw new IngestException( "There are no USGS gages available to "
                                       + "ingest with the given configuration." );
        }

        this.totalLocationCount = details.size();

        if (details.size() > USGSReader.FEATURE_REQUEST_LIMIT)
        {
            List<FeatureDetails> block = new ArrayList<>();

            for (FeatureDetails featureDetails : details)
            {
                if (!Strings.isNumeric( featureDetails.getGageID() ))
                {
                    LOGGER.warn( "{} has an invalid gage id", featureDetails );
                }

                block.add(featureDetails);

                if (block.size() > USGSReader.FEATURE_REQUEST_LIMIT)
                {
                    requestBlocks.add( block );
                    block = new ArrayList<>();
                }
            }

            if (block.size() > 0)
            {
                requestBlocks.add( block );
            }
        }
        else
        {
            requestBlocks.add( details );
        }

        return requestBlocks;
    }

    @Override
    protected Logger getLogger()
    {
        return USGSReader.LOGGER;
    }

    private int totalLocationCount;
    private AtomicInteger parsedLocationCount = new AtomicInteger( 0 );
}
