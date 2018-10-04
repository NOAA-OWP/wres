package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.data.details.TimeSeries;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.pair.PairWriter;
import wres.io.writing.pair.SharedWriterManager;
import wres.util.TimeHelper;

// TODO: Come up with handling for gridded data
public class TimeSeriesRetriever extends Retriever
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    TimeSeriesRetriever (
            final ProjectDetails projectDetails,
            final CacheRetriever getLeftValues,
            final int timeSeriesID,
            SharedWriterManager sharedWriterManager,
            Path outputDirectoryForPairs
    )
    {
        super( projectDetails,
               getLeftValues,
               sharedWriterManager,
               outputDirectoryForPairs );
        this.timeSeriesID = timeSeriesID;
    }

    @Override
    void writePair( SharedWriterManager sharedWriterManager,
                    Path outputDirectory,
                    ForecastedPair pair,
                    DataSourceConfig dataSourceConfig )
    {
        List<DestinationConfig> destinationConfigs = this.getProjectDetails().getPairDestinations();

        for (DestinationConfig destination : destinationConfigs)
        {
            PairWriter writer = new PairWriter.Builder()
                    .setDestinationConfig( destination )
                    .setDate( pair.getValidTime() )
                    .setFeature( this.getFeature() )
                    .setLeadIteration( this.getLeadIteration() )
                    .setPair(pair.getValues())
                    .setProjectDetails( this.getProjectDetails() )
                    .setLead( pair.getLeadDuration() )
                    .setOutputDirectory( outputDirectory )
                    .build();

            sharedWriterManager.accept( writer );
        }
    }

    @Override
    protected SampleMetadata buildMetadata( ProjectConfig projectConfig, boolean isBaseline ) throws IOException
    {
        MeasurementUnit dim = MeasurementUnit.of( this.getProjectDetails().getDesiredMeasurementUnit() );
        String variableIdentifier = ConfigHelper.getVariableIdFromProjectConfig( projectConfig, isBaseline );
        DatasetIdentifier datasetIdentifier = DatasetIdentifier.of(
                this.getGeospatialIdentifier(),
                variableIdentifier,
                this.getProjectDetails().getRight().getLabel()
        );

        TemporalAccessor referenceTime = TimeHelper.convertStringToDate( this.timeSeries.getInitializationDate() );
        Instant reference = Instant.from( referenceTime );

        int earliestLead = this.getProjectDetails().getMinimumLead();

        if (earliestLead == Integer.MIN_VALUE)
        {
            earliestLead = 0;
        }

        int latestLead = this.getProjectDetails().getMaximumLead();

        if (latestLead == Integer.MAX_VALUE)
        {
            latestLead = this.timeSeries.getHighestLead();
        }

        TimeWindow window = TimeWindow.of(
                reference,
                reference,
                ReferenceTime.ISSUE_TIME,
                Duration.of( earliestLead, TimeHelper.LEAD_RESOLUTION),
                Duration.of( latestLead, TimeHelper.LEAD_RESOLUTION)
        );

        // Build the metadata
        SampleMetadataBuilder builder = new SampleMetadataBuilder().setMeasurementUnit( dim )
                                                                   .setIdentifier( datasetIdentifier )
                                                                   .setTimeWindow( window )
                                                                   .setProjectConfig( projectConfig );

        // Add the time-scale information to the metadata.
        // Initially, this comes from the desiredTimeScale.
        // TODO: when the project declaration is undefined,
        // determine the Least Common Scale and populate the
        // metadata with that - that relies on #54415.
        // See #44539 for an overview.
        if ( Objects.nonNull( projectConfig.getPair() )
             && Objects.nonNull( projectConfig.getPair().getDesiredTimeScale() ) )
        {
            builder.setTimeScale( TimeScale.of( projectConfig.getPair().getDesiredTimeScale() ) );
        }

        return builder.build();
    }

    @Override
    protected SampleData<?> createInput() throws IOException
    {
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
        builder.setMetadata( this.metadata );
        builder.setClimatology( this.getClimatology() );

        for (ForecastedPair pair : this.getPrimaryPairs())
        {
            for (SingleValuedPair singleValuedPair : pair.getSingleValuedPairs())
            {
                builder.addTimeSeriesData(
                        pair.getBasisTime(),
                        Collections.singletonList(
                                Event.of( pair.getValidTime(),  singleValuedPair)
                        )
                );
            }
        }

        return builder.build();
    }

    @Override
    protected String getLoadScript( DataSourceConfig dataSourceConfig ) throws SQLException, IOException
    {
        this.script =  Scripter.getLoadScript( this.getProjectDetails(),
                                             dataSourceConfig,
                                             getFeature(),
                                             this.timeSeriesID,
                                             -1 );
        return this.script;
    }

    @Override
    protected SampleData<?> execute() throws IOException
    {
        try
        {
            this.timeSeries = TimeSeries.getByID( this.timeSeriesID );
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException(
                    "Information about what time series to retrieve could not be loaded.",
                    e
            );
        }
        this.createPairs();
        this.metadata = this.buildMetadata( this.getProjectDetails().getProjectConfig(), false );

        if ( this.getPrimaryPairs().isEmpty())
        {
            LOGGER.debug( "Data could not be loaded for {}", metadata.getTimeWindow() );
            LOGGER.debug( "The script used was:" );
            LOGGER.debug( "{}{}", NEWLINE, this.script );
            throw new NoDataException( "No data could be retrieved for Metric calculation for window " +
                                       metadata.getTimeWindow().toString() +
                                       " for " +
                                       this.getProjectDetails().getRightVariableName() +
                                       " at " +
                                       ConfigHelper.getFeatureDescription( this.getFeature() ) );
        }
        else if (this.getPrimaryPairs().size() == 1)
        {
            LOGGER.trace("There is only one pair in window {} for {} at {}",
                         metadata.getTimeWindow(),
                         this.getProjectDetails().getRightVariableName(),
                         ConfigHelper.getFeatureDescription( this.getFeature() ));
        }

        return this.createInput();
    }

    private void createPairs() throws RetrievalFailedException
    {
        Connection connection = null;

        DataScripter script;

        try
        {
            script = new DataScripter( this.getLoadScript( this.getProjectDetails().getRight() ) );
        }
        catch ( SQLException | IOException e )
        {
            throw new RetrievalFailedException( "The logic used to retrieve time series "
                                                + "values could not be formed.", e );
        }

        try
        {
            connection = Database.getConnection();

            try (DataProvider data = script.getData( connection ))
            {
                while (data.next())
                {
                    final long validSeconds = data.getLong( "value_date" );
                    final int lead = data.getInt( "lead" );
                    final Double[] measurements = data.getDoubleArray( "measurements" );

                    if (measurements[0] != null)
                    {
                        measurements[0] = this.convertMeasurement( measurements[0], data.getInt( "measurementunit_id" ) );

                        if (this.getProjectDetails().getMaximumValue() < measurements[0])
                        {
                            if (this.getProjectDetails().getDefaultMaximumValue() != null)
                            {
                                measurements[0] = this.getProjectDetails().getDefaultMaximumValue();
                            }
                            else
                            {
                                measurements[0] = Double.NaN;
                            }
                        }
                        else if (this.getProjectDetails().getMinimumValue() > measurements[0])
                        {
                            if (this.getProjectDetails().getDefaultMinimumValue() != null)
                            {
                                measurements[0] = this.getProjectDetails().getDefaultMinimumValue();
                            }
                            else
                            {
                                measurements[0] = Double.NaN;
                            }
                        }
                    }

                    CondensedIngestedValue condensedIngestedValue = this.formIngestedValue( validSeconds, lead, measurements );
                    EnsemblePair ensemblePair = this.getPair( condensedIngestedValue );

                    if (ensemblePair == null)
                    {
                        LOGGER.trace("A pair of values could not be created.");
                        continue;
                    }
                    
                    ForecastedPair pair = new ForecastedPair( lead,
                                                              condensedIngestedValue.validTime,
                                                              ensemblePair);
                    this.writePair( super.getSharedWriterManager(),
                                    super.getOutputDirectoryForPairs(),
                                    pair,
                                    super.getProjectDetails().getRight() );
                    this.addPrimaryPair( pair );
                }
            }
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "Values needed for evaluation could not be loaded.", e );
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }
    }

    private CondensedIngestedValue formIngestedValue(final long validSeconds, final int lead, final Double[] value)
    {
        Map<Integer, List<Double>> mappedResult = new TreeMap<>(  );
        mappedResult.put( 0, Arrays.asList( value ) );
        return new CondensedIngestedValue( Instant.ofEpochSecond( validSeconds ), lead, mappedResult );
    }

    @Override
    protected Logger getLogger()
    {
        return TimeSeriesRetriever.LOGGER;
    }

    private String script;
    private SampleMetadata metadata;
    private TimeSeries timeSeries;
    private final int timeSeriesID;
}
