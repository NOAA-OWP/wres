package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.TimeHelper;

// TODO: Come up with handling for gridded data
public class TimeSeriesRetriever extends Retriever
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    private SampleMetadata metadata;

    TimeSeriesRetriever (
            final OrderedSampleMetadata sampleMetadata,
            final CacheRetriever getLeftValues,
            Path outputDirectoryForPairs
    )
    {
        super( sampleMetadata,
               getLeftValues,
               outputDirectoryForPairs );
    }

    
    @Override
    protected SampleData<?> createInput() throws IOException
    {
        TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
        builder.setMetadata( this.metadata );
        builder.setClimatology( this.getClimatology() );

        // One single-valued pair per ensemble member
        // TODO: retrieve single-valued pairs separately from ensemble pairs
        // so this mapping isn't needed
        for (Event<EnsemblePair> pair : this.getPrimaryPairs())
        {
            builder.addTimeSeries( Retriever.unwrapEnsembleEvent( pair ) );
        }

        return builder.build();
    }    
    

    @Override
    protected String getLoadScript( DataSourceConfig dataSourceConfig ) throws SQLException, IOException
    {
        return Scripter.getLoadScript( this.getSampleMetadata(), dataSourceConfig);
    }

    @Override
    protected SampleData<?> execute() throws IOException
    {
        this.createPairs();
        this.metadata = this.getSampleMetadata().getMetadata();

        if ( this.getPrimaryPairs().isEmpty())
        {
            LOGGER.warn("There are no pairs in window {} for {} at {}",
                         metadata.getTimeWindow(),
                         this.getProjectDetails().getRightVariableName(),
                         ConfigHelper.getFeatureDescription( this.getFeature() ));
        }
        else if (this.getPrimaryPairs().size() == 1)
        {
            LOGGER.warn("There is only one pair in window {} for {} at {}",
                         metadata.getTimeWindow(),
                         this.getProjectDetails().getRightVariableName(),
                         ConfigHelper.getFeatureDescription( this.getFeature() ));
        }

        return this.createInput();
    }

    private void createPairs() throws RetrievalFailedException
    {

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

            try (DataProvider data = script.buffer())
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

                    PivottedValues pivottedValues = this.formPivottedValues( validSeconds, lead, measurements );
                    EnsemblePair ensemblePair = this.getPair( pivottedValues );

                    if (ensemblePair == null)
                    {
                        LOGGER.trace("A pair of values could not be created.");
                        continue;
                    }

                    this.addPrimaryPair( Event.of( pivottedValues.getValidTime()
                                                                 .minus( pivottedValues.getLeadDuration() ),
                                                   pivottedValues.getValidTime(),
                                                   ensemblePair ) );
                }
            }
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "Values needed for evaluation could not be loaded.", e );
        }
    }

    /**
     * Manually creates a PivottedValues object rather than using an IngestedValueCollection
     * <p>
     *     Since all values in the retrieved set will always be in the same time series and
     *     scaling isn't really valid, the process of pivotting data into groups for scaling
     *     isn't necessary.
     * </p>
     * @param validSeconds The time in seconds representing when the values were valid
     * @param lead The lead duration in {@value wres.util.TimeHelper#LEAD_RESOLUTION}
     * @param value array of retrieved values to "pivot". There will only ever be one.
     * @return The set of pivotted values
     */
    private PivottedValues formPivottedValues( final long validSeconds, final int lead, final Double[] value)
    {
        Map<PivottedValues.EnsemblePosition, List<Double >> mappedResult = new TreeMap<>(  );
        mappedResult.put( new PivottedValues.EnsemblePosition( 0, 0 ), Arrays.asList( value ) );
        return new PivottedValues( Instant.ofEpochSecond( validSeconds ),
                                   Duration.of( lead, TimeHelper.LEAD_RESOLUTION ),
                                   mappedResult );
    }

    @Override
    protected Logger getLogger()
    {
        return TimeSeriesRetriever.LOGGER;
    }
}
