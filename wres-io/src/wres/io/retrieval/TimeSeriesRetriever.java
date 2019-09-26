package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.retrieval.scripting.Scripter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
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
        List<Pair<Instant,Event<Pair<Double,Double>>>> events = new ArrayList<>();
        this.getPrimaryPairs().forEach( next -> events.addAll( Retriever.unwrapEnsembleEvent( next ) ) );       
        List<TimeSeries<Pair<Double,Double>>> timeSeries = Retriever.getTimeSeriesFromListOfEvents( events );
        timeSeries.forEach( builder::addTimeSeries );

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
                    final Integer[] ensembles = data.getIntegerArray( "members" );

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

                    if (measurements.length > 1 || ensembles.length > 1)
                    {
                        LOGGER.debug( "More than one value was found in timeseries id: " +
                                      this.getSampleMetadata().getSampleNumber() + " at lead " + lead );
                    }

                    Map<PivottedValues.EnsemblePosition, List<Double >> mappedResult = new TreeMap<>(  );

                    // 99.9% There will only be one value to store; this is a safety measure
                    for (int measurementPosition = 0; measurementPosition < Math.min(measurements.length, ensembles.length); ++measurementPosition)
                    {
                        Double measurement = measurements[measurementPosition];

                        if (measurement != null)
                        {
                            measurement = this.convertMeasurement( measurements[measurementPosition],
                                                                         data.getInt( "measurementunit_id" ) );

                            if ( this.getProjectDetails().getMaximumValue() < measurement )
                            {
                                if ( this.getProjectDetails().getDefaultMaximumValue() != null )
                                {
                                    measurement = this.getProjectDetails().getDefaultMaximumValue();
                                }
                                else
                                {
                                    measurement = Double.NaN;
                                }
                            }
                            else if ( this.getProjectDetails().getMinimumValue() > measurement )
                            {
                                if ( this.getProjectDetails().getDefaultMinimumValue() != null )
                                {
                                    measurement = this.getProjectDetails().getDefaultMinimumValue();
                                }
                                else
                                {
                                    measurement = Double.NaN;
                                }
                            }
                        }
                        else
                        {
                            measurement = Double.NaN;
                        }

                        List<Double> value = new ArrayList<>();
                        value.add( measurement );
                        mappedResult.put(
                                new PivottedValues.EnsemblePosition( measurementPosition, ensembles[measurementPosition] ),
                                value
                        );
                    }

                    PivottedValues pivottedValues = new PivottedValues( Instant.ofEpochSecond( validSeconds ),
                                               Duration.of( lead, TimeHelper.LEAD_RESOLUTION ),
                                               mappedResult );

                    Pair<Double, Ensemble> ensemblePair = this.getPair( pivottedValues );

                    if (ensemblePair == null)
                    {
                        LOGGER.trace("A pair of values could not be created.");
                        continue;
                    }

                    this.addPrimaryPair( Pair.of( pivottedValues.getValidTime()
                                                                .minus( pivottedValues.getLeadDuration() ),
                                                  Event.of( pivottedValues.getValidTime(),
                                                            ensemblePair ) ) );
                }
            }
        }
        catch ( SQLException e )
        {
            throw new RetrievalFailedException( "Values needed for evaluation could not be loaded.", e );
        }
    }

    @Override
    protected Logger getLogger()
    {
        return TimeSeriesRetriever.LOGGER;
    }
}
