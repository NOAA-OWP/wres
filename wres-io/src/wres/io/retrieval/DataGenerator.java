package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Future;

import wres.config.generated.Feature;
import wres.datamodel.sampledata.SampleData;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.util.IterationFailedException;
import wres.util.NotImplementedException;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 *
 * TODO: Find way to convert from an iterable to an iterator factory
 */
public class DataGenerator implements Iterable<Future<SampleData<?>>>
{
    /**
     * The feature whose {@link wres.datamodel.sampledata.SampleData} to generate
     */
    private final Feature feature;

    /**
     * The project that describes <i>how</i> to generate {@link wres.datamodel.sampledata.SampleData} retrieval tasks
     */
    private final Project project;

    /**
     * The path to where all generated {@link wres.datamodel.sampledata.SampleData} pairs should be stored
     */
    private final Path outputDirectoryForPairs;

    /**
     * Constructor
     * @param feature The feature whose {@link wres.datamodel.sampledata.SampleData} to generate
     * @param project The project that describes <i>how</i> to generate {@link wres.datamodel.sampledata.SampleData}
     *                retrieval tasks
     * @param outputDirectoryForPairs The path to where all generated {@link wres.datamodel.sampledata.SampleData}
     *                                pairs should be stored
     */
    public DataGenerator( Feature feature, Project project, Path outputDirectoryForPairs )
    {
        this.feature = feature;
        this.project = project;
        this.outputDirectoryForPairs = outputDirectoryForPairs;
    }

    @Override
    public Iterator<Future<SampleData<?>>> iterator()
    {
        Iterator<Future<SampleData<?>>> iterator;
        try
        {
            switch (this.project.getPairingMode())
            {
                case BASIC:
                    // Create an iterator that doesn't need to take complex pooling rules into account
                    iterator =  new BasicSampleDataIterator( this.feature,
                                                             this.project,
                                                             this.outputDirectoryForPairs);
                    break;
                case ROLLING:
                    // Create an iterator that needs to take groups of issue dates into account
                    iterator = new PoolingSampleDataIterator( this.feature,
                                                              this.project,
                                                              this.outputDirectoryForPairs);
                    break;
                case TIME_SERIES:
                    // Create an iterator that will lump all data for a feature together
                    iterator = new TimeSeriesSampleDataIterator( this.feature,
                                                                 this.project,
                                                                 this.outputDirectoryForPairs);
                    break;
                case BY_TIMESERIES:
                    // Create an iterator that will create a single SampleData
                    // instance for every ingested time series for the project and feature
                    iterator = new ByForecastSampleDataIterator( this.feature,
                                                                 this.project,
                                                                 this.outputDirectoryForPairs);
                    break;
                default:
                    throw new NotImplementedException( "The pairing mode of '" +
                                                       this.project.getPairingMode() +
                                                       "' has not been implemented." );
            }
        }
        catch (IOException e)
        {
            String message = "A MetricInputIterator could not be created for '"
                             + ConfigHelper.getFeatureDescription( this.feature )
                             + "'.";
            throw new IterationFailedException( message, e );
        }

        return iterator;
    }


}
