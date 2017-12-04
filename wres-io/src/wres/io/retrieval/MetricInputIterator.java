package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Projects;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.Strings;

abstract class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    // Setting the initial window number to -1 ensures that our windows are 0 indexed
    private int windowNumber = -1;
    private Integer windowCount;

    private final Feature feature;

    private final ProjectDetails projectDetails;
    private final List<String> projectSources;
    private NavigableMap<String, Double> leftHandMap;
    private VectorOfDoubles climatology;
    private String earliestObservationDate;

    protected int getWindowNumber()
    {
        return this.windowNumber;
    }

    protected void incrementWindowNumber()
    {
        this.windowNumber++;
    }

    protected Integer getWindowCount() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        if (this.windowCount == null)
        {
            this.windowCount = this.calculateWindowCount();
        }

        return this.windowCount;
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    protected void addLeftHandValue(String date, Double measurement)
    {
        if (this.leftHandMap == null)
        {
            this.leftHandMap = new TreeMap<>(  );
        }

        this.leftHandMap.put( date, measurement );
    }

    protected VectorOfDoubles getClimatology() throws IOException
    {
        if (this.getProjectDetails().usesProbabilityThresholds() && this.climatology == null)
        {
            ClimatologyBuilder climatologyBuilder = new ClimatologyBuilder( this.getProjectDetails(),
                                                                            this.getProjectDetails().getLeft(),
                                                                            this.getFeature() );
            this.climatology = climatologyBuilder.getClimatology();
        }

        return this.climatology;
    }

    public MetricInputIterator( final ProjectConfig projectConfig,
                                final Feature feature,
                                final List<String> projectSources )
            throws SQLException, NoDataException,
            InvalidPropertiesFormatException
    {
        this.projectDetails = Projects.getProject( projectConfig );
        this.feature = feature;
        this.projectSources = projectSources;

        this.createLeftHandCache();

        if (this.leftHandMap == null || this.leftHandMap.size() == 0)
        {
            throw new NoDataException( "No data for the left hand side of " +
                                       " the evaluation could be loaded. " +
                                       "Please check your specifications." );
        }

        // TODO: This needs a better home
        // x2; 1 step for retrieval, 1 step for calculation
        ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) * 2 );
    }

    @Override
    public boolean hasNext()
    {
        boolean next = false;

        try
        {
            if (ConfigHelper.isForecast( this.getRight() ))
            {
                int nextWindowNumber = this.getWindowNumber() + 1;
                int beginning = this.getProjectDetails().getLead(nextWindowNumber) +
                                   this.getProjectDetails().getLeadOffset( this.getFeature() );
                int end = this.getProjectDetails().getLead(nextWindowNumber + 1) +
                             this.getProjectDetails().getLeadOffset( this.getFeature() );

                next = beginning < this.getProjectDetails().getLastLead( this.getFeature() ) &&
                       end >= this.getProjectDetails().getMinimumLeadHour() &&
                       end <= this.getProjectDetails().getLastLead( this.getFeature() );
            }
            else
            {
                next = this.getWindowNumber() == -1;
            }

            if (!next && this.getWindowNumber() < 0)
            {
                String message = "Due to the configuration of this project,";
                message += " there are no valid windows to evaluate. ";
                message += "The range of all lead times go from {} to ";
                message += "{}, and the size of the window is {} hours. ";
                message += "Based on the difference between the initialization ";
                message += "of the left and right data sets, there is a {} ";
                message += "hour offset. This puts an initial window out ";
                message += "range of the specifications.";

                this.getLogger().error(message,
                                       this.getProjectDetails().getMinimumLeadHour(),
                                       this.getProjectDetails().getLastLead( feature ),
                                       this.getProjectDetails().getWindowWidth(),
                                       this.getProjectDetails().getLeadOffset( this.getFeature() ));
            }
        }
        catch ( SQLException | InvalidPropertiesFormatException e )
        {
            this.getLogger().error( Strings.getStackTrace( e ));
        }
        catch ( NoDataException e )
        {
            this.getLogger().error("The last lead time for pairing could not be " +
                                   "determined; There is no data to pair and " +
                                   "iterate over.");
        }

        return next;
    }

    @Override
    public Future<MetricInput<?>> next()
    {
        Future<MetricInput<?>> nextInput = null;

        if (this.hasNext())
        {
            this.incrementWindowNumber();
            try
            {
                // TODO: Pass the leftHandMap instead of the function
                InputRetriever retriever = new InputRetriever( this.getProjectDetails(),
                                                               (String firstDate, String lastDate) -> {
                                                                   return Collections
                                                                           .getValuesInRange( this.leftHandMap, firstDate, lastDate );
                                                               },
                                                               this.projectSources );
                retriever.setFeature(feature);
                retriever.setClimatology( this.getClimatology() );
                retriever.setProgress( this.getWindowNumber() );
                retriever.setLeadOffset( this.getProjectDetails()
                                             .getLeadOffset( this.getFeature() ) );
                retriever.setOnRun( ProgressMonitor.onThreadStartHandler() );
                retriever.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

                nextInput = Database.submit(retriever);
            }
            catch ( SQLException | IOException e )
            {
                this.getLogger().error( Strings.getStackTrace( e ) );
            }
        }

        if (nextInput == null)
        {
            throw new NoSuchElementException( "There are no more windows to evaluate" );
        }

        return nextInput;
    }

    protected DataSourceConfig getLeft()
    {
        return this.getProjectDetails().getLeft();
    }

    protected DataSourceConfig getRight()
    {
        return this.getProjectDetails().getRight();
    }

    protected DataSourceConfig getBaseline()
    {
        return this.getProjectDetails().getBaseline();
    }

    protected int getFirstLeadInWindow()
            throws InvalidPropertiesFormatException, NoDataException,
            SQLException
    {
        return ( this.getWindowNumber() * this.getProjectDetails().getWindowWidth()) +
               this.getProjectDetails().getLeadOffset( this.getFeature() );
    }

    abstract int calculateWindowCount()
            throws SQLException,
            InvalidPropertiesFormatException,
            NoDataException;

    abstract void createLeftHandCache() throws SQLException, NoDataException;

    abstract Logger getLogger();
}
