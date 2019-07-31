package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringJoiner;

import wres.config.generated.Feature;

/**
 * TODO: JBr - make this class immutable, probably using a builder given the number of instance variables.
 */

class GridDataRequest implements Request
{
    GridDataRequest()
    {
        this.features = new ArrayList<>(  );
        this.paths = new LinkedList<>(  );
    }

    @Override
    public void addPath(String path)
    {
        this.paths.add(path);
    }

    @Override
    public void addFeature(Feature feature)
    {
        this.features.add(feature);
    }

    @Override
    public void setEarliestIssueTime( Instant earliestIssueTime )
    {
        this.earliestIssueTime = earliestIssueTime;
    }

    @Override
    public void setLatestIssueTime( Instant latestIssueTime )
    {
        this.latestIssueTime = latestIssueTime;
    }

    @Override
    public void setEarliestValidTime( Instant earliestValidTime )
    {
        this.earliestValidTime = earliestValidTime;
    }

    @Override
    public void setLatestValidTime( Instant latestValidTime )
    {
        this.latestValidTime = latestValidTime;
    }

    @Override
    public void setEarliestLead( Duration earliestLead )
    {
        this.earliestLead = earliestLead;
    }

    @Override
    public void setLatestLead( Duration latestLead )
    {
        this.latestLead = latestLead;
    }

    @Override
    public void setVariableName( String variableName )
    {
        this.variableName = variableName;
    }

    @Override
    public void setIsForecast(Boolean isForecast)
    {
        this.isForecast = isForecast;
    }

    @Override
    public Queue<String> getPaths()
    {
        // Do not expose the internal container, create a new one
        return new LinkedList<>(  this.paths );
    }

    @Override
    public List<Feature> getFeatures()
    {
        return Collections.unmodifiableList( this.features );
    }

    @Override
    public Instant getEarliestIssueTime()
    {
        return this.earliestIssueTime;
    }

    @Override
    public Instant getLatestIssueTime()
    {
        return this.latestIssueTime;
    }

    @Override
    public Instant getEarliestValidTime()
    {
        return this.earliestValidTime;
    }

    @Override
    public Instant getLatestValidTime()
    {
        return this.latestValidTime;
    }

    @Override
    public Duration getEarliestLead()
    {
        return this.earliestLead;
    }

    @Override
    public Duration getLatestLead()
    {
        return this.latestLead;
    }

    @Override
    public String getVariableName()
    {
        return this.variableName;
    }

    @Override
    public Boolean getIsForecast()
    {
        return this.isForecast;
    }
    
    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        joiner.add( "GridDataRequest instance " + this.hashCode() + ": { " );
        joiner.add( "    variableName: " + this.getVariableName() + "," );
        joiner.add( "    earliestIssueTime: " + this.getEarliestIssueTime() + "," );
        joiner.add( "    latestIssueTime: " + this.getLatestIssueTime() + "," );
        joiner.add( "    earliestValidTime: " + this.getEarliestValidTime() + "," );
        joiner.add( "    latestValidTime: " + this.getLatestValidTime() + "," );
        joiner.add( "    earliestLead: " + this.getEarliestLead() + "," );
        joiner.add( "    latestLead: " + this.getLatestLead() + "," );
        joiner.add( "    isForecast: " + this.getIsForecast() + "," );
        joiner.add( "    features: " + this.getFeatures() + "," );
        joiner.add( "    paths: " + this.getPaths() );
        joiner.add( "}" );

        return joiner.toString();
    }

    private final Queue<String> paths;
    private final List<Feature> features;
    private String variableName;
    private Instant earliestIssueTime;
    private Instant latestIssueTime;
    private Instant earliestValidTime;
    private Instant latestValidTime;
    private Duration earliestLead;
    private Duration latestLead;
    private Boolean isForecast;
}
