package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.Feature;
import wres.grid.client.Request;

class GridDataRequest implements Request
{
    public GridDataRequest()
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
        return this.paths;
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
