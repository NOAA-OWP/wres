package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.Feature;
import wres.grid.client.Request;

class GridDataRequest implements Request
{
    public GridDataRequest()
    {
        this.features = new ArrayList<>(  );
        this.paths = new ArrayList<>(  );
        this.indices = new ArrayList<>(  );
    }

    public void addIndex(final Integer xIndex, final Integer yIndex)
    {
        this.indices.add(Pair.of(xIndex, yIndex));
    }

    public void addPath(String path)
    {
        this.paths.add(path);
    }

    public void addFeature(Feature feature)
    {
        this.features.add(feature);
    }

    public void setEarliestIssueTime( Instant earliestIssueTime )
    {
        this.earliestIssueTime = earliestIssueTime;
    }

    public void setLatestIssueTime( Instant latestIssueTime )
    {
        this.latestIssueTime = latestIssueTime;
    }

    public void setEarliestValidTime( Instant earliestValidTime )
    {
        this.earliestValidTime = earliestValidTime;
    }

    public void setLatestValidTime( Instant latestValidTime )
    {
        this.latestValidTime = latestValidTime;
    }

    public void setEarliestLead( Duration earliestLead )
    {
        this.earliestLead = earliestLead;
    }

    public void setLatestLead( Duration latestLead )
    {
        this.latestLead = latestLead;
    }

    public void setVariableName( String variableName )
    {
        this.variableName = variableName;
    }

    public void setIsForecast(Boolean isForecast)
    {
        this.isForecast = isForecast;
    }

    private final List<Pair<Integer, Integer>> indices;
    private final List<String> paths;
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
