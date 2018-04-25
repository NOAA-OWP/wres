package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.Feature;

/**
 * Prototype Interface for requesting grid data
 */
public interface Request
{
    void addIndex(final Integer xIndex, final Integer yIndex);
    void addPath(String path);
    void addFeature(Feature feature);
    void setEarliestIssueTime( Instant earliestIssueTime );
    void setLatestIssueTime( Instant latestIssueTime );
    void setEarliestValidTime( Instant earliestValidTime );
    void setLatestValidTime( Instant latestValidTime );
    void setEarliestLead( Duration earliestLead );
    void setLatestLead( Duration latestLead );
    void setVariableName( String variableName );
    void setIsForecast(Boolean isForecast);

    List<Pair<Integer, Integer>> getIndices();
    List<String> getPaths();
    List<Feature> getFeatures();
    Instant getEarliestIssueTime();
    Instant getLatestIssueTime();
    Instant getEarliestValidTime();
    Instant getLatestValidTime();
    Duration getEarliestLead();
    Duration getLatestLead();
    String getVariableName();
    Boolean getIsForecast();
}
