package wres.reading.wrds.thresholds;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.reading.wrds.geography.Location;
import wres.datamodel.units.UnitMapper;
import wres.statistics.generated.Threshold;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Builder;

/**
 * Extracts a mapping between features and their thresholds from a WRDS formatted thresholds document
 */
@Builder( toBuilder = true )
class ThresholdExtractor
{
    /** The Threshold data from WRDS. */
    private final ThresholdResponse response;

    /** The publisher of the threshold data. */
    private final String provider;

    /** The publisher of the rating curve for the thresholds. */
    private final String ratingProvider;

    /** The type of data for the threshold. */
    @Builder.Default
    private final ThresholdType type = ThresholdType.FLOW;

    /** The boolean operator to apply to loaded values. */
    @Builder.Default
    private final ThresholdOperator operator = ThresholdOperator.GREATER_EQUAL;

    /** The side of the data to apply the threshold to. */
    @Builder.Default
    private final ThresholdOrientation orientation = ThresholdOrientation.LEFT;

    /** The unit mapper. */
    private final UnitMapper unitMapper;

    /** Warnings received while reading or filtering thresholds. */
    private final Set<String> warnings = new HashSet<>();

    /**
     * Extract the mapping of all features to their thresholds
     *
     * @return A mapping between feature definitions and all of their thresholds
     */
    Map<Location, Set<Threshold>> extract()
    {
        Objects.requireNonNull( this.response, "A valid response was not passed to extract" );

        Collection<ThresholdDefinition> thresholdDefinitions = this.response.getThresholds();

        // Check that the user-declared filters return one or more locations with thresholds
        Set<String> providers = thresholdDefinitions.stream()
                                                    .map( ThresholdDefinition::getThresholdProvider )
                                                    .filter( Objects::nonNull )
                                                    .collect( Collectors.toUnmodifiableSet() );

        if ( Objects.nonNull( this.provider ) && !providers.contains( this.provider ) )
        {
            String warning = "While attempting to filter WRDS thresholds against the user-"
                             + "declared threshold provider '"
                             + this.provider
                             + "', discovered no thresholds that match the provider within the "
                             + "WRDS response. The WRDS response contained "
                             + thresholdDefinitions.size()
                             + " threshold definitions with the following threshold providers: "
                             + providers
                             + ". Choose one of these providers instead.";
            this.warnings.add( warning );
        }

        Set<String> ratingsProviders = thresholdDefinitions.stream()
                                                           .map( ThresholdDefinition::getRatingProvider )
                                                           .filter( Objects::nonNull )
                                                           .collect( Collectors.toUnmodifiableSet() );

        if ( Objects.nonNull( this.ratingProvider ) && !ratingsProviders.contains( this.ratingProvider ) )
        {
            String warning = "While attempting to filter WRDS thresholds against the user-"
                             + "declared threshold ratings provider '"
                             + this.ratingProvider
                             + "', discovered no thresholds that match the ratings provider "
                             + "within the WRDS response. The WRDS response contained "
                             + thresholdDefinitions.size()
                             + " threshold definitions with the following ratings providers: "
                             + ratingsProviders
                             + ". Choose one of these providers instead.";
            this.warnings.add( warning );
        }

        Map<Location, Set<Threshold>> resultsMap = new LinkedHashMap<>();

        for ( ThresholdDefinition definition : thresholdDefinitions )
        {
            // If the user specifies a threshold provider, then it must match the provider found in the threshold for
            // it to be used. If the user specifies a rating curve provider, then that must match too. If either is
            // unspecified (i.e., null), then the thresholds are not filtered with respect to that attribute.
            if ( ( ( this.provider == null ) || this.provider.equals( definition.getThresholdProvider() ) )
                 && ( this.ratingProvider == null || this.ratingProvider.equals( definition.getRatingProvider() ) ) )
            {
                Map<Location, Set<Threshold>> singleResult = definition.getThresholds( this.type,
                                                                                       this.operator,
                                                                                       this.orientation,
                                                                                       this.unitMapper );
                Location singleLocation = singleResult.keySet()
                                                      .iterator()
                                                      .next();
                Set<Threshold> singleOuter = singleResult.values()
                                                         .iterator()
                                                         .next();

                // Check for location already in map.  If so, add to existing set.  Otherwise, put new entry.
                if ( resultsMap.containsKey( singleLocation ) )
                {
                    resultsMap.get( singleLocation )
                              .addAll( singleOuter );
                }
                else
                {
                    resultsMap.put( singleLocation, singleOuter );
                }
            }
        }

        return resultsMap;
    }

    /**
     * @return any warnings encountered while extracting or filtering thresholds from the threshold response
     */
    Set<String> getWarnings()
    {
        return Collections.unmodifiableSet( this.warnings );
    }
}
