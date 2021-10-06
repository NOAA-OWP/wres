package wres.io.thresholds.wrds.v3;

import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.exceptions.NoThresholdsFoundException;
import wres.io.thresholds.wrds.WRDSThresholdType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extracts a mapping between features and their thresholds from a WRDS formatted thresholds document
 */
public class GeneralThresholdExtractor
{
    /**
     * Create an Extractor from the given response
     *
     * @param response The deserialized Thresholds document
     */
    public GeneralThresholdExtractor( GeneralThresholdResponse response )
    {
        this.response = response;
    }

    /**
     * Set who the provider of the threshold data was
     *
     * @param provider The name of the provider, like 'NWS-NRLDB'
     * @return The updated extractor
     */
    public GeneralThresholdExtractor from( String provider )
    {
        this.provider = provider;
        return this;
    }

    /**
     * Set the provider of the rating curve for the thresholds
     *
     * @param ratingProvider The name of the provider of the rating curve, like 'NRLDB'
     * @return The updated extractor
     */
    public GeneralThresholdExtractor ratingFrom( String ratingProvider )
    {
        this.ratingProvider = ratingProvider;
        return this;
    }

    /**
     * Sets what side the threshold will apply to
     *
     * @param side The side of the data that the threshold will apply to
     * @return The updated extractor
     */
    public GeneralThresholdExtractor onSide( ThresholdConstants.ThresholdDataType side )
    {
        this.sides = side;
        return this;
    }

    /**
     * Whether or not calculated thresholds should be used
     *
     * @param useCalculated Whether or not to use calculated thresholds
     * @return The updated extractor
     */
    public GeneralThresholdExtractor useCalculatedValues( boolean useCalculated )
    {
        this.calculated = useCalculated;
        return this;
    }

    /**
     * The operator through which to apply the threshold value
     *
     * @param thresholdOperator The operator
     * @return The updated extractor
     */
    public GeneralThresholdExtractor operatesBy( ThresholdConstants.Operator thresholdOperator )
    {
        this.thresholdOperator = thresholdOperator;
        return this;
    }

    /**
     * Tells the extractor to extract flow thresholds
     *
     * @return The updated extractor
     */
    public GeneralThresholdExtractor readFlow()
    {
        this.thresholdType = WRDSThresholdType.FLOW;
        return this;
    }

    /**
     * Tells the extractor to extract stage thresholds
     *
     * @return The updated extractor
     */
    public GeneralThresholdExtractor readStage()
    {
        this.thresholdType = WRDSThresholdType.STAGE;
        return this;
    }

    public GeneralThresholdExtractor convertTo( UnitMapper mapper )
    {
        this.desiredUnitMapper = mapper;
        return this;
    }

    /**
     * Extract the mapping of all features to their thresholds
     *
     * @return A mapping between feature definitions and all of their thresholds
     */
    public Map<WrdsLocation, Set<ThresholdOuter>> extract()
    {
        Objects.requireNonNull( this.response, "A valid response was not passed to extract" );

        Collection<GeneralThresholdDefinition> thresholdDefinitions = this.response.getThresholds();

        // Check that the user-declared filters return one or more locations with thresholds
        Set<String> providers = thresholdDefinitions.stream()
                                                    .filter( next -> Objects.nonNull( next.getThresholdProvider() ) )
                                                    .map( GeneralThresholdDefinition::getThresholdProvider )
                                                    .collect( Collectors.toUnmodifiableSet() );

        if ( Objects.nonNull( this.provider ) && !providers.contains( this.provider ) )
        {
            throw new NoThresholdsFoundException( "While attempting to filter WRDS thresholds against the user-"
                                                  + "declared threshold provider '"
                                                  + this.provider
                                                  + "', discovered no thresholds that match the provider within the "
                                                  + "WRDS response. The WRDS response contained "
                                                  + thresholdDefinitions.size()
                                                  + " threshold definitions with the following threshold providers: "
                                                  + providers
                                                  + ". Choose one of these providers instead." );
        }

        Set<String> ratingsProviders = thresholdDefinitions.stream()
                                                           .filter( next -> Objects.nonNull( next.getRatingProvider() ) )
                                                           .map( GeneralThresholdDefinition::getRatingProvider )
                                                           .collect( Collectors.toUnmodifiableSet() );

        if ( Objects.nonNull( this.ratingProvider ) && !ratingsProviders.contains( this.ratingProvider ) )
        {
            throw new NoThresholdsFoundException( "While attempting to filter WRDS thresholds against the user-"
                                                  + "declared threshold ratings provider '"
                                                  + this.ratingProvider
                                                  + "', discovered no thresholds that match the ratings provider "
                                                  + "within the WRDS response. The WRDS response contained "
                                                  + thresholdDefinitions.size()
                                                  + " threshold definitions with the following ratings providers: "
                                                  + ratingsProviders
                                                  + ". Choose one of these providers instead." );
        }


        Map<WrdsLocation, Set<ThresholdOuter>> resultsMap = new LinkedHashMap<>();

        for ( GeneralThresholdDefinition definition : thresholdDefinitions )
        {
            //If the user specifies a threshold provider then it must match that found in the threshold for it 
            //to be used.  If the user specifies a rating curve provider, then that must match as well.
            //If either is unspecified (i.e., null), then it is not used to determine if a threshold is used.
            if ( ( ( this.provider == null ) || this.provider.equals(definition.getThresholdProvider()) )
                 && ( this.ratingProvider == null || this.ratingProvider.equals(definition.getRatingProvider()) ) )
            {
                Map<WrdsLocation, Set<ThresholdOuter>> singleResult = definition.getThresholds( this.thresholdType,
                                                                                                this.thresholdOperator,
                                                                                                this.sides,
                                                                                                this.calculated,
                                                                                                this.desiredUnitMapper );
                WrdsLocation singleLocation = singleResult.keySet().iterator().next();
                Set<ThresholdOuter> singleOuter = singleResult.values().iterator().next();

                //Check for location already in map.  If so, add to existing set.  Otherwise, put new entry.
                if ( resultsMap.containsKey( singleLocation ) )
                {
                    resultsMap.get( singleLocation ).addAll( singleOuter );
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
     * The Threshold data from WRDS
     */
    private final GeneralThresholdResponse response;

    /**
     * The publisher of the threshold data
     */
    private String provider;

    /**
     * The publisher of the rating curve for the thresholds
     */
    private String ratingProvider;

    /**
     * Whether or not calculated thresholds should be used
     */
    private boolean calculated = true;

    /**
     * The type of data for the threshold
     */
    private WRDSThresholdType thresholdType = WRDSThresholdType.FLOW;

    /**
     * The boolean operator to apply to loaded values
     */
    private ThresholdConstants.Operator thresholdOperator = ThresholdConstants.Operator.GREATER_EQUAL;

    /**
     * The side of the data to apply the threshold to
     */
    private ThresholdConstants.ThresholdDataType sides = ThresholdConstants.ThresholdDataType.LEFT;

    private UnitMapper desiredUnitMapper;

}
