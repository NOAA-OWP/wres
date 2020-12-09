package wres.io.thresholds.wrds.response;

import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Extracts a mapping between features and their thresholds from a WRDS formatted thresholds document
 */
public class ThresholdExtractor {
    /**
     * Create an Extractor from the given response
     *
     * @param response The deserialized Thresholds document
     */
    public ThresholdExtractor(ThresholdResponse response) {
        this.response = response;
    }

    /**
     * Set who the provider of the threshold data was
     *
     * @param provider The name of the provider, like 'NWS-NRLDB'
     * @return The updated extractor
     */
    public ThresholdExtractor from(String provider) {
        this.provider = provider;
        return this;
    }

    /**
     * Set the provider of the rating curve for the thresholds
     *
     * @param ratingProvider The name of the provider of the rating curve, like 'NRLDB'
     * @return The updated extractor
     */
    public ThresholdExtractor ratingFrom(String ratingProvider) {
        this.ratingProvider = ratingProvider;
        return this;
    }

    /**
     * Sets what side the threshold will apply to
     *
     * @param side The side of the data that the threshold will apply to
     * @return The updated extractor
     */
    public ThresholdExtractor onSide(ThresholdConstants.ThresholdDataType side) {
        this.sides = side;
        return this;
    }

    /**
     * Whether or not calculated thresholds should be used
     *
     * @param useCalculated Whether or not to use calculated thresholds
     * @return The updated extractor
     */
    public ThresholdExtractor useCalculatedValues(boolean useCalculated) {
        this.calculated = useCalculated;
        return this;
    }

    /**
     * The operator through which to apply the threshold value
     *
     * @param thresholdOperator The operator
     * @return The updated extractor
     */
    public ThresholdExtractor operatesBy(ThresholdConstants.Operator thresholdOperator) {
        this.thresholdOperator = thresholdOperator;
        return this;
    }

    /**
     * Tells the extractor to extract flow thresholds
     *
     * @return The updated extractor
     */
    public ThresholdExtractor readFlow() {
        this.thresholdType = WRDSThresholdType.FLOW;
        return this;
    }

    /**
     * Tells the extractor to extract stage thresholds
     *
     * @return The updated extractor
     */
    public ThresholdExtractor readStage() {
        this.thresholdType = WRDSThresholdType.STAGE;
        return this;
    }

    public ThresholdExtractor convertTo(UnitMapper mapper) {
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
        Objects.requireNonNull(this.response, "A valid response was not passed to extract");
        return response.getThresholds()
                .stream()
                .filter(
                        (ThresholdDefinition definition) -> definition.getThresholdProvider().equals(this.provider) &&
                                (this.ratingProvider == null || definition.getRatingProvider().equals(this.ratingProvider))
                )
                .parallel()
                .map(
                        definition -> definition.getThresholds(
                                this.thresholdType,
                                this.thresholdOperator,
                                this.sides,
                                this.calculated,
                                this.desiredUnitMapper
                        )
                )
                .flatMap(locationThresholds -> locationThresholds.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * The Threshold data from WRDS
     */
    private final ThresholdResponse response;

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
