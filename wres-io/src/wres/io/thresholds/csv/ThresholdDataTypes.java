package wres.io.thresholds.csv;

import wres.config.generated.FeatureType;
import wres.config.generated.ThresholdType;
import wres.datamodel.thresholds.ThresholdConstants;

class ThresholdDataTypes {
    private final ThresholdConstants.ThresholdDataType thresholdDataType;
    private final ThresholdType thresholdType;
    private final FeatureType featureType;
    private final ThresholdConstants.Operator operator;

    /**
     * Construct.
     *
     * @param thresholdDataType the threshold data type
     * @param featureType the feature type
     * @param thresholdType the threshold type
     * @param operator the threshold operator
     */
    ThresholdDataTypes( ThresholdConstants.ThresholdDataType thresholdDataType,
                                FeatureType featureType,
                                ThresholdType thresholdType,
                                ThresholdConstants.Operator operator )
    {
        this.thresholdDataType = thresholdDataType;
        this.featureType = featureType;
        this.thresholdType = thresholdType;
        this.operator = operator;
    }

    /**
     * @return the threshold data type.
     */

    ThresholdConstants.ThresholdDataType getThresholdDataType()
    {
        return this.thresholdDataType;
    }

    /**
     * @return the threshold type.
     */

    ThresholdType getThresholdType()
    {
        return this.thresholdType;
    }

    /**
     * @return the feature type.
     */

    FeatureType getFeatureType()
    {
        return this.featureType;
    }

    /**
     * @return the threshold operator.
     */

    ThresholdConstants.Operator getOperator()
    {
        return this.operator;
    }
}
