package wres.io.thresholds.csv;

import wres.config.generated.ThresholdType;
import wres.datamodel.thresholds.ThresholdConstants;

class ThresholdDataTypes {
    private final ThresholdConstants.ThresholdDataType thresholdDataType;
    private final ThresholdType thresholdType;
    private final ThresholdConstants.Operator operator;

    /**
     * Construct.
     *
     * @param thresholdDataType the threshold data type
     * @param thresholdType the threshold type
     * @param operator the threshold operator
     */
    ThresholdDataTypes( ThresholdConstants.ThresholdDataType thresholdDataType,
                                ThresholdType thresholdType,
                                ThresholdConstants.Operator operator )
    {
        this.thresholdDataType = thresholdDataType;
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
     * @return the threshold operator.
     */

    ThresholdConstants.Operator getOperator()
    {
        return this.operator;
    }
}
