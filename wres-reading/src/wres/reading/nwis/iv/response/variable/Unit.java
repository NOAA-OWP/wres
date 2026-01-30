package wres.reading.nwis.iv.response.variable;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A unit.
 */
@Setter
@Getter
public class Unit implements Serializable
{
    @Serial
    private static final long serialVersionUID = 3361311118306617838L;

    /** Unit code. */
    private String unitCode;
}
