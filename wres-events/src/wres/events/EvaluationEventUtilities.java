package wres.events;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * A utility class to help with the messaging of evaluations.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EvaluationEventUtilities
{

    /**
     * Used to generate unique evaluation identifiers.
     */

    private static final RandomString ID_GENERATOR = new RandomString();

    /**
     * Returns a unique identifier for identifying a component of an evaluation, such as the evaluation itself or 
     * messaging client.
     * 
     * @return a unique identifier
     */

    public static String getUniqueId()
    {
        return EvaluationEventUtilities.ID_GENERATOR.generate();
    }
    
    /**
     * Generate a compact, unique, identifier for an evaluation. Thanks to: 
     * https://neilmadden.blog/2018/08/30/moving-away-from-uuids/
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class RandomString
    {
        private static final SecureRandom random = new SecureRandom();
        private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

        private String generate()
        {
            byte[] buffer = new byte[20];
            random.nextBytes( buffer );
            return encoder.encodeToString( buffer );
        }
    }
    
}
