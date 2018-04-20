# Import libraries silently
suppressMessages( suppressWarnings( library( data.table ) ) )
suppressMessages( suppressWarnings( library( hydroGOF ) ) ) 
suppressMessages( suppressWarnings( library( verification ) ) )
suppressMessages( suppressWarnings( library( SpecsVerification ) ) ) 

##########################################################################################
#
# When adding a new metric, put it into the correct list below (or add a new list) and
# then edit the getMetric method to return the appropriate metric function. When adding
# a new category of metrics (i.e. a new list), the getTransform also needs to be edited
# to handle the expected transformation of the pairs. For example, ensemble forecasts 
# must be translated to ensemble mean forecasts for single.valued.continuous, whereas 
# they must be translated to forecast probabilities for discrete.probability.
#
##########################################################################################

single.valued.continuous <-list(
	c( rep("mean error", 2) ),
	c( rep("mean absolute error", 2) ),
	c( rep("mean square error", 2) ),
	c( rep("root mean square error", 2) ),
	c( rep("pearson correlation coefficient", 2) ),
	c( rep("coefficient of determination", 2) ),
	c( rep("mean square error skill score", 2) ),
	c( rep("kling gupta efficiency", 2) ),
	c( rep("volumetric efficiency", 2) ),
	c( rep("index of agreement", 2) ),
	c( rep("sample size", 2) ),
	c( rep("bias fraction", 2) )
)

discrete.probability <-list(
	c( rep("brier score", 2) ),
	c( rep("brier skill score", 2) ),
	c( rep("relative operating characteristic score", 2) )
)

categorical <-list(
	c( rep("probability of detection", 2) ),
	c( rep("probability of false detection", 2) ),
	c( rep("threat score", 2) ),
	c( rep("peirce skill score", 2) ),
	c( rep("equitable threat score", 2) )
)

ensemble <-list(
	c( rep("continuous ranked probability score", 2) )
)

##########################################################################################
#
# A function that generates one or more named metrics at one threshold and for all 
# features in the input pairs.
#
# pairs          The paired file
# threshold      The real valued threshold, which applies to the left for
#                continuous measures.
# thresholdType  The threshold type 
# ...	           The list of metrics to compute
#   
##########################################################################################

generateAllMetricsForAllFeatures <- function( pairs, threshold, thresholdType, ... ) 
{
      # Read, skip dates, fill header row, which may not be the first row
	test <- read.table( file = pairs, nrows = 1, stringsAsFactors = FALSE )
      # Header row
	if( grepl("Feature", test$V1 ) )
	{	
		# Need to jump through hoops here, as header causes problems for read.table with header=T
		con <- file( pairs, "r", blocking = FALSE)
		lines <- readLines( con )
		dataString<-paste0(lines[2:length(lines)], collapse="\n")
		data <- read.csv(textConnection(dataString),header=FALSE, stringsAsFactors=FALSE)
	} 
	else 
	{
		data <- fread( pairs, fill = TRUE, stringsAsFactors = FALSE )
		data <- data[ data$V1!="Feature", ]
	}

	# Convert numeric columns to numeric type
      for( i in 5: ncol(data))
      {
		data[,i] <- sapply( data[,i],as.numeric )
      }

      # Replace NaN with NA
      data[sapply(data,is.na)] = NA

      # Remove rows with missing obs
      #data = na.omit( data )
	data=data[complete.cases(data[, "V5"]),]

	# Find the features
      features <- unique(data$V1)
	# Iterate through the features and generate the metrics for each one
	for( i in 1: length(features) )
	{
		results <- generateAllMetricsForOneFeature ( data[ data$V1==features[i], ], threshold, thresholdType, ... )
		print( paste( "Metric results for '", features[[i]], "':", sep="" ) )
		print( results )		
	} 
}

##########################################################################################
#
# A function that generates one or more named metrics at one threshold and for one 
# feature. Expects a subset of pairs with only one feature.
#
# pairs          The pairs
# threshold      The real valued threshold, which applies to the left for
#                continuous measures
# thresholdType  The threshold type 
# ...	           The list of metrics to compute
# Returns        The list of metric results, one for each metric
#   
##########################################################################################

generateAllMetricsForOneFeature <- function( pairs, threshold, thresholdType, ... ) 
{
	# Validate
	if( length( unique( pairs$V1 ) ) >1 )
	{
		stop( " Expected pairs for one feature only. ")
	}
	# Iterate through the metrics
	metrics <- list( ... )
	# Results to return in a named list
	results<-list( length(metrics) )
	names(results)<-metrics
	for ( i in 1: length( metrics ) )
	{
		results[[i]]=generateOneMetricForOneFeature( pairs, threshold, thresholdType, metrics[[i]] )
	} 
	results
}

##########################################################################################
#
# A function that generates one named metric at one threshold and for one feature.
# Expects a subset of pairs with only one feature.
#
# pairs          The pairs
# threshold      The real valued threshold, which applies to the left for
#                continuous measures
# thresholdType  The threshold type 
# metric         The metric to compute
# Returns        A data.table of metric results with two columns: window number and metric name
#   
##########################################################################################

generateOneMetricForOneFeature <- function( pairs, threshold, thresholdType, metric ) 
{
      # Validate the threshold information
      getThresholdPredicate( threshold, thresholdType )

	# Iterate through each window
	windows <- unique( pairs$V4 )
	metricToCompute <- getMetric( metric )
	metric.results<-data.frame( matrix(ncol = 2, nrow = length( windows ) ) )
	colnames(metric.results) <- c( "Window number", metric )
	for (i in 1: length (windows) )
	{
		# Subset the data by window
		nextWindow <- pairs[ pairs$V4 == windows[[i]],]

		# Transform the pairs in a metric-appropriate way
		nextWindow <- suppressWarnings( transformPairs( nextWindow, metric, threshold, thresholdType ) )
		
		# For single-valued input, remove any rows with NaN
		if( ncol( nextWindow ) == 6 )
		{
			nextWindow <- na.omit( nextWindow )			
		}
		nextLeft <- as.vector( unlist( nextWindow[,5] ) )  # Observations in column 5
		nextRight <-  nextWindow[,6:ncol( nextWindow )]
		# Single-valued metrics: transform right
		if( doesThisMetricExist( metric, single.valued.continuous ) )
		{
			# Ensemble mean required
			if( ncol( nextWindow ) > 6 )
			{	
				nextRight <- rowMeans( data.frame( nextWindow[,6:ncol(nextWindow)] ), na.rm = TRUE )
			}
			else 
			{
				nextRight <- as.vector( unlist( nextWindow[,6] ) )
			}
		}
		# Discrete probability: requires vector
		else if ( doesThisMetricExist( metric, discrete.probability ) )
		{
			nextRight <- as.vector( unlist( nextRight ) )
		}
		metric.results[i,1] = windows[i]
		metric.results[i,2] = metricToCompute( nextLeft, nextRight )
	}
	metric.results
}

##########################################################################################
#
# Applies a threshold to a set of pairs in a metric appropriate way. For continuous 
# measures, the pairs are subset by left value. For dichotomous measures, the pairs 
# are classified according to threshold.
#
# pairs          The pairs
# metric         The named metric
# threshold      The threshold
# thresholdType  The threshold type 
# Returns        A the transformed pairs
#  
##########################################################################################

transformPairs <- function( pairs, metric, threshold, thresholdType )
{
      # Get the threshold predicate
      thresholdPredicate <- getThresholdPredicate( threshold, thresholdType )

      # Continuous measures for single-valued input
	if( doesThisMetricExist( tolower( metric ), single.valued.continuous ) )   
	{
		# All data
		if( is.na( threshold ) )
		{
			pairs
		}
		else 
		{
	      	# Observations in V5
			pairs[ thresholdPredicate( pairs$V5) , ]
		}
	}
	# Discrete probability measures for ensemble input
	else if ( doesThisMetricExist( tolower( metric ), discrete.probability ) )
	{	
		# All data
		if( is.na( threshold ) )
		{
			stop( "Cannot use the threshold 'NA' or 'All data' for a
					 discrete probability metric.")
		}
		else 
		{
			# Convert to probabilities
			subPairs<-pairs[,5:ncol( pairs )]
			subPairs <- apply( subPairs, 2, function(x) as.double ( thresholdPredicate( x ) ) )
			counts <- apply( subPairs, 1, function(x) sum( !is.na(x) ) ) - 1		
			f.probs <- rowSums( subPairs[,2:ncol( subPairs )] ) / counts
			pairs[,5] <- subPairs[,1]	
			pairs[,6] <- f.probs			
			pairs[,1:6]
		}
	}
	# Categorical measures for single-valued input
	else if ( doesThisMetricExist( tolower( metric ), categorical ) )
	{	
		# All data
		if( is.na( threshold ) )
		{
			stop( "Cannot use the threshold 'NA' or 'All data' for a
					 discrete probability metric.")
		}
		else if( ncol( pairs ) > 6 )
		{
			stop( paste( "Expected single-valued input for the ",metric,sep="" ) )
		}
		else 
		{			
			# Convert to binary obs and pred
			subPairs<-pairs[,5:ncol( pairs )]
			subPairs <- apply( subPairs, 2, function(x) as.double ( thresholdPredicate( x ) ) )
			pairs[,5] <- subPairs[,1]	
			pairs[,6] <- subPairs[,2]			
			pairs
		}
	}
	else if( doesThisMetricExist( tolower( metric ), ensemble ) )
	{
		# All data
		if( is.na( threshold ) )
		{
			pairs
		}
		else 
		{
	      	# Observations in V5
			pairs[ thresholdPredicate( pairs$V5 ) , ]     
		}
	}
	else 	
	{
		stop( paste( "Could not find metric: ", metric ) )
	}	
}

##########################################################################################
#
# A function that returns a named metric function and deals with the threshold 
# specification in a metric appropriate way. For continuous measures, subset by left
# value. For dichotomous measures classify using the threshold.
#
# metric    The named metric
# Returns   A named metric function
#  
##########################################################################################

getMetric <- function( metric )
{
	lower <- tolower( metric )
	# Find metric
	if( lower == "mean error" )
	{
		function( left, right) me( right, left )
	}
	else if( lower  == "mean absolute error" )
	{
		mae
	}
	else if( lower  == "root mean square error" )
	{
		rmse
	}
	else if( lower  == "pearson correlation coefficient" )
	{
		cor
	}
	else if( lower  == "coefficient of determination" )
	{
		function( left, right ) cor( left, right )^2 
	}
	else if( lower  == "mean square error skill score" )
	{
		function( left, right ) NSE( right, left )
	}
	else if( lower  == "kling gupta efficiency" )
	{
		function( left, right) KGE( right, left, method = "2012" )
	}
	else if( lower  == "volumetric efficiency" )
	{
		function( left, right ) VE( right, left )
	}
	else if( lower  == "index of agreement" )
	{
		function( left, right ) md( right, left )
	}
	else if( lower  == "sample size" )
	{
		function( left, right ) length( left )
	}
	else if( lower  == "mean square error" )
	{
		function( left, right ) mse( right, left )
	}
	else if( lower  == "brier score" )
	{
		mse #MSE in probability space
		
		# Equivalently
	      #function( left, right )
		#{		
		#	result <- verify( obs=left, pred=right, show = FALSE, 
		#		bins = FALSE, frcst.type="prob", obs.type="binary" )
	      #	result$bs
		#}
	}
	else if( lower  == "relative operating characteristic score" )
	{
	      function( left, right )
		{		
			result <- suppressWarnings( roc.area( obs = left, pred = right ) )
	      	2 * result$A - 1
		}
	}
	else if( lower  == "brier skill score" )
	{
		function( left, right ) NSE( right, left ) #NSE in probability space
	}
	else if( lower  == "probability of detection" )
	{
		function( left, right )
		{		
			result <- table.stats( obs=left, pred=right, silent = TRUE )
	      	result$POD
		}		
	}
	else if( lower  == "probability of false detection" )
	{
		function( left, right )
		{		
			result <- table.stats( obs=left, pred=right, silent = TRUE )
	      	result$F
		}		
	}
	else if( lower  == "threat score" )
	{
		function( left, right )
		{		
			result <- table.stats( obs=left, pred=right, silent = TRUE )
	      	result$TS
		}		
	}
	else if( lower  == "equitable threat score" )
	{
		function( left, right )
		{		
			result <- table.stats( obs=left, pred=right, silent = TRUE )
	      	result$ETS
		}		
	}
	else if( lower  == "continuous ranked probability score" )
	{
		# Mean CRPS using Hersbach (2000)
		function( left, right )
		{		
			mean( EnsCrps( as.matrix( right ), left ) )
		}		
	}
	else if( lower  == "bias fraction" )
	{
		function( left, right )
		{		
			me( right, left ) / mean( left )
		}		
	}
	else 	
	{
		stop( paste( "Could not find metric: ", metric ) )
	}
} 

##########################################################################################
#
# Function that returns true if the named metric exists in a particular list, false 
# otherwise.
#
# metric         The named metric
# metric.list    The list of named metrics to check
# Return         True if the metric exists in the metric.list, false otherwise
#
##########################################################################################

doesThisMetricExist<-function(metric, metric.list) 
{
	returnMe = match(TRUE,sapply(1:length(metric.list), function(i) any(metric.list[[i]] == metric)))
	if(is.na(returnMe))
	{
		return(0)
	}
	return(returnMe)
}

##########################################################################################
#
# Function that returns a predicate that evaluates to true if the input meets the 
# specified threshold condition, otherwise false
#
# threshold      The numeric threshold
# thresholdType  The named threshold type
# Return         A predicate that evaluates to true if the threshold condition is met
#
##########################################################################################

getThresholdPredicate<-function(threshold, thresholdType) 
{
    if( thresholdType == ">" )
    {
        function( input ) input > threshold; 
    }
    else if( thresholdType == ">=" )
    {
        function( input ) input >= threshold; 
    }
    else if( thresholdType == "<" )
    {
        function( input ) input < threshold; 
    }
    else if( thresholdType == "<=" )
    {
        function( input ) input <= threshold; 
    }
    else
    {
        stop( paste( "Unrecognized relational operator: ", thresholdType ) )
    }
}

##########################################################################################
#
# Main function that accepts command line arguments in this order:
#
# Path to pairs.csv file (sorted or unsorted)
# Metric name	
# Threshold
#
##########################################################################################

main <- function()
{
	# Get the trailing command line args
	args <-commandArgs(TRUE)
	if(length(args) != 3 && length(args) != 4  )
	{
		stop("Usage: path-to-pairs metric threshold thresholdType (optional)")
	}

      #Threshold type
      thresholdType <- ">="

	# Echo
	print( paste("Pairs: ", args[1], sep="" ) )
	print( paste("Metric: ", args[2], sep="" ) )
	print( paste("Threshold: ", args[3], sep="" ) )
      if( length(args) == 4 )
      {
	    thresholdType = args[4]
      }
      print( paste("Threshold type (optional): ", thresholdType, sep="" ) )  

	# Generate the mean error for a particular threshold. NA = all data
	generateAllMetricsForAllFeatures( args[1], as.numeric( args[3] ), thresholdType, args[2] )
}
main()






