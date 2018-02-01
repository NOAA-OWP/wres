# Import libraries silently
suppressMessages( suppressWarnings( library( data.table ) ) )
suppressMessages( suppressWarnings( library( hydroGOF ) ) ) 

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
	c( rep("root mean square error", 2) ),
	c( rep("pearson correlation coefficient", 2) ),
	c( rep("coefficient of determination", 2) ),
	c( rep("mean square error skill score", 2) ),
	c( rep("kling gupta efficiency", 2) ),
	c( rep("volumetric efficiency", 2) ),
	c( rep("index of agreement", 2) )
)

##########################################################################################
#
# A function that generates one or more named metrics at one threshold and for all 
# features in the input pairs.
#
# pairs     The paired file
# threshold The real valued threshold, which applies to the left for
#           continuous measures. This is currently assumed to be >=
# ...	      The list of metrics to compute
#   
##########################################################################################

generateAllMetricsForAllFeatures <- function( pairs, threshold, ... ) 
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
	# Convert columns with header information to numeric type
	data[,5] <- sapply( data[,5],as.numeric )
	data[,6] <- sapply( data[,6],as.numeric )
	# Find the features
      features <- unique(data$V1)
	# Iterate through the features and generate the metrics for each one
	for( i in 1: length(features) )
	{
		results <- generateAllMetricsForOneFeature ( data[ data$V1==features[i], ], threshold, ... )
		print( paste( "Metric results for '", features[[i]], "':", sep="" ) )
		print( results )		
	} 
}

##########################################################################################
#
# A function that generates one or more named metrics at one threshold and for one 
# feature. Expects a subset of pairs with only one feature.
#
# pairs     The pairs
# threshold The real valued threshold, which applies to the left for
#           continuous measures
# ...	      The list of metrics to compute
# Returns   The list of metric results, one for each metric
#   
##########################################################################################

generateAllMetricsForOneFeature <- function( pairs, threshold, ... ) 
{
	# Validate
	if( length( unique( pairs$V1 ) ) >1 )
	{
		stop( " Expected pairs for one feature only. ")
	}
	# Iterate through the metrics
	metrics <- list( ... )
	# Results to return in a named list
	results<-list( length(metrics))
	names(results)<-metrics
	for ( i in 1: length( metrics ) )
	{
		results[[i]]=generateOneMetricForOneFeature( pairs, threshold, metrics[[i]] )
	} 
	results
}

##########################################################################################
#
# A function that generates one named metric at one threshold and for one feature.
# Expects a subset of pairs with only one feature.
#
# pairs     The pairs
# threshold The real valued threshold, which applies to the left for
#           continuous measures
# metric    The metric to compute
# Returns   A data.table of metric results with two columns: window number and metric name
#   
##########################################################################################

generateOneMetricForOneFeature <- function( pairs, threshold, metric ) 
{
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
		nextWindow <- suppressWarnings( transformPairs( nextWindow, metric, threshold ) )
		nextLeft <- unlist( nextWindow[,5] )  # Observations in column 5
		nextRight <- unlist( nextWindow[,6] )
		# Compute the mean if the right has more than one column, i.e. ensemble mean
		if( ncol(pairs) > 6 )
		{	
			nextRight <- rowMeans( data.frame( nextWindow[,6:ncol(nextWindow)] ) )
		}
		metric.results[i,1] = windows[i]
		metric.results[i,2] = metricToCompute( nextRight, nextLeft )
	}
	metric.results
}

##########################################################################################
#
# Applies a threshold to a set of pairs in a metric appropriate way. For continuous 
# measures, the pairs are subset by left value. For dichotomous measures, the pairs 
# are classified according to threshold.
#
# pairs     The pairs
# metric    The named metric
# Returns   A the transformed pairs
#  
##########################################################################################

transformPairs <- function( pairs, metric, threshold )
{
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
			pairs[ pairs$V5 >= threshold , ]     # Assumed >=
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
		me
	}
	else if( lower  == "mean absolute error" )
	{
		mae
	}
	else if( lower  == "root mean square error" )
	{
		rms
	}
	else if( lower  == "pearson correlation coefficient" )
	{
		cor
	}
	else if( lower  == "coefficient of determination" )
	{
		R2
	}
	else if( lower  == "mean square error skill score" )
	{
		NSE
	}
	else if( lower  == "kling gupta efficiency" )
	{
		KGE
	}
	else if( lower  == "volumetric efficiency" )
	{
		VE
	}
	else if( lower  == "index of agreement" )
	{
		md
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
	if(length(args) != 3 )
	{
		stop("Usage: path-to-pairs metric threshold")
	}

	# Echo
	print( paste("Pairs: ", args[1], sep="" ) )
	print( paste("Metric: ", args[2], sep="" ) )
	print( paste("Threshold: ", args[3], sep="" ) )

	# Generate the mean error for a particular threshold. NA = all data
	generateAllMetricsForAllFeatures( args[1], as.numeric( args[3] ), args[2] )
}
main()






