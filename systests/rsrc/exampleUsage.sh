# cd to an output directory
# Run something similar to the below
# You can set the stats names to check in the "for name..." line.
# You can specify the thresholds (NA = All data) in the "for value..." line based on the thresholds shown in the metric output.
# Do not bother with this step until both the thresholds and the pairs have been confirmed.

#000 series
for name in "brier score" "brier skill score" "mean error" "mean absolute error" "mean square error" "root mean square error" "pearson correlation coefficient" "coefficient of determination" "mean square error skill score" "kling gupta efficiency" "volumetric efficiency" "index of agreement" "sample size"; do 
  echo "============================================================ $name" >> deleteme.txt; 
  for value in NA 0.5 1.0 2.0 5.0 10.0 15.0 20.0 25.0; do 
    Rscript ../../rsrc/CheckMetrics.R "sorted_pairs.csv" "$name" $value >> deleteme.txt; 
done; 
done

#100 series
for name in "brier score" "brier skill score" "mean error" "mean absolute error" "mean square error" "root mean square error" "pearson correlation coefficient" "coefficient of determination" "mean square error skill score" "kling gupta efficiency" "volumetric efficiency" "index of agreement" "sample size"; do 
  echo "============================================================ $name"; 
  for value in NA 0.00142 0.00906 0.09628 0.42475 1.69901 4.0932; do 
    Rscript ../../rsrc/CheckMetrics.R "sorted_pairs.csv" "$name" $value ; 
  done; 
done


