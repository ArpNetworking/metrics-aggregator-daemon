Histograms
==========

MAD primarily computes histograms.  This document details the implementation of the histograms created
by MAD.

Representation
--------------
Histograms are built by incrementing a counter in a bucket and stored as a sparse list of
bucket -> count entries.  The bucket is defined by the smallest number stored in that bucket, computed
by using an IEEE Double with a mantissa truncated to n bits of precision.  The default in MAD is to use
7 bits of precision.  Additionally, the min, max and sum of the samples are stored as IEEE Double values
alongside the histogram.

For a given value, the bucket is found by the following:  
bucket = (double)(val &  0xffffe00000000000L)

The result is a consistent set of buckets that scale with the value of the data.  Larger values
have a larger absolute error, but with a consistent error proportion.

Statistics
----------

min - accurate (stored)  
max - accurate (stored)  
sum - accurate (stored)  
count - accurate (derived)  
mean - accurate (derived)  
quantiles - estimated.  accuracy depends on precision of the histogram and value stored.  n bits of
precision provides accuracy to within (1 / (2^n)) * value.  ex: 7 bits of precision provides accuracy
to within 1% of the computed value.
