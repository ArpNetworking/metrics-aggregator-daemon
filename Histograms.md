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

Statistics
----------

min - accurate (stored)
max - accurate (stored)
sum - accurate (stored)
quantiles - estimated.  accuracy depends on precision of the histogram and value stored.  n bits of
precision provides accuracy to within (1 / (2^n)) * value.  ex: 7 bits of precision provides accuracy
to within 1% of the computed value.
