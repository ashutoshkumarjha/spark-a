#!/usr/bin/env bash
rm -rf /Users/jnordling/point-poly/data/output
/Users/jnordling/projects/spark-1.4/bin/spark-submit\
 --master spark://bcn.blueraster.com:7077\
 --executor-memory 10G\
 /Users/jnordling/point-poly/spark-clip.py

