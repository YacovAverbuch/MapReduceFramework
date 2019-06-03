author: Yacov Averbuch

This program is part of the Operating Systems course at the Hebrew university
# Description

This framework support dividing a big mapping task into small independent tasts that
can run simultanusly in parallel in multy-tredind scheme. after the mapping task finished,
it sopport reducing the output from all the threads and create one output vector.

Given a vector of data A of distinct items, and maping function from aech a in A to pair of 
(key_2, value_2), the user has to implement the 'MapReduceClient.h' and run the function
'start_map_reduce' with the input vector, the output vector, the implementation of the 
client and the number of thread he wants to create.

'SampleClient.cpp' is an implmentation of the client for the next problem: given vector
of strings to find out the number of appeareince of each letter of the alpha-bait in those
strings.

'makefile' - to generate static library from 'MapReduceFramework.cpp'


