The implementation consists of following parts, and was executed on AWS EMR.

1> LinkMap and LinkReduce for generating the link graph

LinkMap input:
key: line number, which is not used
value: a line from the input file

LinkMap output:
key: current page title
value: page rank value, and outbound links, which are separated by ",,".

LinkReduce: just an indentity function

2> PageRankMap and PageRankReduce for computing ranking values

PageRankMap input:
same as output of LinkReduce

PageRankMap output:
Key: page name
value: rank value or link graph
 
PageRankReduce input:
same as output of PageRankMap

PageRankRedkuce output:
Same as input of PageRankMap

3> CleanMap and CleanReduce for getting the final result

CleanMap input:
Same as output of PageRankReduce

CleanMap output:
key:page rank value
value:page title

CleanReduce: just an identity function

4> driving method
a.generate link graph
b.do page rank for 5 iteration
c.do clean up task