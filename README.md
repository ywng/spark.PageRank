# spark.PageRank
Implement PageRank in Spark scala.

### Map:
Emit a (fromPage, toPage) tuple pair for each link of webpages. <br />
Create a ranks vector [(pageID1, rank), (pageID2, rank), …] for each unique key of previous tuples.

### Reduce:
To compute in parallel, we cannot construct a big matrix and do normal matrix vector multiplication. <br />
Notice that each (fromPage, toPage) is a cell (non-zero) in the stochastic web matrix M. Group by fromPage, we essentially get a column to compute each new Ri with contribution (1/#outgoing links) * old_ Ri in Ranks vector V.<br /><br />
Reduce by key (_ + _) with toPageID as key, we will get the new rank for the given page.
