# sequence-generator
Sequence generator is a partial non-blocking implmentation for fetching the oracle next number from oracle sequences.
Making a database call to get the next number from oracle sequence is not a very efficient approach. It can bring down the performance of you application. The work around for this is to create a oracle sequence with INCREMENT BY value greater than 1.
<br>e.g.
CREATE SEQUENCE TEST_SEQ MINVALUE 1 MAXVALUE 999999999999999999999999999 START WITH 1 INCREMENT BY 1000 CACHE 20
<br>In this case oracle sequence will be incremented by 1000 every time a call "SELECT <SEQ_ID>.NEXTVAL FROM dual" is made.
This sequence-generator service prefetches or caches the oracle sequence and the number of sequences cached are equal to "INCREMENT BY" value of the given sequence.
If the "INCREMENT BY" value is 1000, then sequence generator service will make a db a call only once for 1000 invocations.
Sequence generator service is written using non blocking algorithm (it is partial non blocking service). The code uses synchronized access only to handle the cached sequence exhausted case. If 1000 sequences are cached then it will enter into synchronized block only once in 1000 invocations.
