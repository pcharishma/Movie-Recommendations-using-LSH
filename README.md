# Movie-Recommendations-using-LSH
It is very similar to Netfilx movie recommendation system which uses Locality Sensitive Hashing

### Problem Statement:
Suppose there are 100 different movies, numbered from 0 to 99. A user is represented as a set of movies. 
I am using Jaccard coefficient to measure the similarity of sets.

#### Finding similar users: 
Applied minhash to obtain a signature of 20 values for each user.
Assume that the i-th hash function for the signature: h(x,i) = (3x + 13i) % 100, where x is the
original row number in the matrix. Note that i ranges from 0 to 19.
Locality Sensitive Hashing is used to speed up the process of finding similar users, where the signature is divided into 5
bands, with 4 values in each band.

#### Making recommendations: 
Based on the LSH result, for each user U, fI am trying to find the top-5 users who are most similar to U (by their Jaccard similarity, if same, choose the user that has smallest ID), and
recommend top-3 movies to U where the movies are ordered by the number of these top-5 users who have watched them (if same, choose the movie that has the smallest ID).

#### Input Formatting:
`U1,0,12,45`
`U2,2,3,5,99`

#### Output Formatting:
`U1,5,20,38`
`U2,1,5,12`

#### Format of Execution:
```bin/spark-submit lsh_reccomendation.py input.txt output.txt```
