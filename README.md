# SparkMLPractice
Learning and evaluate spark MLlib

Spark is a new and fast evolving big data computing framework. Many people think it's the future of big data. 
The foundation of Spark is its in-memory RDD compute model. It can cache the mediate compute result in memory (while Hadoop MapReduce need to flush them into low speed disk) which means Spark especailly fit for iterative computing like machine learning. 

In this practice we will evaluate Spark 1.3 mmlib and mlib. We also cover DataFrame which is another highlight feature in Ver 1.3.

1. What's the different between MLlib and ML
Basically speaking  
   mllib focus on machine learning low level  API. It packes many ml algorithms. All of them had be verified to be able to work on distributed environment. 
   Until now (V1.3) MLlib supports
    - linear model (SVM, LR, LogR, Naive Bayes, decision tree),
    - collaborative filtering,
    - clustering, 
    - dimensionality reduction, 
    - feature extraction, 
    
   On the other hand, ML focus on how to construct an efficiency machine learning system. A productive machine learning system including not only the core algorithm but also a serial steps like data clean, preprocess, model training, model evaluation, parameters adjusting. 
  ML provides some abstract class to represent the needed step in machine learning

    Transfomer - abstract for data cleaning
    Estimator - abstract for learning algorithm
    Pipeline  - combine transfomer and estimator into a streaming line
    

2. DataFrame
3. Basic MLlib practice (including ALS, SVD, K-mean cluster) and DataFrame
4. ML
5. Deep Learning(deeplearning4j on spark)


