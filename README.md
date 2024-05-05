# Final-project DDM (PySpark)
Project on Spark (includes parts A and B) Distributed Database Management 
(Done in pair with Daniel Maor)

**Part A**

Dataset: includes data on households and their viewing habits and consists out of 4 files. Full description is in *dataset.txt* file

Tasks overview:
* Spam Detection: Identify and handle malicious data entries inserted by a rival association using Spark. This involves computational detection based on specific conditions related to viewing patterns and program characteristics.
* Data Transformation and Loading: Preprocess and transform the data using Spark, focusing on efficiency and optimization, and prepare it for detailed analysis.
* Design and Deployment of Database: Design a database that efficiently manages and retrieves data across different Designated Market Areas (DMAs) without full replication due to budget constraints. This includes calculating the popularity of various genres within each DMA and adjusting for the wealth of the DMA.
* Implementation via Spark: Execute the database design using PySpark, ensuring the system is scalable and data is accessible across different servers.

I dive deeper in the tasks in *PartA.txt* file

**Part B**

In this part the task is to apply advanced data processing techniques and derive insightful recommendations for television channel preferences based on demographic and viewing data (same data as in Part A)

*Static Data Analysis:*
* Feature Extraction: Adjust and normalize demographic data, using techniques like normalization for numerical variables and one-hot encoding for categorical variables. Combine these into a single feature vector per household.
* Visual Analysis: Employ PCA to visualize potential natural groupings among households and plot the results.
* Clustering: Implement K-means to cluster households into 6 groups, adding new data columns for cluster membership and proximity to cluster centroids.
* Subset Division: Using PySpark Windows, divide households within each cluster into subsets (3rds and 17ths subsets) based on their proximity to the centroid.
* Viewing Analysis: Analyze viewing patterns across different clusters and subsets to ascertain the distinctiveness of viewing preferences, which involves calculating a 'diff rank' for how station preferences differ from the general population.


*Dynamic Data Analysis - Streaming:*
Utilize Spark Streaming to process real-time viewing data from Kafka, repeating the viewing analysis for each data batch.
Focus specifically on analyzing the '3rds subset' for each cluster during streaming to understand dynamic viewing habits.

I dive deeper in the tasks in *PartB.txt* file

