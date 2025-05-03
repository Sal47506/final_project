# Large Scale Data Processing: Final Project
For the final project, you are provided 6 CSV files, each containing an undirected graph, which can be found [here](https://drive.google.com/file/d/1khb-PXodUl82htpyWLMGGNrx-IzC55w8/view?usp=sharing). The files are as follows:  

| File name                   | Number of edges | Matching size (Bipartite-Algo)       | Matching size (Alon-Itai)         |
|------------------------------|-----------------|--------------------------------------|-----------------------------------|
| com-orkut.ungraph.csv        | 117,185,083      |                                      | 1,376,710 (927 seconds)           |
| twitter_original_edges.csv   | 63,555,749       | 92,018 (426 seconds)                              |                                   |
| soc-LiveJournal1.csv         | 42,851,237       | 1,546,942  (411 seconds)                          |   1,657,749 (554 seconds)                                |
| soc-pokec-relationships.csv  | 22,301,964       | 589,590 (233 seconds)                             |                                   |
| musae_ENGB_edges.csv         | 35,324           | 2,253 (5 seconds)                    | 2,392 (11 seconds)                |
| log_normal_100.csv           | 2,671            | 48 (5 seconds)                       | 49 (10 seconds)                   |


- **Largest file**: Processed on Google Cloud Platform using a `2x4 N2` machine for both the master and worker nodes.
- **Smallest two files**: Ran locally on the developer's machine.
- **Other files**: Processed on GCP with `2x4 N1` machines for both the master and worker nodes.


The specifications of the local machine used for testing can be found in the image below:

![Local Machine Specs](machine.png)


Algorithm 1: Bipartite Greedy matching (smaller vertices come first in priority)

Algorithm 2: Alon-Babai-Itai MIS Algorithm (Paper here: https://web.math.princeton.edu/~nalon/PDFS/Publications2/A%20fast%20and%20simple%20randomized%20parallel%20algorithm%20for%20the%20maximal%20independent%20set%20problem.pdf)

### Writeup/Report

We used two approaches for obtaining the matchings: **bipartite greedy matching** and the **Alon-Babai-Itai MIS algorithm**.

The bipartite matching algorithm focuses on finding a matching in a bipartite graph using a greedy approach. It begins by collecting all edges and vertices, then sorting the edges based on the weight of the source vertices. It iterates through the sorted edges and adds an edge to the matching set only if neither of its endpoints (source or destination vertices) are already matched. This ensures that each vertex is used at most once in the matching. The algorithm is efficient and straightforward, making it suitable for scenarios where finding a quick approximate matching is desirable.

The Alon-Itai algorithm iteratively computes a Maximal Independent Set (MIS) for a graph. In each iteration, it selects an independent set of vertices (a set of vertices where no two are adjacent), adds it to the MIS, and removes these vertices along with their neighbors from the graph. The process repeats until the graph is empty. A key optimization within the algorithm is the `inPhaseOptimized` function, which marks vertices probabilistically based on their degree and resolves conflicts to ensure a valid independent set. This algorithm is effective for problems requiring independent sets and is particularly useful for distributed or parallel computing needs.

If we were given a new test, we would probably run Alon-Itai, even if bipartite is slightly faster for smaller cases. While both algorithms are parallelizable and would work perfectly fine for most test cases, space begins to become a problem for bipartite when the input size gets extremely large. This is due to Scala’s lack of effective garbage collection, which can result in bipartite functions like `collect` and heap operations leading to heap space exhaustion—a problem we actually encountered.

To address this issue, we implemented Alon-Itai, which does not suffer from the same memory limitations. Both algorithms are very scalable in terms of time, as we make use of the Graph data structure and RDDs. Bipartite is faster, but AIB is more powerful, scalable, and accurate. AIB can work on any graph, even if it is cyclical or irregular, making it more versatile. It also uses randomization and local degree information, whereas bipartite does not. This allows AIB to find larger matchings and makes it more robust to variations in graph structure.

The only advantage of bipartite is its speed on smaller graphs. However, it fails to complete execution on very large graphs due to memory constraints. For massive graphs, we experienced Spark failures due to memory exhaustion and unbalanced partitions. AIB, on the other hand, streams most of the computation through graph operations and message passing, which results in a more stable memory profile across iterations—even though it takes slightly longer to run. As for the number of iterations, bipartite completes in one pass, while AIB takes `log(E)` iterations, where `E` is the number of edges.

Our implementation for the AIB algorithm does have some novel and original ideas. In the AIB paper, the authors originally intended for it to be used for finding an MIS. We were able to tweak it to find a matching, something which has never been done before.

The bipartite greedy matching algorithm has a proven **1/2-approximation guarantee**. This means the size of the matching it produces is always at least half that of the optimal matching. You can find a proof of this guarantee [here](https://bowaggoner.com/courses/gradalg/notes/lect06-approx.pdf?utm_source=chatgpt.com).As for the Alon-Babai-Itai (AIB) algorithm, the original authors did not provide a formal approximation guarantee. However, based on our testing, it consistently outperforms the bipartite greedy approach in both size and quality of matchings. This suggests that its approximation ratio is likely better than 1/2 in practice, even if not yet theoretically proven.

Our presentation slides can be found in this repo. Also, the matchings we submit are the largest ones we've got, typically form the AIB algirhtmm. They can be found in this repo named matching_solutions.zip. If you cannot access them, they will be in this google drive link:

https://drive.google.com/drive/folders/1k_0Heynrc9ibgxOoIczbpG2sp4zC_MDE?usp=sharing

### Matching Instructions

To run either algorithms do:
```
// Unix (Bipartite)
spark-submit --master "local[*]" --class "final_project.main" target/scala-2.12/project_3_2.12-1.0.jar compute data/log_normal_100.csv output_dir lubyalgo

// Unix (Alon-Itai)
spark-submit --master "local[*]" --class "final_project.main" target/scala-2.12/project_3_2.12-1.0.jar compute data/log_normal_100.csv output_dir alonitai

// Unix
spark-submit --master "local[*]" --class "final_project.matching_verifier" target/scala-2.12/project_3_2.12-1.0.jar data/log_normal_100.csv output_dir/part-00000
```