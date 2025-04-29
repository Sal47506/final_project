# Run SBT clean and compile
build:
    sbt clean compile

# Package the project
package:
    sbt package

# Run the main class with Spark
run-main algorithm={lubyalgo,alonitai}:
    spark-submit --master "local[*]" --class "final_project.main" target/scala-2.12/project_3_2.12-1.0.jar compute data/log_normal_100.csv output_dir {{algorithm}}

# Verify the matching
verify:
    spark-submit --master "local[*]" --class "final_project.matching_verifier" target/scala-2.12/project_3_2.12-1.0.jar data/log_normal_100.csv output_dir/part-00000
