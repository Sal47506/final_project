# Converts a CSV of vertex IDs (from a matching or MIS) to edge pairs (a,b)
# Usage: python vertex_to_edge.py input_vertex_csv output_edge_csv

import sys

if len(sys.argv) != 3:
    print("Usage: python vertex_to_edge.py input_vertex_csv output_edge_csv")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

power = 2**32

with open(input_path) as f:
    with open(output_path, "w") as out:
        for line in f:
            l = line.strip().split(",")
            total = int(l[0])
            a, b = (total // power, total % power)
            out.write(f"{a},{b}\n")
