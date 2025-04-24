# Generates the line graph from an edge list.
# Usage: python line_graph.py input_edge_csv output_linegraph_csv

import sys

def Main(filepath, outpath):
    with open(outpath, "w") as out:
        with open(filepath) as f:
            ids = dict()
            power = 2**32
            for i, line in enumerate(f):
                l = line.strip().split(",")
                a = int(l[0])
                b = int(l[1])
                Vertex_Id = str(power*a + b)
                if a in ids:
                    for num in ids[a]:
                        out.write(Vertex_Id + "," + num + "\n")
                    ids[a].append(Vertex_Id)
                else:
                    ids[a] = [Vertex_Id]
                if b in ids:
                    for num in ids[b]:
                        out.write(Vertex_Id + "," + num + "\n")
                    ids[b].append(Vertex_Id)
                else:
                    ids[b] = [Vertex_Id]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python line_graph.py input_edge_csv output_linegraph_csv")
        sys.exit(1)
    Main(sys.argv[1], sys.argv[2])
