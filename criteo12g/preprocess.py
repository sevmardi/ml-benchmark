import sys
from tqdm import tqdm 

col_map = {14:500000, 15:16000, 16:13000, 17:7000, 18:20000, 19:5, 20:7000,
21:1500, 22:50, 23:300000, 24:60000, 25:50000, 26:10, 27:2300, 28:7000,
29:60, 30:5, 31:1000, 32:20, 33:400000, 34:160000, 35:400000, 36:50000, 37:10000, 38:100, 39:50}

maxes = {1: 5775, 2: 257675, 3: 65535, 4: 969, 5: 23159456, 6: 431037, 7: 56311, 8: 6047, 9: 29019, 10: 11, 11: 231, 12: 4008, 13: 7393}

offset = {14:14,}
prev = 0
for key in range(15,40):
	offset[key] = col_map[key-1]+prev
	prev=  offset[key]

#take the train set as CLI argument 
fname = sys.argv[1] #/data/vw/criteo-display-advertising-dataset/train.txt"
output = sys.argv[2] #the output file e.g. output.svm


num_terms = sum(col_map.values()) + 14 + 1
num_lines = sum(1 for line in open(fname))
pbar = tqdm(total = num_lines)
with open(fname, "r") as f:
    with open(output, "w") as out:
        line = f.readline()
        while line:
            lst = line.split("\t")
            for i in range(0, 40):
                if i == 0:
                    out.write("%s " % (lst[i]))
                elif 1 <= i <= 13:
                    try:
                        out.write("%d:%0.15f " % (i+1, int(lst[i])/float(maxes[i])))
                        if int(lst[i]) > max_vals[i]:
                            max_vals[i] = int(lst[i])
                    except:
                        pass
                else:
                    try: 
                        int_val = int(lst[i], 16) % 262145
                        int_val += offset[i]
                        out.write("%d:1.0 " % (int_val + 1))
                    except ValueError:
                        pass
            out.write("%d:1.0 " % 262146)
            line = f.readline()
            out.write("\n")
            pbar.update(1)
pbar.close()

print("Done!")
