
import numpy as np

from matplotlib import pyplot as plt
from matplotlib.collections import LineCollection
import pandas as pd
from sklearn import manifold
from sklearn.metrics import euclidean_distances
from sklearn.decomposition import PCA

'''
To transpose the data
np.array(a).T.tolist()
'''
print "hello world"
#Load data into an pandas data structure
data = pd.read_csv("/Users/shanedooley/Documents/COBS/normal_counts_clean_r2.txt",sep='\t',index_col=['Genome']) #read csv 
#print data
print "Loaded data"

#n_samples = 52
seed = np.random.RandomState(seed=3)
##data = seed.randint(0, 20, 2 * n_samples).astype(np.float)
#data = data.reshape((n_samples, 2))
## Center the data
data -= data.mean()
print "Transposing Data"
data = data.transpose()
print "Calculating euclidean distances"
similarities = euclidean_distances(data)
print "Running MDS"
# Add noise to the similarities
#noise = np.random.rand(n_samples, n_samples)
#noise = noise + noise.T
#noise[np.arange(noise.shape[0]), np.arange(noise.shape[0])] = 0
#similarities += noise

mds = manifold.MDS(n_components=2, max_iter=3000, eps=1e-9, #random_state=seed,
                   dissimilarity="precomputed", n_jobs=-1)
pos = mds.fit(similarities).embedding_

nmds = manifold.MDS(n_components=2, metric=False, max_iter=3000, eps=1e-12,
                    dissimilarity="precomputed", random_state=seed, n_jobs=-1,
                    n_init=1)
                    
npos = nmds.fit_transform(similarities, init=pos)

# Rescale the data
#pos *= np.sqrt((data ** 2).sum()) / np.sqrt((pos ** 2).sum())
#npos *= np.sqrt((data ** 2).sum()) / np.sqrt((npos ** 2).sum())

# Rotate the data
clf = PCA(n_components=2)
data = clf.fit_transform(data)

pos = clf.fit_transform(pos)

npos = clf.fit_transform(npos)

fig = plt.figure(1)
ax = plt.axes([0., 0., 1., 1.])

plt.scatter(data[:, 0], data[:, 1], c='r', s=52)
plt.scatter(pos[:, 0], pos[:, 1], s=52, c='g')
plt.scatter(npos[:, 0], npos[:, 1], s=52, c='b')
#plt.legend(('True position', 'MDS', 'NMDS'), loc='best')

similarities = similarities.max() / similarities * 100
similarities[np.isinf(similarities)] = 0

# Plot the edges
start_idx, end_idx = np.where(pos)
#a sequence of (*line0*, *line1*, *line2*), where::
#            linen = (x0, y0), (x1, y1), ... (xm, ym)
segments = [[data[i, :], data[j, :]]
            for i in range(len(pos)) for j in range(len(pos))]
values = np.abs(similarities)
lc = LineCollection(segments,
                    zorder=0, cmap=plt.cm.hot_r,
                    norm=plt.Normalize(0, values.max()))
lc.set_array(similarities.flatten())
lc.set_linewidths(0.5 * np.ones(len(segments)))
ax.add_collection(lc)

plt.show()