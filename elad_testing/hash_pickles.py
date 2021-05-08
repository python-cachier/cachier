import pickle
import hashlib


obj = pickle.dumps(['a', 'b', 'c'])
print(hashlib.sha256(obj).hexdigest())