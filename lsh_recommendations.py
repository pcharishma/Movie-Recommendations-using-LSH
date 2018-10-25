from pyspark import SparkConf,SparkContext
import sys
import time
from collections import defaultdict
from itertools import combinations
from collections import Counter
APP_NAME = "INF553"

def top3recommendation(top5candidates,user):
	top3reco={}
	cc={}
	for k,v in top5candidates.items():
		cc[k] = []
		temp = []
		for v1 in v:
			temp.extend(matrix1[user[v1]])
		counter=Counter(temp)
		for movie, counts in counter.items():
			x = (movie,counts)
			cc[k].append(x)



	for u,m in cc.items():		
		if len(cc[u])<=3:
			top3reco[user[k]] = [m1[0] for m1 in m]
		else:
			z =[]
			for m1 in range(0,len(m)):
				if (cc[u][m1][1]) is True:
					z.append(cc[u][m1][1])
					z.sort(reverse=True)
					z=z[:3]

				if cc[u][m1][1] not in z:
					cc[u].remove(cc[u][m1])
			top3reco[user[k]].append(cc[u][m])
	return(top3reco)

def top5users(x,jaccardsim):
	reco = {}
	for i in x:
		if i[0] not in reco:
			reco[i[0]] = [i[1]]
		else:
			reco[i[0]].append(i[1])
        
		if i[1] not in reco:
			reco[i[1]] = [i[0]]
		else:
			reco[i[1]].append(i[0])
			
	top5candidates={}
	for k,v in reco.items():
		if len(v) <= 5:
			if k not in top5candidates:
				top5candidates[k]=v
		else:
			aa = {}
			a1 = []
			for v1 in v:
				a = [v1,k]
				a.sort()
				a=tuple(a)
				if a in jaccardsim.keys():
					if jaccardsim[a] != 0:
						aa[jaccardsim[a]] = a
						a1.append(jaccardsim[a])
			a1.sort(reverse =True)
			if len(a1)>5:
				a1 = a1[:5]
            
			for a11 in a1:
				if a11 in aa.keys():
					temp = list(aa[a11])
					temp.remove(k)
                
					if k not in top5candidates:
						top5candidates[k] = temp
					else:
						top5candidates[k].extend(temp)
	

def jaccard_similarity(x,user,matrix1):
	jaccardsim={}
	for i in x:
		jaccardsim.setdefault(i,"")
	for i in x:
		setA = set(matrix1[user[i[0]]])
		setB = set(matrix1[user[i[1]]])
		intersection = setA & setB
		union = setA | setB
		jaccardsim[i] =  float(len(intersection)/len(union))
	return(jaccardsim)
	
	
def locality_sensitive_hashing(sigmatrix):
	lsh =[]
	lsh.append(sigmatrix[0:4])
	lsh.append(sigmatrix[4:8])
	lsh.append(sigmatrix[8:12])
	lsh.append(sigmatrix[12:16])
	lsh.append(sigmatrix[16:20])
	index = [i for i in range(0,len(user))]
	x=[]
	for result in combinations(index,2):
		x.append(result)
	for l in range(0,len(lsh)):
		for i in x:
    
			temp1=lsh[l][0]
			temp2=lsh[l][1]
			temp3=lsh[l][2]
			temp4=lsh[l][3]
        
			if temp1[i[0]] == temp1[i[1]]:
				if temp2[i[0]] == temp2[i[1]]:
					if temp3[i[0]] == temp3[i[1]]:
						if temp4[i[0]] == temp4[i[1]]:
							continue
			else:
				x.remove(i)
				
	return(x)
	

def minhashing(mat2):
	rows = len(mat2)
	cols = len(mat2[0])
	sigrows = 20

	#initializing 

	sigmatrix = []
	for i in range(sigrows):
		sigmatrix.append([sys.maxsize]*cols)
    
	for r in range(rows):
		# if data != 0 and signature > hash value, replace signature with hash value
		for c in range(cols):
			if mat2[r][c] == 1:
				for i in range(sigrows):
					if sigmatrix[i][c] > ((3*r)+(13*i))%100:
						sigmatrix[i][c] = ((3*r)+(13*i))%100
	return(sigmatrix)

def matrix(users,user,movies):
	mat1={}
	mat2=[]
	for k,v in users.items():
		for v1 in v:
			if v1 not in mat1:
				mat1[v1]=[0]*len(user)
	for u in range(0,len(user)):
		temp = users[user[u]]
		for t in temp:
			mat1[t][u]=1
	for i in movies:
		mat2.append(mat1[str(i)])
	return(mat2)
	
	

def user_movie(data):
	users ={}
	movies=[]
	user=[]
	for i in data:
		if i[0] not in users:
			users[i[0]] = i[1:len(i)]
			user.append(i[0])
		for j in i[1:len(i)]:
			if j not in movies:
				movies.append(j)
	movies.sort(key = int)
	return(users,user,movies)

def main(sc,inp):
	start_time = time.time()
	inpFile = sc.textFile(inp)
	data=[]
	for i in inpFile.collect():
		x = [j for j in i.replace('u','').split(",")]
		data.append(x)
	return(data)


if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf =conf)
	inp = sys.argv[1]
	out = sys.argv[2]
	
	data = main(sc,inp)
	matrix1, user, movies = user_movie(data)
	mat2 = matrix(matrix1, user, movies)
	signature_matrix = minhashing(mat2)
	candidates = locality_sensitive_hashing(signature_matrix)
	
	jaccardsim = jaccard_similarity(candidates, user, matrix1) 
	
	top5candidates = top5users(candidates,jaccardsim)
	
	top3reco = top3recommendation(top5candidates, user)
	
	f = open(out+, "w")
    f.write('\n'.join('{}]]'.format(str(x).strip('[]')) for x in sorted([(x,v) for x,v in top3reco.items() if x is not None])))
    f.close()
	