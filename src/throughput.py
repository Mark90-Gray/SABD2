# importing the requests library 
import requests 
import time
import sys


# api-endpoint 


if __name__ == '__main__':
	URL = ""
	URL1 = ""
	
	name_file=str(sys.argv[1])
	size_window=str(sys.argv[2])	

	var = 1
	f = open("testIN"+name_file+size_window+".txt", "a+")
	g = open("testOUT"+name_file+size_window+".txt", "a+")
	while var == 1 :

	# sending get request and saving the response as response object 
		r = requests.get(url = URL) 
		k = requests.get(url = URL1)

	# extracting data in json format 
		data = r.json() 
		data2 = k.json()
	# extracting latitude, longitude and formatted address 
	# of the first matching location 
		throughput_medio= data[0]['avg'] 
		throughput_medio1= data2[0]['avg']
		t= time.time()
		
	# printing the output 
		f.write("%s; %s\n" %(t,throughput_medio))
		g.write("%s; %s\n" %(t,throughput_medio1))
		print "tempo:%s, th:%s" %(t,throughput_medio)
		print "tempo:%s, thOUT:%s" %(t,throughput_medio1)
		time.sleep(1)
		 

