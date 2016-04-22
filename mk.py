import sys
import random
def write():
    print('Creating new text file') 
    try:
        file = open("input.txt", "w")
        for a in range(0,10):
			file.write('%s,%s\n' % (random.choice('abcd'),random.choice('efgh') ))
					
				
        file.close()
    except:
        print('Error!!')
        sys.exit(0) # quit Python

write()
