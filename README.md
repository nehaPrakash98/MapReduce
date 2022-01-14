## CS532: Map Reduce Implementation in Java

## Team Members 
1. Chirag Uday Kamath
2. Divya Maiya
3. Neha Prakash

## Project Organization

### How to run the project

Run the following commands in `map-reduce-teamcdn-2` directory: 

**Windows**
```
$ ./runnerScriptWindows.sh
```

**Linux**
```
$ chmod +x runnerScriptMac.sh
$ ./runnerScriptMac.sh
```

### How to test fault tolerance
Run the following commands in map-reduce-teamcdn-2 directory:

**Windows**
```
$ ./runnerScriptWindowsFaultTolerance.sh
```

**Linux**
```
$ chmod +x runnerScriptMacFaultTolerance.sh
$ ./runnerScriptMacFaultTolerance.sh
```

### Project Structure

This project is our implementation of Map Reduce library. The entire source code for this project is contained within `src/main`. 

The `src/main/intermediate` directory consists of the intermediate file (inter.txt) that gets created once the `MapWorker` (our implemetation of Map) is executed. This intermediate file corresponds to the output of the map function.

The `src/main/map` directory corresponds to the implementation of the Map function. It consists of a `MapperInterface` and a `MapWorker` class. For the intermediate milestone, the `Master` class reads the input from the specified text file and calls the `MapWorker` with the input as a parameter. The `MapWorker` invokes the user-defined map function. The output is written to an intermediate file. 

The `src/main/reduce` directory corresponds to the implementation of the Reduce function. It consists a `ReducerInterface` and a `ReduceWorker` class. For the intermediate milestone the reducer reads the intermediate file written after the execution of the `MapWorker`. The `ReduceWorker` then sorts the input so as to group all relavant keys together. Then, the user-defined reduce function is invoked. 
The output of the user-defined reduce function is considered as the final output. 

The `src/main/master` directory consists of the implementation for `Master`. For the intermediate milestone, the `Master` reads the input files, then calls the `MapWorker`. Then it invokes the `ReduceWorker` with the necessary details.

The `src/main/utils/FileUtil` directory consists of all the utils needed to read and write from and to a file/directory. It also has any additional util functions that were needed during the development of the project.

The `src/main/input` and the `src/main/output` consists of the input and output directories respectively. Please read the below sections for more explanation. 

The `src/main/applications` directory consists of the 3 test cases or examples. More information about these examples can be found below. 

### Inputs

All the input files that are used by this project are contained in the path `src/main/input`. This directory contains 3 input files corresponding to the applications/examples implemented for this project. More details are explained in the below section.

### Examples implemented to demonstrate the working of the project

The examples/applications are in the `src/main/applications` directory. This inturn has 3 sub-directories that contains directories corresponding to the 3 aforementioned examples. 

**Application Logs**

Given a dump of application logs, find all the error codes such as 500, 404 and 304

Input: 
```
02:49:12 127.0.0.1 GET / 200
02:49:35 127.0.0.1 GET /index.html 200
03:01:06 127.0.0.1 GET /images/sponsered.gif 304
03:52:36 127.0.0.1 GET /search.php 200
04:17:03 127.0.0.1 GET /admin/style.css 404
05:04:54 127.0.0.1 GET /favicon.ico 404
```

Output:
```
304 1
404 2
```


**MutualFriends**

For each pair of people in a social network, count the mutual friends
* Input: A list of pairs consisting of mapping between a person and his/her friends. For example: A: B, C, D shows that a person A has friends B, C, D. 
* Output: The output is a pair mapping from a pair of users to their mutual friends. For example, (A, B) > (C, D) implies that C, D are the mutual friends of (A, B).

Input:
```
A -> B C D
B -> A C D E
C -> A B D E
D -> A B C E
E -> B C D
```

Output:
```
(A B) -> (C D)
(A C) -> (B D)
(A D) -> (B C)
(B C) -> (A D E)
(B D) -> (A C E)
(B E) -> (C D)
(C D) -> (A B E)
(C E) -> (B D)
(D E) -> (B C)
```

**Word Count**

Given an input file containing a number of sentences/words, populate an output file containing the number of occurances of each word in the input. 

Input:
```
This is a project for a class
```

Output:
```
a 2
class 1
for 1
is 1
project 1
This 1
```

**Server Requests**

Given a list of IP addresses that sent requests to a particular server, populate an output file that only consists of IP addresses that made over a certain threshold (10 in our case) number of requests 

Input:
```
121.3.5.6
192.168.0.1
192.168.0.1
192.168.0.1
192.168.0.1
192.168.0.1
192.168.0.1
121.3.5.6
192.168.0.1
192.168.0.1
192.168.0.1
192.168.0.1
192.168.0.1
```

Output:
```
192.168.0.1 11
```

### Outputs

**Please note that the output takes around a minute to get generated.**

All the output files are written to the directory with the path `src/main/ouput`. 

The expected outputs generated using the Spark program are prefixed with `expected`

### Comparing output with expected output

The `src/main/utils/FileUtil` directory contains a `fileComparer` function that compares the expected output (generated with the Spark program) with the actual output. 
The result is printed on the output when the Shell Script is run.
