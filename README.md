# BigData

Problem:

A program that traverses a Linux directory structure as fast as possible
and reports duplicate files. Duplicates are files with same content.

Description

The “force brute” algorithm for this problem is to compare each file in the directory to all other files.
So if we have n files, the time complexity for this problem is in the order of O(n.log(n)). In order to
reduce time complexity of the problem, one approach is to distribute the processing over a cluster of
computers. So if we use p machines for this problem and distribute the file comparison task, the time
complexity can be reduced to O(n/p.log(n)) where p is the number of machines. The approach used for
this problem is the Master/Slave architecture. In this architecture, a group of slave machines is tasked
with comparing different blocks of files. Each slave machine will identify similar files, if any, in one
block of files and sends the comparison result to the Master. The Master will coordinate the whole
process and consolidate the results.

Following is a high level description of the algorithm:

1. A Master process reads the input directory and divides all files into many blocks of
predetermined size.
2. The master sends the blocks of files to all slave processes.
3. The Master asks every slave process to compare one file to all files in one block of files.
4. The Master will assign comparisons task to all available slaves in parallel.
5. Every Slave sends the result of one comparison to the Master who then assigns the next task to
that Slave.
6. After comparing all files to all blocks of files, the Master compiles the results received from the
different Slaves and then displays the final result.

The program was developed with mono C# (Mono Develop IDE) on a Linux machine running Ubuntu
17.4. The program consists of the following files:

- Program.cs.
- MasterProcess.cs.
- SlaveProcess.cs
- XML configuration file configFile.xml.

The configuration files identifies the following parameters:

IP address and port number for every slave process.
Block size that defines the size of the blocks of files (in MB).
The program uses asynchronous sockets for sending and receiving files and most coordination
commands between the Master and Slave processes. Asynchronous sockets callbacks are called
recursively on both ends to handle large amount of data. For example, Slaves keep calling the
asynchronous socket’s callback recursively to copy a large file received from the Master. Many thread
synchronization objects are used to coordinate the flow of messages and data exchanged between the
Master and the Slaves.
Use the following command to run the Slave process (Salves need to be started first) : mono
program_name.exe slave portnumber
Use the following command to run the Master process : mono program_name.exe master directory
configfile. 

Following is the description of the command line parameters:


portnumber is the port number for the slaves (needs to match port number for that slave in
config. file).
directory is the location of the folder whose files will be compared to each other.
Configfile is the location of the xml configuration file.
For example, the following commands were used to run the Master & Slave processes:
Master:
mono BigData.exe master home/adel/Documents/Projects/BigData/Downloads
/home/adel/Documents/configFile.xml
Slave :
mono BigData.exe slave 2000

The program was tested with following configurations:

- Size of input directory approximately 80 MB.
- Number of files in input directory: 2910 files.
- Block size tested 7 MB => program created 10 block (indexed 0 to 9).
- 1 st set of tests:
◦ Master and Slave processes running on same computer (DELL OptiPlex 745 - Core2 CPU
@ 2.66GH).
◦ Total running time was 27 minutes.
- 2 nd set of tests:
◦ Master and 1 Slave process running on one computer (DELL OptiPlex 745 – 6GB RAM &
Core2 CPU @ 2.66GH).
◦ A second Slave process running on a second computer (MacBook Pro - 8 GB RAM &
Core2 CPU @ 2.53GH).
◦ Total running time was 14 minutes.

Notes

- Since Master and Slave processes create blocks of files locally (in program working folder) they
must have enough disk space for the newly created blocks (at least as much space as the input
directory).
- Ideally run Master and Slave processes in same subnet to avoid communication problems.
- This program is not a production level application. Most “rainy days” scenarios have been
identified in the code but some have not been implemented. For example, the program detects if
a file is received with the wrong hash code but nothing is done. The slave should keep track of
all corrupted files and ask Master to send again before starting the comparison phase.
- Timers were added on both sides to re-send some critical messages if they’re not properly
received. Not all cases have been implemented so if some critical coordination messages are
lost (or not received in the right sequence) the program might just hang forever (thread waiting
for a signal). If this happens please restart the program.
- The program identifies soft links. These files are simply disregarded from the initial file set.
