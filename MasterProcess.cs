using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Collections;
using System.Security.AccessControl;
using System.Threading;
using System.Security.Cryptography;
using System.Diagnostics;
using Mono.Unix;


namespace BigData
{

	// Master process class
	public class MasterProcess
	{

		// Input directory entered in program command line
		string dir = null;

		// Master configuration file name entered in program command line
		string configFile = null;

		// Size of block of data (i.e. set of files) sent to each slave
		// Read from config. file

		static double blockSize = 0.0;


		/// <summary>
		/// State of slaves 
		/// </summary>
		enum SlaveState
		{
			READY,
			BUSY,                // Salve is Busy comparing one file to files in on block
			WAITING_FILE_SAVE,   // Waiting to receive next file to be stored locally
			WAITING_FILE_COMPARE // Waiting to receive next file to be compared to all files 
			                     // in one block
		}


		/// <summary>
		/// Master keeps local information about Slave processes
		/// </summary>
		private class Slave
		{
			public int id;              // slave process Id
			public Socket socket;   
			public SlaveState state;     // slave curetn state
			public string ipAddress;    // slave host IP v4 address
			public int portNumber;
			public long nbComparaisons; // used to store final statistics
			                            // about comparaison done on Slave host
		}

		/// <summary>
		/// Using assynchronous sockets to send data between M & S.
		/// This event is used to coordinate threads
		/// </summary>


		static AutoResetEvent waitAckFileReceived = new AutoResetEvent(false);
		static AutoResetEvent waitInfoSent = new AutoResetEvent(false);
		static AutoResetEvent waitCompare = new AutoResetEvent(false);
		static AutoResetEvent waitStateUpdate = new AutoResetEvent(false);
		static List<AutoResetEvent> waitInfoReceived = new List<AutoResetEvent>();
		static List<AutoResetEvent> waitSlavesCompare = new List<AutoResetEvent>();

		// keeps track of all slave info

		static Hashtable slaves = new Hashtable();

		// Initial list of long Filenames

		static string[] files = null;

		// list of moodfied file names

		static string[] newFiles = null;

		/// <summary>
		/// Enumneration defining coordination commands exchanged between master and slave processes
		/// </summary>
		public enum Command
		{
			Info = -1, // From Master to Slave : sends information regarding the blocks of files that are going to be sent
			CopyFile = 1, // From Master to Slave : during Initialization phase notifies slave that file needs to be stored locally
			NextFile = 2, // From Slave to Master : during Initialization phase notifies master to send next file to be stored locally
			Compare = 3, // From Master to Slave : during Comparaison phase notifies slave that file needs to be compared to all files in one block
			Result = 4, // From Slave to Master : during Comparaison phase send comparaison result to Master. Master will then send next file to compare.
			End = 5  // From Master to Slave : End. Slave then closes sockets & exit. 
		}

		/// <summary>
		/// This list keeps track of the number of files in each block.
		/// </summary>
		static List<int> blocks = new List<int>();

		//receive messages from slave processes
		static byte[] receiveBuffer = new byte[10024];

		// Index of the current file to be compared to a set of files (i.e. block) by one available slave
		static int currFileCompareIdx = 0;

		// Index of the block (i.e. set of files) whose files will be compared to current file
		static int currentBlockFileCompareIdx = 0;

		// file & block index of the current file being sent to all slaves to be stored locally
		// used to resend file if no acknowledged by one of the slaves
		static int currentFileSentIdx = 0;
		static int blockIdxcurrentFileSent = 0;

		static int  currentFileCmpIdx =0;
		static int currentFileCmpBlkIdx = 0;
		static int currentBlkCmpIdx = 0;


		// used to store size of data to be received from Slave processes - used to validate
		// received data

		static Hashtable currentDataSize = new Hashtable();

		public static System.Timers.Timer sentFilesTimer = new System.Timers.Timer(5000);
	
		// Used to store list of similar files after comparing each file
		// to all files in one block by one slave process

		static List<BigData.SlaveProcess.SimilarFiles> similarFiles = new List<SlaveProcess.SimilarFiles>();

		static bool keepReading = false;
		static byte[] results = null;

		static private System.Object lockRecAsyncReceive = new System.Object(); // mutex  variable for coordinating access to 
																				// asynch socket receive call back from different threads
		static TimeSpan ts; // total duration 

		// used for debugging

		static bool verbose = true;  // compile with false to turn Verbose off
		static StreamWriter log;  // log file



		/// <summary>
		/// Initializes a new instance of the <see cref="BigData.MasterProcess"/> class.
		/// </summary>
		/// <param name="inpD">Inp d.</param>
		/// <param name="confF">Conf f.</param>
		/// <param name="mode">Mode.</param>
		public MasterProcess(string inpD, string confF)
		{
			dir = inpD;
			configFile = confF;
			sentFilesTimer.Elapsed += OnTimedEvent;

			log = new StreamWriter("MasterLog.txt");

		}

		/// <summary>
		/// Initializes & connects sockets to Slave Host machines
		/// </summary>
		private void ConnectSockets()
		{
			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;

				Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

				IPAddress ipAddr = IPAddress.Parse(slave.ipAddress);

				IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, slave.portNumber);

				// begins assychonous connection to remote slave
				socket.BeginConnect(ipEndPoint, new AsyncCallback(ConnectCallback), socket);
			
				slave.socket = socket;
			}

		}

		/// <summary>
		/// check if file has been sucessfully received
		/// </summary>
		/// <param name="source">Source.</param>
		/// <param name="e">E.</param>
		private static void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)
		{
			// if timer expires
			// checks if all slaves are waiting for next file and if that's the case
			// signal event to send next file

			if (CheckSlavesState(SlaveState.WAITING_FILE_SAVE))
			{
				waitAckFileReceived.Set();
				return;
			}
				
			// if timer expires and one or more slaves haven't acknowledged
			// reception then send again

			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;

				if (slave.state != SlaveState.WAITING_FILE_SAVE)
				{
					int fileIdx = MapBlkFileIdxToFileIdx(blockIdxcurrentFileSent, currentFileSentIdx);
					string file = files[fileIdx];
					AsynchronousFileSend(false, slave.socket, Command.CopyFile, file, blockIdxcurrentFileSent, currentFileSentIdx, fileIdx);

				}
			}
		}

		/// <summary>
		/// Reads the configuration file and determine all parameters
		/// </summary>
		/// <returns><c>true</c>, if config was  read, <c>false</c> otherwise.</returns>
		/// <param name="configFile">Config file.</param>
		bool ReadConfig(string confFile)
		{
			int id = 0;

			Console.WriteLine("Reading configuration file\n");
			log.WriteLine("Reading configuration file\n");

			Slave slave = null;
			XmlReader reader = XmlReader.Create(confFile);
			{
				while (reader.Read())
				{
					if (reader.IsStartElement())
					{
						switch (reader.Name.ToString())
						{
						case "IP":
							string ipAd = reader.ReadString ();
							slave = new Slave ();
							slave.id = id;
							slave.ipAddress = ipAd;
							slaves.Add (ipAd, slave);
							AutoResetEvent waitSlaveCompare = new AutoResetEvent (false);
							waitSlavesCompare.Add (waitSlaveCompare);

							AutoResetEvent waitInfo = new AutoResetEvent (false);
							waitInfoReceived.Add (waitInfo);

							id++;
							break;
						case "PortNumber":
							string p = reader.ReadString();
							int portNumber = Int32.Parse(p);
							slave.portNumber = portNumber;
							break;
						case "BlockSize":
							string s = reader.ReadString();
							blockSize = double.Parse(s);
							break;

						}
					}
				}
			}

			return true;
		}

		/// <summary>
		/// Returns the index of next available slave process from pool of available slaves
		/// or null otherwise
		/// </summary>
		Slave GetNextAvailableSlave()
		{
			Slave slv = null;
			lock (slaves) 
			{
				foreach (DictionaryEntry entry in slaves) 
				{
					Slave slave = (Slave)entry.Value;
					if (slave.state == SlaveState.WAITING_FILE_COMPARE) 
					{
						slv = slave;
						break;
					}
				}
			}
			return slv;
		}

		/// <summary>
		/// Modifies file name as follows:
		/// 1) prefixes file index in block
		/// 2) replaces slash with underscore
		/// </summary>
		/// <returns>The new file name.</returns>
		/// <param name="name">Name.</param>
		/// <param name="idx">Index.</param>
		static string ModifyFileName(string name, int idx)
		{
			string	mName = name.Replace (@"/", "_");
			mName = Convert.ToString (idx) + mName;
			return mName;
		}
		/// <summary>
		/// Creates the sets of file.
		/// This methods creates equal blocks of files (size blockSize defined in config. file)
		/// These blocks of files are saved in sub -directories
		/// Assumes tha Master host has enough space to keep these new sub-directories.
		/// </summary>
		private void CreateBlocksOfFile()
		{
			// starts by creating equals set of file

			int blockIndex = 0;
			int fileIndex = 0;
			long size = 0;
			double sizeD = 0.0;
			bool createBlock = true;
			int blockFileNb = 0;

			// Gets all filenames in identified directory

			string[] fileList = Directory.GetFiles(dir, "*.*", SearchOption.AllDirectories);

			// checks all files & disregards any identified soft links
			DisregardSymbolicLinks (fileList);

			// Create the list of modified files names
			// Since all files will be copied to new blocks (on master & slaves) their names are modified as follows:
			// For example:
			// 1) file name : home/adel/Documents/Projects/BigData/Downloads/388855.jpg modified to
			// 2) New file name: 0_home_adel_Documents_Projects_BigData_Downloads_388855.jpg 
			// The program will parse the intial directory and copy files to blocks named block0, block1, block3 etc. created
			// as subdir to current working folder. The block size is set in the configuration file. The program will copy up to the 
			// predefined size to every block (-/+ the size of the last file). 

			newFiles = new string[files.Length];


			DirectorySecurity securityRules = new DirectorySecurity();
			securityRules.AddAccessRule(new FileSystemAccessRule("Everyone", FileSystemRights.FullControl, AccessControlType.Allow));

			DirectoryInfo dest = null;

			Console.WriteLine("Creating local blocks of files\n" + Environment.NewLine);
			log.WriteLine("Creating local blocks of files\n");


			//gets current directory

			string path = Directory.GetCurrentDirectory();

			string subDir = "BigData_M";

			path += @"/" + subDir;

			if (Directory.Exists (path)) // removes directories that may have been created in previous runs
			{ 
				Directory.Delete (path, true);
			} 

			while (createBlock)
			{
				
				var newDir = subDir + "/"+"block" + Convert.ToString(blockIndex);

				dest = Directory.CreateDirectory (newDir);

				while (fileIndex < files.Length)
				{
					string sourcefile = files[fileIndex];
					string fileName = Path.GetFileName(sourcefile);

					string newfileName = ModifyFileName(sourcefile, blockFileNb);

					string desFileName = dest.FullName + "/" + newfileName;

					string destFile = Path.Combine(dest.FullName, desFileName);

					File.Copy(sourcefile, destFile);

					// keeps track of modified files names
					newFiles[fileIndex] = newfileName;

					blockFileNb++;

					fileIndex++;

					FileInfo fInf = new FileInfo(destFile);
					size += fInf.Length;

					// Converts Bytes to Megabytes
					// since block size value in config file
					// is in MB

					sizeD = (size / 1024f) / 1024f;

					if (fileIndex == files.Length)
					{
						createBlock = false;
						sizeD += blockSize;
					}

					if (sizeD >= blockSize)
					{
						// saves nb files in current block (i.e. folder)
						blocks.Add(blockFileNb);

						// next block
						blockIndex++;
						size = 0;
						blockFileNb = 0;
						break;
					}

				}

			}
		}
		/// <summary>
		/// Map block index and file index in block
		/// to file index in orginal file list
		/// </summary>
		/// <param name="block">Block index</param>
		/// <param name="fileNb">File index.</param>
		static int MapBlkFileIdxToFileIdx(int blockIdx, int fileIdx)
		{
			int index = 0;

			for (int i = 0; i < blockIdx; i++)
			{
				index += blocks[i];
			}
			return (index + fileIdx);
		}
		/// <summary>
		/// Maps a file index in original set of files
		/// to a block index & file index in block
		/// </summary>
		/// <param name="fileIdx">index in orginal set</param>
		/// <param name="blockNb">Block index.</param>
		/// <param name="fIdx">File index in block</param>
		void MapsFIdxtoBckNbFIndex(int fileIdx, out int blockIdx, out int fIdx)
		{
			int i = 0;
			int nbFiles = 0;

			do
			{
				nbFiles += blocks[i];
				i++;
			}
			while (fileIdx >= nbFiles);


			blockIdx = i - 1;
			fIdx = fileIdx - (nbFiles - blocks[i - 1]);
		}

		/// <summary>
		/// Start this instance.
		/// </summary>
		public void Start()
		{

			Console.WriteLine("The Master process has started\n" + Environment.NewLine);
			log.WriteLine("The Master process has started\n");
			// read the config file to determine parameters

			ReadConfig(configFile);

			// starts a timer to measure execution time

			var watch = Stopwatch.StartNew();

			CreateBlocksOfFile();

			// Create & connect the sockets

			ConnectSockets();

			// Send block struture information to all slaves
			// nb of blocks & nb of files in each block

			//SendBlockInfoToSlaves();

			SendCmdToSlaves (Command.Info);

			waitInfoSent.WaitOne();

			// Send all files to every slave.
			// Slaves will create blocks of files & store all files localy
			// Master will then assign file comparaison tasks to every slave

			SendAllFilesToSlaves();

			// Update the state of each slave to "WAITING_FILE_COMPARE" meaning
			// ready to compare 1 identified file to all files in one identified block

			UpdateSlavesState(true, null, SlaveState.WAITING_FILE_COMPARE);

			// Start comparing files

			Compare();

			// measures elapsed time

			watch.Stop();

			ts = watch.Elapsed;

			SendCmdToSlaves (Command.End);

			// waits until statistics information is received from
			// all slaves

			WaitHandle.WaitAll (waitInfoReceived.ToArray ());
	
			PrintStatistics ();

			log.Close();

			CloseSockets ();

		}
	
		/// <summary>
		/// Disregards symbolic links.
		/// </summary>
		static void DisregardSymbolicLinks(string[] fileL)
		{
			int dim = fileL.Length;
			int[] indexes = new int[dim];
			int count = 0;
			for (int i = 0; i < dim; i++) 
			{
				if (IdentifyLink(fileL[i]))
				{
					indexes [count] = i; 
					count++;
				}
			}

			files = new string[dim-count];

			int j = 0;
			for (int i = 0; (i < dim-count && i != indexes [j]); i++) 
			{
				files [j] = fileL [i];
				j++;
			}
		}

		/// <summary>
		/// Compare every file in set to all files in every block
		/// Taks handed to every available slave.
		/// </summary>
		void Compare()
		{
			bool loop = true;

			string output = "\n\n-----------------------------" +
			                "\nStarting the comparaison phase\n" +
			                "\n-----------------------------\n\n";
			Console.WriteLine(output);
			log.WriteLine(output);


			while (loop)
			{
				Slave slave = null;

				while (((slave = GetNextAvailableSlave()) != null) && (currFileCompareIdx < files.Length))
				{
					SendFileToCompare(currFileCompareIdx, currentBlockFileCompareIdx, slave);

					waitStateUpdate.WaitOne (); // blocks until slave's state gets updated to BUSY in different thread
					// before evaluating the while loop again. 

					currentBlockFileCompareIdx++;

					if (currentBlockFileCompareIdx == blocks.Count)
					{
						// if current file has been compared to files in all blocks

						// move to next file

						currFileCompareIdx++;

						if (currFileCompareIdx == files.Length)
						{
							loop = false;
							break;
						}

						// and start comparaison from  1st block

						currentBlockFileCompareIdx = 0;
					}
				}
			}

			// finished comparing files

			ProcessResults();
		}

		/// <summary>
		/// Request the next available slave process to compare file indentified by file index
		/// to all files in block identified by block index (hosted on remote slave)
		/// The file is not sent but identified by its indexes
		/// </summary>
		void SendFileToCompare(int fileIdx, int blkIdx, Slave slv)
		{
			byte[] buffer = null;

			// file index in original list is mapped
			// to block index & file index

			int blockIndex = 0;
			int fileIndex = 0;

			// maps the file index in original list to block index & file index

			MapsFIdxtoBckNbFIndex(fileIdx, out blockIndex, out fileIndex);

			if (verbose)
			{
				string output = String.Format("Requesting slave {0} to compare file {1} to all files in block{2}\n", slv.ipAddress, files[fileIdx], blkIdx);
				Console.WriteLine(output);
				log.WriteLine(output);
			}

			using (var memoryStream = new MemoryStream())
			{
				using (BinaryWriter writer = new BinaryWriter(memoryStream))
				{
					writer.Write((int)Command.Compare);
					writer.Write(blockIndex); // block index of file that will be compared to all other files in block
					writer.Write(fileIndex);  // file index in block 
					writer.Write(blkIdx);     // index identifying block whose files will be compared to original file
				}

				buffer = memoryStream.ToArray();
			}
				

			StateObject state = new StateObject ();
			state.workSocket = slv.socket;


			// update slave's state accordingly

			UpdateSlavesState (false, slv.socket, SlaveState.BUSY);


			slv.socket.BeginSend(buffer,0,buffer.Length,0,new AsyncCallback(CompareSendCallback), state);

			waitStateUpdate.Set ();
		
		}

		/// <summary>
		/// Sends all blocks of files to all slaves.
		/// </summary>
		void SendAllFilesToSlaves()
		{
			int index = 0;

			string output = "\n\n----------------------------------------" +
				            "\nSending all blocks of files to all salves\n" +
				            "\n-----------------------------------------\n\n";
			Console.WriteLine(output);
			log.WriteLine (output);


			//sentFilesTimer.AutoReset = true;

			for (int j = 0; j < blocks.Count; j++)
			{
				output = String.Format("Sending all files in block{0} to all salves\n" + Environment.NewLine, j);
				Console.WriteLine(output);
				log.WriteLine(output);

				for (int i = 0; i < blocks[j]; i++)
				{

					string file = files[index];



					// keep track of current file being sent to all slaves
					currentFileSentIdx = i;
					blockIdxcurrentFileSent = j;

					// starts timer
					//sentFilesTimer.Enabled = true;

					AsynchronousFileSend(true, null, Command.CopyFile, file, j, i, index);

					index++;

					// waits for all slaves to acknowledge before sending next one
					waitAckFileReceived.WaitOne();
				}
			}
		}


		/// <summary>
		/// Master sends block struture info to all slaves
		/// Slaves use this info for validating received files
		/// </summary>
		void SendBlockInfoToSlaves()
		{
			byte[] buffer;

			using (var memoryStream = new MemoryStream())
			{
				using (BinaryWriter writer = new BinaryWriter(memoryStream))
				{
					writer.Write((int)Command.Info);

					//writes information regading the number of bocks to send to every slaves

					writer.Write((int)blocks.Count);

					for (int i = 0; i < blocks.Count; i++)
						writer.Write(blocks[i]);
				}

				buffer = memoryStream.ToArray();
			}

			// send same file (idendified by bock index & file index) to every slave process

			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;
				Socket sock = slave.socket;
				sock.Send(buffer);
			}

		}

		/// <summary>
		/// Sends the cmd identified to all slaves.
		/// </summary>
		/// <param name="cmd">Cmd.</param>
		void SendCmdToSlaves(Command cmd)
		{
			byte[] buffer = null;

			MemoryStream memoryStream = new MemoryStream ();
			BinaryWriter writer = new BinaryWriter (memoryStream);

			switch (cmd) 
			{

			case Command.Info:
				writer.Write ((int)cmd);

				//writes information regading the number of bocks to send to every slaves

				writer.Write ((int)blocks.Count);

				for (int i = 0; i < blocks.Count; i++)
					writer.Write (blocks [i]);
			
				buffer = memoryStream.ToArray ();
				break;

			case Command.End:
				writer.Write ((int)cmd);
				buffer = memoryStream.ToArray ();
				break;

			}
			// send same file (idendified by bock index & file index) to every slave process

			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;
				Socket sock = slave.socket;
				sock.Send(buffer); //sned synchronously since it's a small message
			}
			
		}
			
		/// <summary>
		/// Asynchronouses one file & set of parameters to all salves
		/// </summary>
		/// <param name="cmd">Cmd.</param>
		/// <param name="file">File.</param>
		/// <param name="blkIdx">Blk index.</param>
		static void AsynchronousFileSend(bool all, Socket socket, Command cmd, string file, int blkIdx, int fileIdxBlk, int fileIdx)
		{
			byte[] preBuffer;
			string name = null;
			string output = null;

			using (var memoryStream = new MemoryStream())
			{
				using (BinaryWriter writer = new BinaryWriter(memoryStream))
				{
					writer.Write((int)cmd);
					writer.Write((int)blkIdx);
					writer.Write((int)fileIdxBlk);

					name = Path.GetFileName(newFiles[fileIdx]);

					writer.Write(name);

					// sending file size for validation purposes
					// at the Slaves side

					long length = new System.IO.FileInfo(file).Length;

					writer.Write(length);

					// sending MD5 hashcode for validation purposes

					string hashcode = GetMD5HashFromFile(file);

					writer.Write(hashcode);


				}
				preBuffer = memoryStream.ToArray();
			}

			// send 

			if (!all) // resending file (idendified by bock index & file index) 

			{
				socket.BeginSendFile(file, preBuffer, null, 0, new AsyncCallback(FileSendCallback), socket);

				if (verbose)
				{
					output = String.Format("Resending file {0} : Block Index {1}", file, blkIdx);
					Console.WriteLine(output);
					log.WriteLine(output);
				}

			}

			else // send file (idendified by block index & file index) to every slave process

			{
				UpdateSlavesState(true, null, SlaveState.BUSY);

				foreach (DictionaryEntry entry in slaves)
				{
					Slave slave = (Slave)entry.Value;
					Socket sock = slave.socket;
					sock.BeginSendFile(file, preBuffer, null, 0, new AsyncCallback(FileSendCallback), sock);

					//sock.Send (preBuffer);

					if (verbose)
					{
						output = String.Format("Sending to salve {0}\n" +
							"File: {1}: block{2} : index{3}\n", slave.ipAddress, file, blkIdx, fileIdxBlk);
						Console.WriteLine(output);
						log.WriteLine(output);
					}
				}
			}
				
		}
			
		/// <summary>
		/// Handles asynchronous FileSend callback 
		/// </summary>
		/// <param name="ar">Asynchronous result object</param>
		private static void FileSendCallback(IAsyncResult ar)
		{
			// Retrieve the socket from the state object.

			Socket socket = (Socket)ar.AsyncState;

			// Complete sending the data to slave process.

			socket.EndSendFile(ar);

		}
			
		/// <summary>
		/// Handles asynchronous file compare request send callback
		/// </summary>
		/// <param name="ar">Asynchronous result object</param>
		private static void CompareSendCallback(IAsyncResult ar)
		{
			// Retrieve the socket from the state object.

			StateObject state = (StateObject)ar.AsyncState;
			Socket socket = state.workSocket;

			// Complete sending the data to slave process.

			socket.EndSend(ar);

			string ipAddress = ((IPEndPoint)(socket.RemoteEndPoint)).Address.ToString();

			Slave slv = (Slave)slaves [ipAddress];

			AutoResetEvent waitEvent = (AutoResetEvent)waitSlavesCompare [slv.id];

			waitEvent.WaitOne (); // this thread waits for the identified Slave process only
		
		}
			
		/// <summary>
		/// Handles asynchronous Connect callback.
		/// </summary>
		/// <param name="ar">Ar.</param>
		private static void ConnectCallback(IAsyncResult ar)
		{
			// Retrieve the socket from the state object.

			Socket socket = (Socket)ar.AsyncState;
			socket.EndConnect(ar);

			string ipAddress = ((IPEndPoint)(socket.RemoteEndPoint)).Address.ToString();

			string output = string.Format("Master has connected to slave: {0}\n", ipAddress);
			Console.WriteLine(output);
			log.WriteLine(output);

			//Setup receive callback

			StateObject state = new StateObject();
			state.workSocket = socket;

			socket.BeginReceive(receiveBuffer, 0, receiveBuffer.Length, SocketFlags.None, new AsyncCallback(DataReceivedCallBack), state);
		}

		/// <summary>
		/// Handles asynchronous Receive callback.
		/// </summary>
		/// <param name="ar">Ar.</param>
		private static void DataReceivedCallBack(IAsyncResult ar)
		{
			// Retrieve the socket from the state object.

			StateObject state = (StateObject)ar.AsyncState;
			Socket socket = state.workSocket;

			int bytes = socket.EndReceive(ar);

			lock (lockRecAsyncReceive)  // locking this part of the code to make sure it's accessed by one thread at a time
			{
				int size = 0;
				Command cmd = 0;

				string ipAddress = ((IPEndPoint)(socket.RemoteEndPoint)).Address.ToString ();
		
				// Processing data

				using (var memoryStream = new MemoryStream (receiveBuffer)) 
				{
					using (BinaryReader reader = new BinaryReader (memoryStream)) 
					{
						if (keepReading) 
						{ // if recursive call with cmd = Command.Result to read more data
							cmd = Command.Result;
						} 
						else 
						{ //(!keepReading) 
							cmd = (Command)reader.ReadInt32 ();
						}

						switch (cmd) 
						{

						// Slave is requesting another file to be stored remotely

						case Command.NextFile:
							int blkIdx = reader.ReadInt32 ();
							int fileIdx = reader.ReadInt32 ();
							if (verbose) 
							{
								string output = String.Format ("Received acknowledgment from slave {0}\n" +
								               "File block index: {1} file index: {2}\n", ipAddress, blkIdx, fileIdx);
								Console.WriteLine (output);
								log.WriteLine (output);
							}
						    // Updating the sending slave process' state to WAITING_FILE_SAVE
						    // meaning that slave machine is waiting for next file to be stored locally
							UpdateSlavesState (false, socket, SlaveState.WAITING_FILE_SAVE);

						    // if all slaves are in WAITING_FILE_SAVE state then signal event to send
						    // next file and stop timer (timer will be reset on next file sent)

							if (CheckSlavesState (SlaveState.WAITING_FILE_SAVE)) 
							{
								// if all slave sent acknowledgement then stop timer
								sentFilesTimer.Stop ();
								waitAckFileReceived.Set ();
							}

							break;

						case Command.Result: // Slave is sending the result of a comparaison
							try {
								int dim = 0;

								if (!keepReading) 
								{

									currentFileCmpIdx = reader.ReadInt32 ();
									currentFileCmpBlkIdx = reader.ReadInt32 ();
									currentBlkCmpIdx = reader.ReadInt32 ();

									dim = reader.ReadInt32 (); // read dimension of similar files list

									if (dim != 0) 
									{
										size = reader.ReadInt32 ();   // size of the byte array to be received

										currentDataSize [socket] = size; // keeps track of the expected size in case Similar files
										// list size is LARGE and we need to recursively read the socket.

										// first time --> size of results (the byte array for similar files list)
										// is equals to all bytes read from socket minus 24 bytes (size of 6 x Int32)

										results = new byte[bytes - 24]; // first read of results 

										if (bytes == size + 24) 
										{ // no more data coming
											keepReading = false;
											Buffer.BlockCopy (receiveBuffer, 24, results, 0, bytes - 24);
										} 

										else 
										
										{
											state.dataSize += (bytes - 24); // data received so far
											keepReading = true;
											break;
										}
									}
								} 

								else 
								
								{   
									// recursive asynchronous call to read LARGE list of similar files
									// resizes results bytes array (old length + size)

									Array.Resize (ref results, results.Length + bytes);

									// adds received socket bytes (i.e. receiveBuffer) to results byte array

									Buffer.BlockCopy (receiveBuffer, 0, results, results.Length, bytes);

									// update size of received data

									state.dataSize += bytes;

									int dataSize = (int)currentDataSize [socket];

									if (state.dataSize == dataSize) 
									{ // all data received
										keepReading = false;
									} 

									else 
									{
										keepReading = true;
										socket.BeginReceive (receiveBuffer, 0, receiveBuffer.Length, SocketFlags.None, new AsyncCallback (DataReceivedCallBack), state);
									}

								}

								if (verbose) 
								{
									int index = MapBlkFileIdxToFileIdx (currentFileCmpBlkIdx, currentFileCmpIdx);
									string name = files [index];
									string output = String.Format ("\nReceived comparaison result from slave {0}\n" +
									               "Comparaison of file {1}\n" +
									               "To all files in block{2}\n", ipAddress, name, currentBlkCmpIdx);
									Console.WriteLine (output);
									log.WriteLine (output);
								}

						
								if ((dim != 0) && (!keepReading)) 
								{
									var simFiles = SimilarFilesListSerializer.Deserialize (results);

									UpdateResults (simFiles);
								}
								// Updating the sending slave process' state to WAITING_FILE_COMPARE
								// meaning that slave machine is waiting for next file to be compared
								// to all files in one identified block
								UpdateSlavesState (false, socket, SlaveState.WAITING_FILE_COMPARE);
							
								Slave slv = (Slave)slaves [ipAddress];

								AutoResetEvent waitEvent = (AutoResetEvent)waitSlavesCompare [slv.id];

								waitEvent.Set ();

							} 


							catch (Exception e) 
							{
								Console.WriteLine (e.Message);
							}

							waitCompare.Set ();
							break;
						case Command.CopyFile:
							waitInfoSent.Set ();
							break;
						case Command.Info:
							long nbCmp = reader.ReadInt64 ();
							Slave slave = (Slave)slaves [ipAddress];
							slave.nbComparaisons = nbCmp;

							Slave slve = (Slave)slaves [ipAddress];

							AutoResetEvent waitInfo = (AutoResetEvent)waitInfoReceived [slve.id];

							waitInfo.Set ();
							break;

						}
					}
				}

				// wait for next message from slaves

				socket.BeginReceive (receiveBuffer, 0, receiveBuffer.Length, SocketFlags.None, new AsyncCallback (DataReceivedCallBack), state);
			
			} // end lock
		}

		/// <summary>
		/// Processes the results:
		/// Adds any list of similar files received from slaves to a master list of
		/// similar files.
		/// </summary>
		static void UpdateResults(List<SlaveProcess.SimilarFiles> simF)
		{
			if (simF != null)
			{
				for (int i = 0; i < simF.Count; i++)
				{
					if (!CheckForDuplicates(simF[i])) // disregard duplicates
						similarFiles.Add(simF[i]);
				}
			}

		}
		/// <summary>
		/// Checks for duplicates in similarFiles list
		/// </summary>
		/// <returns><c>true</c>, if duplicates found <c>false</c> otherwise.</returns>
		/// <param name="simf">Simf.</param>
		static bool CheckForDuplicates(SlaveProcess.SimilarFiles simf)
		{
			bool duplicate = false;

			if (similarFiles.Count > 0) 
			{
				for (int i = 0; i < similarFiles.Count; i++) 
				{
					if (
						((similarFiles [i].fileID1.blkfileIdx == simf.fileID2.blkfileIdx) && (similarFiles [i].fileID1.blockIdx == simf.fileID2.blockIdx))
						&&
						((similarFiles [i].fileID2.blkfileIdx == simf.fileID1.blkfileIdx) && (similarFiles [i].fileID2.blockIdx == simf.fileID1.blockIdx))) 
					{
						duplicate = true;
						break;
					}
				}
			}

			return duplicate;
		}

		/// <summary>
		/// Prints the statistics.
		/// </summary>
		static void PrintStatistics()
		{
			string output = "\n\n--------------------" +
							"\nProgram Statistics" +
							"\n--------------------\n\n";
			Console.WriteLine(output);
			log.WriteLine (output);

			output = string.Format ("Number of slave processes used {0}\n", slaves.Count);
			Console.WriteLine (output);
			log.WriteLine (output);


			foreach (DictionaryEntry entry in slaves) 
			{
				Slave slv = (Slave)entry.Value;

				output =string.Format ("Slave process {0} : running on host {0}:{1}", slv.id, slv.ipAddress, slv.portNumber);
				Console.WriteLine (output);
				log.WriteLine (output);
			}
				
			output = string.Format ("\nNumber of files identified {0}", files.Length);
			Console.WriteLine (output);
			log.WriteLine (output);

			output = string.Format ("\nBlock size in config. file {0} MB", blockSize );
			Console.WriteLine (output);
			log.WriteLine (output);

			output = string.Format ("\nNumber of blocks created {0}", blocks.Count );
			Console.WriteLine (output);
			log.WriteLine (output);

			for (int i = 0; i < blocks.Count; i++) 
			{
				output = string.Format ("Number of files in block{0} : {1}", i, blocks[i]);
				Console.WriteLine (output);
				log.WriteLine (output);
			}
				
			foreach (DictionaryEntry entry in slaves) 
			{
				Slave slv = (Slave)entry.Value;

				output =string.Format ("\nNb of files compared on Slave {0} : {1}", slv.ipAddress, slv.nbComparaisons);
				Console.WriteLine (output);
				log.WriteLine (output);
			}


			// Formats and displays
			output = String.Format( "\n\n---------------------------" +
				    				"\nTotal runtime is {0:00}:{1:00}:{2:00}"+
									"\n----------------------------",ts.Hours, ts.Minutes, ts.Seconds);
			Console.WriteLine(output);
			log.WriteLine(output);

		}

		/// <summary>
		/// Updates the state of the slaves
		/// Upate all slaves if all is true
		/// or just the identified slave if all is false
		/// </summary>
		/// <param name="all">if true update all slaves<c>true</c> otherwise only one</param>
		/// <param name="socket">Socket.</param>
		/// <param name="state">State.</param>
		static void UpdateSlavesState(bool all, Socket socket, SlaveState state)
		{
			lock (slaves) 
			{
				if (all) 
				{  // update all slaves with identified state
					foreach (DictionaryEntry entry in slaves) 
					{
						Slave slv = (Slave)entry.Value;

						// Update state
						slv.state = state;
					}
				} 

				else 
				{
					// Identifies slave

					string ipAddress = ((IPEndPoint)(socket.RemoteEndPoint)).Address.ToString ();

					Slave slv = (Slave)(slaves [ipAddress]);

					// Update state
					slv.state = state;
				}
			}
		}

		/// <summary>
		/// Compare all slaves' state to the one entered as parameter
		/// </summary>
		/// <returns><c>true</c>if all slaves' states are equal to parameter<c>false</c> otherwise.</returns>
		/// <param name="state">State.</param>
		static bool CheckSlavesState(SlaveState state)
		{
			bool result = true;

			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;
				if (slave.state != state)
				{
					result = false;
					break;
				}
			}

			return result;

		}

		/// <summary>
		/// Shutdown & Close all sockets.
		/// </summary>
		private void CloseSockets()
		{
			foreach (DictionaryEntry entry in slaves)
			{
				Slave slave = (Slave)entry.Value;
				Socket socket = slave.socket;

				// Release the socket.

				socket.Shutdown(SocketShutdown.Both);
				socket.Close();
			}

		}

		/// <summary>
		/// Processes final results:
		/// Converts block & file indexes of similar files (if any)
		/// to original file indexes and then to file names. 
		/// Logs to console & file
		/// </summary>
		void ProcessResults()
		{
			int blockIdx = 0;
			int fileIdx = 0;
			int index = 0;
			string output = null;
			string filename1 = null;
			string filename2 = null;

			if (similarFiles.Count == 0)
			{
				output = "\n-----------------------------"+
						 "\nResults of the comparaison:"+ 
						 "\n-----------------------------"+
						 "\n\nThe program didn't identify any similar files!\n";
				Console.WriteLine(output);
				log.WriteLine(output);
				log.Close();
				return;
			}

			for (int i = 0; i < similarFiles.Count; i++)
			{
				SlaveProcess.SimilarFiles simFiles = similarFiles[i];
				blockIdx = simFiles.fileID1.blockIdx;
				fileIdx = simFiles.fileID1.blkfileIdx;
				index = MapBlkFileIdxToFileIdx(blockIdx, fileIdx);
				filename1 = files[index];

				blockIdx = simFiles.fileID2.blockIdx;
				fileIdx = simFiles.fileID2.blkfileIdx;
				index = MapBlkFileIdxToFileIdx(blockIdx, fileIdx);
				filename2 = files[index];

				if (i == 0)
				{
					output = "\n-----------------------------"+
						     "\nResults of the comparaison:"+ 
						     "\n-----------------------------"+
						     "\n\nThe following files are similar:\n";
					Console.WriteLine(output);
					log.WriteLine(output);
				}

				output = string.Format("File {0} is similar to file {1}\n", filename1, filename2);
				Console.WriteLine(output);
				log.WriteLine(output);
			}
		}
		/// <summary>
		/// Gets the MD5 hash code for identified file.
		/// </summary>
		/// <returns>string containing the hash code for the file.</returns>
		/// <param name="fileName">File name.</param>
		static public string GetMD5HashFromFile(string fileName)
		{
			using (var md5 = MD5.Create())
			{
				using (var stream = File.OpenRead(fileName))
				{
					return BitConverter.ToString(md5.ComputeHash(stream)).Replace("-", string.Empty);
				}
			}
		}

		/// <summary>
		/// returns Salve with idetified Id number
		/// </summary>
		/// <returns>The slave.</returns>
		/// <param name="id">Identifier.</param>
		static Slave GetSlave(int id)
		{
			Slave slave = null;

			foreach (DictionaryEntry entry in slaves)
			{
				slave = (Slave)entry.Value;
				if (slave.id == id)
				{
					break;	
				}
			}

			return slave;
		}

		/// <summary>
		/// Checks if a file is a symbolic link.
		/// Symbolic links are disregarded 
		/// </summary>
		/// <returns><c>true</c>, if link was identifyed, <c>false</c> otherwise.</returns>
		/// <param name="file">File.</param>
		static bool IdentifyLink(string file)
		{
			bool symbLink = false;
			UnixSymbolicLinkInfo i = new UnixSymbolicLinkInfo( file );
			switch( i.FileType )
			{
			case FileTypes.SymbolicLink:
				symbLink = true;
				break;
			}

			return symbLink;
		}
		/// <summary>
		/// Test to validate file and blocks creation and mapping
		/// </summary>
		void TestBlockCreationAndMapping()
		{
			if (File.Exists("Test.txt"))

			{
				File.Delete("Test.txt");
			}

			StreamWriter testLog = new StreamWriter("Test.txt");

			int blkIdx = 0;
			int fileIdx = 0;
			for (int i = 0; i < files.Length; i++)
			{

				MapsFIdxtoBckNbFIndex(i, out blkIdx, out fileIdx);
				string output = string.Format("Index    {0}\n" +
					"Filename {1}\n" +
					"Block # {2}\n" +
					"File index in block # {3}\n\n", i, files[i], blkIdx, fileIdx);
				testLog.WriteLine(output);

			}

			testLog.Close();
		}

		/// <summary>
		/// Testing similar files generation before serialization.
		/// </summary>
		private static void TestSimilarFiles(List<SlaveProcess.SimilarFiles> simF)
		{
			StreamWriter logTest = new StreamWriter("TestFiles.txt", true);

			string output = null;

			int count = simF.Count;

			lock (logTest) 
			{
				if (count != 0) 
				{

					string.Format ("\n\nComparing file with file Idx {0} and block Idx {1} to all files in block idx {2}\n", currentFileCmpIdx, currentFileCmpBlkIdx, currentBlkCmpIdx);
					logTest.WriteLine (output);

					for (int i = 0; i < count; i++) 
					{
						output = string.Format ("File Idx {0} & Block Idx {1} is similar to File Idx {2} & Block Idx {3}\n",
							simF [i].fileID1.blkfileIdx,
							simF [i].fileID1.blockIdx,
							simF [i].fileID2.blkfileIdx,
							simF [i].fileID2.blockIdx);
						logTest.WriteLine (output);
					}
				}

			}

			logTest.Close();
		}

	}
}

